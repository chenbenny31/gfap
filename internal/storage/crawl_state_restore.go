package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type restoreMarker struct {
	Schema int    `json:"schema"`
	Phase  string `json:"phase"`
	Source string `json:"source"`
}

type restoreState struct {
	listingCount int64
	barrenCount  int64
	deadCount    int64
	workCount    int64
	marker       *restoreMarker
}

type RestoreStats struct {
	Mode string // empty, preserved, or restored
	Rows int64  // rows in fully acknowledged restore batches
}

func (store *CrawlStateStore) mongoSource() string {
	return store.collection.Database().Name() + "." + store.collection.Name()
}

func (store *CrawlStateStore) encodeRestoreMarker(phase string) string {
	// A struct containing only fixed field types cannot fail JSON encoding.
	data, _ := json.Marshal(restoreMarker{Schema: 1, Phase: phase, Source: store.mongoSource()})
	return string(data)
}

func (store *CrawlStateStore) readStartupState(ctx context.Context) (restoreState, error) {
	ctx, cancel := context.WithTimeout(ctx, crawlStateOpTimeout)
	defer cancel()
	var state restoreState
	pipe := store.redis.TxPipeline()
	next := pipe.ZCard(ctx, keyListingNext)
	barren := pipe.HLen(ctx, keyListingBarren)
	dead := pipe.HLen(ctx, keyDead)
	work := []*redis.IntCmd{
		pipe.LLen(ctx, keyReady), pipe.LLen(ctx, keyProcessing),
		pipe.ZCard(ctx, keyDelayed), pipe.ZCard(ctx, keyLease),
		pipe.HLen(ctx, keyAttempts), pipe.HLen(ctx, keyStrikes),
	}
	marker := pipe.Get(ctx, keyCrawlStateRestore)
	cmds, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return state, fmt.Errorf("crawl state: inspect startup: %w", err)
	}
	for _, cmd := range cmds {
		if err := cmd.Err(); err != nil && !(cmd == marker && errors.Is(err, redis.Nil)) {
			return state, fmt.Errorf("crawl state: inspect %s: %w", cmd.Name(), err)
		}
	}
	state.listingCount, state.barrenCount, state.deadCount = next.Val(), barren.Val(), dead.Val()
	for _, count := range work {
		state.workCount += count.Val()
	}
	if marker.Err() == nil {
		var savedMarker restoreMarker
		decoder := json.NewDecoder(strings.NewReader(marker.Val()))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&savedMarker); err != nil {
			return state, fmt.Errorf("crawl state: invalid restore marker: %w", err)
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			return state, errors.New("crawl state: trailing restore marker data")
		}
		if savedMarker.Schema != 1 || (savedMarker.Phase != "restoring" && savedMarker.Phase != "complete") || savedMarker.Source != store.mongoSource() {
			return state, errors.New("crawl state: restore marker schema, phase, or source mismatch")
		}
		state.marker = &savedMarker
	}
	return state, nil
}

// Restore is a startup barrier: no workers, frontier reconcilers or checkpoint
// writer may run until it succeeds. It preserves a valid nonempty Redis frontier;
// it does not merge newer Mongo observations over an older nonempty RDB.
func (store *CrawlStateStore) Restore(ctx context.Context) (stats RestoreStats, err error) {
	store.startupComplete = false
	if err = store.checkIdentity(ctx); err != nil {
		return stats, err
	}
	state, err := store.readStartupState(ctx)
	if err != nil {
		return stats, err
	}
	resuming := state.marker != nil && state.marker.Phase == "restoring"
	if resuming && state.workCount != 0 {
		return stats, errors.New("crawl state: unfinished restore has live work or retry state; refusing to overwrite")
	}
	if !resuming {
		preserve := state.listingCount > 0 || (state.marker != nil && state.marker.Phase == "complete" &&
			state.workCount == 0 && (state.barrenCount > 0 || state.deadCount > 0))
		if preserve {
			err = store.checkIdentity(ctx)
			store.startupComplete = err == nil
			stats.Mode = "preserved"
			return stats, err
		}
		if state.workCount != 0 || state.barrenCount != 0 || state.deadCount != 0 {
			return stats, errors.New("crawl state: missing listing schedule with existing frontier state; automatic cold restore is unsafe")
		}
	}

	findCtx, findCancel := context.WithTimeout(ctx, crawlStateOpTimeout)
	cursor, err := store.collection.Find(findCtx, bson.D{}, options.Find().
		SetBatchSize(crawlStateBatchSize).SetSort(bson.D{{Key: "_id", Value: 1}}))
	findCancel()
	if err != nil {
		return stats, fmt.Errorf("crawl state: open restore cursor: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer closeCancel()
		if closeErr := cursor.Close(closeCtx); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("crawl state: close restore cursor: %w", closeErr))
			store.startupComplete = false
		}
	}()
	started := false
	rows := make([]mongoCrawlRecord, 0, crawlStateBatchSize)
	for {
		nextCtx, nextCancel := context.WithTimeout(ctx, crawlStateOpTimeout)
		more := cursor.Next(nextCtx)
		nextCancel()
		if !more {
			break
		}
		if !started {
			if err = store.checkIdentity(ctx); err != nil {
				return stats, err
			}
			if !resuming {
				markCtx, markCancel := context.WithTimeout(ctx, crawlStateOpTimeout)
				if state.marker == nil {
					var set bool
					set, err = store.redis.SetNX(markCtx, keyCrawlStateRestore, store.encodeRestoreMarker("restoring"), 0).Result()
					if err == nil && !set {
						err = errors.New("restore marker appeared concurrently")
					}
				} else {
					err = store.redis.Set(markCtx, keyCrawlStateRestore, store.encodeRestoreMarker("restoring"), 0).Err()
				}
				markCancel()
				if err != nil {
					return stats, fmt.Errorf("crawl state: begin restore: %w", err)
				}
			}
			started = true
		}
		row, decodeErr := decodeMongoCrawlRecord(cursor.Current)
		if decodeErr != nil {
			return stats, decodeErr
		}
		rows = append(rows, row)
		if len(rows) == crawlStateBatchSize {
			if err = store.restoreBatch(ctx, rows); err != nil {
				return stats, err
			}
			stats.Rows += int64(len(rows))
			rows = rows[:0]
		}
	}
	if err = cursor.Err(); err != nil {
		return stats, fmt.Errorf("crawl state: read restore cursor: %w", err)
	}
	if !started {
		if resuming {
			return stats, errors.New("crawl state: checkpoint source is empty during unfinished restore")
		}
		err = store.checkIdentity(ctx)
		store.startupComplete = err == nil
		stats.Mode = "empty"
		return stats, err
	}
	if len(rows) > 0 {
		if err = store.restoreBatch(ctx, rows); err != nil {
			return stats, err
		}
		stats.Rows += int64(len(rows))
	}
	if err = store.checkRestoreBarrier(ctx); err != nil {
		return stats, err
	}
	// Never append this to the field-write transaction. Redis executes later
	// commands even when an earlier command fails at runtime.
	markCtx, markCancel := context.WithTimeout(ctx, crawlStateOpTimeout)
	err = store.redis.Set(markCtx, keyCrawlStateRestore, store.encodeRestoreMarker("complete"), 0).Err()
	markCancel()
	if err != nil {
		return stats, fmt.Errorf("crawl state: complete restore: %w", err)
	}
	err = store.checkIdentity(ctx)
	store.startupComplete = err == nil
	stats.Mode = "restored"
	return stats, err
}

func (store *CrawlStateStore) checkRestoreBarrier(ctx context.Context) error {
	if err := store.checkIdentity(ctx); err != nil {
		return err
	}
	state, err := store.readStartupState(ctx)
	if err != nil {
		return err
	}
	if state.marker == nil || state.marker.Phase != "restoring" || state.workCount != 0 {
		return errors.New("crawl state: restore barrier changed; refusing further writes")
	}
	return nil
}

func (store *CrawlStateStore) restoreBatch(ctx context.Context, rows []mongoCrawlRecord) error {
	ctx, cancel := context.WithTimeout(ctx, crawlStateOpTimeout)
	defer cancel()
	if err := store.checkRestoreBarrier(ctx); err != nil {
		return err
	}
	now := time.Now().Unix()
	pipe := store.redis.TxPipeline()
	for _, row := range rows {
		next, dead := row.NextDue, row.Dead
		if next != nil && *next == PendingScore {
			due := int64(0)
			next = &due
		}
		if dead != nil {
			switch {
			case dead.Until == 0:
				next = nil // permanently dead URLs need no revisit
			case dead.Until <= now:
				dead = nil
			case next != nil && *next < dead.Until:
				until := dead.Until
				next = &until
			}
		}
		if next == nil {
			pipe.ZRem(ctx, keyListingNext, row.URL)
		} else {
			pipe.ZAdd(ctx, keyListingNext, redis.Z{Score: float64(*next), Member: row.URL})
		}
		if next == nil && dead == nil && row.Barren == 0 {
			pipe.HDel(ctx, keyListingBarren, row.URL)
		} else {
			pipe.HSet(ctx, keyListingBarren, row.URL, row.Barren)
		}
		if dead == nil {
			pipe.HDel(ctx, keyDead, row.URL)
		} else {
			pipe.HSet(ctx, keyDead, row.URL, dead.Reason+"|"+strconv.FormatInt(dead.Until, 10))
		}
	}
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("crawl state: restore batch (marker retained): %w", err)
	}
	for _, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			return fmt.Errorf("crawl state: restore %s (marker retained): %w", cmd.Name(), err)
		}
	}
	return store.checkIdentity(ctx)
}
