package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

const (
	mongoCrawlCollection = "crawler_state_v1"
	crawlStateBatchSize  = 100
	crawlStateOpTimeout  = 10 * time.Second
	keyCrawlStateRestore = "crawler:state:restore:v1"
)

type mongoDeadRecord struct {
	Reason string `bson:"reason"`
	Until  int64  `bson:"until"`
}

// Nullable fields are always written, so a cleared quarantine replaces an
// older observation. There are deliberately no per-pass timestamps here.
type mongoCrawlRecord struct {
	URL     string           `bson:"_id"`
	Schema  int32            `bson:"schema"`
	NextDue *int64           `bson:"next_due"`
	Barren  int64            `bson:"barren"`
	Dead    *mongoDeadRecord `bson:"dead"`
}

// CrawlStateStore saves URL records from Redis to Mongo and restores them at startup.
// Restore must succeed before Checkpoint is called. Call these methods sequentially.
// The caller owns and closes the underlying Redis and Mongo clients.
type CrawlStateStore struct {
	redis           *redis.Client
	collection      *mongo.Collection
	redisRunID      string
	startupComplete bool
}

type CheckpointStats struct {
	Observed int64 // observations, including URLs seen in multiple scans
	Batches  int64
	Upserted int64
	Modified int64
}

// NewCrawlStateStore configures checkpoint storage and records the Redis process identity.
// Restore enables checkpoint writes after its startup checks succeed.
func NewCrawlStateStore(ctx context.Context, redisStore *Redis, mongoStore *Mongo) (*CrawlStateStore, error) {
	if redisStore == nil || redisStore.client == nil ||
		mongoStore == nil || mongoStore.col == nil {
		return nil, errors.New("crawl state: Redis and Mongo clients are required")
	}

	journal := true
	store := &CrawlStateStore{
		redis: redisStore.client,
		collection: mongoStore.col.Database().Collection(mongoCrawlCollection, options.Collection().
			SetWriteConcern(&writeconcern.WriteConcern{W: 1, Journal: &journal}).
			SetReadPreference(readpref.Primary())),
	}

	var err error
	store.redisRunID, err = store.redisIdentity(ctx)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// redisIdentity reads the current Redis process ID for restart detection.
func (store *CrawlStateStore) redisIdentity(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, crawlStateOpTimeout)
	defer cancel()

	info, err := store.redis.Info(ctx, "server").Result()
	if err != nil {
		return "", fmt.Errorf("crawl state: Redis identity: %w", err)
	}
	for _, line := range strings.Split(info, "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "run_id:"); ok && value != "" {
			return value, nil
		}
	}
	return "", errors.New("crawl state: Redis run_id is missing")
}

// checkIdentity requires the Redis process recorded during construction.
func (store *CrawlStateStore) checkIdentity(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	id, err := store.redisIdentity(ctx)
	if err != nil {
		return err
	}
	if id != store.redisRunID {
		return errors.New("crawl state: Redis process changed; restart the crawler before checkpointing or restoring")
	}
	return nil
}

// Checkpoint reads bounded, internally consistent URL tuples and writes them
// sequentially. A failed pass can leave successfully saved rows in Mongo; it is
// never advertised as an atomic whole-corpus snapshot.
func (store *CrawlStateStore) Checkpoint(ctx context.Context) (stats CheckpointStats, err error) {
	if !store.startupComplete {
		return stats, errors.New("crawl state: successful startup Restore is required before checkpointing")
	}
	if err = store.checkIdentity(ctx); err != nil {
		return stats, err
	}
	for _, key := range []string{keyListingNext, keyListingBarren, keyDead} {
		var cursor uint64
		for {
			if err = ctx.Err(); err != nil {
				return stats, err
			}
			var pairs []string
			batchCtx, cancel := context.WithTimeout(ctx, crawlStateOpTimeout)
			if key == keyListingNext {
				pairs, cursor, err = store.redis.ZScan(batchCtx, key, cursor, "", crawlStateBatchSize).Result()
			} else {
				pairs, cursor, err = store.redis.HScan(batchCtx, key, cursor, "", crawlStateBatchSize).Result()
			}
			cancel()
			if err != nil {
				return stats, fmt.Errorf("crawl state: scan %s: %w", key, err)
			}
			if len(pairs)%2 != 0 {
				return stats, fmt.Errorf("crawl state: malformed scan reply for %s", key)
			}
			// COUNT is a hint. Split even a larger compact-encoding reply into
			// bounded transactions/writes; duplicate tracking is only per batch.
			for offset := 0; offset < len(pairs); offset += 2 * crawlStateBatchSize {
				end := min(offset+2*crawlStateBatchSize, len(pairs))
				urls := make([]string, 0, crawlStateBatchSize)
				seen := make(map[string]bool, crawlStateBatchSize)
				for i := offset; i < end; i += 2 {
					if !seen[pairs[i]] {
						urls = append(urls, pairs[i])
						seen[pairs[i]] = true
					}
				}
				rows, readErr := store.readRedisCrawlRecords(ctx, urls)
				if readErr != nil {
					return stats, readErr
				}
				stats.Observed += int64(len(rows))
				writes := make([]mongo.WriteModel, 0, len(rows))
				for _, row := range rows {
					writes = append(writes, mongo.NewReplaceOneModel().
						SetFilter(bson.M{"_id": row.URL}).SetReplacement(row).SetUpsert(true))
				}
				writeCtx, writeCancel := context.WithTimeout(ctx, crawlStateOpTimeout)
				result, writeErr := store.collection.BulkWrite(writeCtx, writes, options.BulkWrite().SetOrdered(true))
				writeCancel()
				if result != nil {
					stats.Upserted += result.UpsertedCount
					stats.Modified += result.ModifiedCount
				}
				if writeErr != nil {
					return stats, fmt.Errorf("crawl state: checkpoint batch: %w", writeErr)
				}
				stats.Batches++
			}
			if cursor == 0 {
				break
			}
		}
	}
	return stats, store.checkIdentity(ctx)
}

func (store *CrawlStateStore) readRedisCrawlRecords(ctx context.Context, urls []string) ([]mongoCrawlRecord, error) {
	ctx, cancel := context.WithTimeout(ctx, crawlStateOpTimeout)
	defer cancel()
	if err := store.checkIdentity(ctx); err != nil {
		return nil, err
	}
	type tuple struct {
		next         *redis.FloatCmd
		barren, dead *redis.StringCmd
	}
	reads := make([]tuple, 0, len(urls))
	pipe := store.redis.TxPipeline()
	for _, u := range urls {
		reads = append(reads, tuple{pipe.ZScore(ctx, keyListingNext, u),
			pipe.HGet(ctx, keyListingBarren, u), pipe.HGet(ctx, keyDead, u)})
	}
	cmds, err := pipe.Exec(ctx)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("crawl state: tuple transaction: %w", err)
	}
	// Exec may return redis.Nil for an earlier absent field even when a
	// later command failed with WRONGTYPE. Inspect every command result.
	for _, cmd := range cmds {
		if err := cmd.Err(); err != nil && !errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("crawl state: tuple %s: %w", cmd.Name(), err)
		}
	}
	if err := store.checkIdentity(ctx); err != nil {
		return nil, err
	}
	rows := make([]mongoCrawlRecord, 0, len(urls))
	for i, read := range reads {
		row := mongoCrawlRecord{URL: urls[i], Schema: 1}
		if read.next.Err() == nil {
			n := read.next.Val()
			if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > float64(PendingScore) || math.Trunc(n) != n {
				return nil, errors.New("crawl state: invalid listing score")
			}
			due := int64(n)
			row.NextDue = &due
		}
		if read.barren.Err() == nil {
			row.Barren, err = strconv.ParseInt(read.barren.Val(), 10, 64)
			if err != nil || row.Barren < 0 {
				return nil, errors.New("crawl state: invalid barren counter")
			}
		}
		if read.dead.Err() == nil {
			reason, rawUntil, ok := strings.Cut(read.dead.Val(), "|")
			until, parseErr := strconv.ParseInt(rawUntil, 10, 64)
			if !ok || parseErr != nil {
				return nil, errors.New("crawl state: malformed dead entry")
			}
			row.Dead = &mongoDeadRecord{Reason: reason, Until: until}
		}
		if err := row.validate(); err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (record mongoCrawlRecord) validate() error {
	parsedURL, err := url.Parse(record.URL)
	if err != nil || parsedURL.Hostname() == "" ||
		(parsedURL.Scheme != "http" && parsedURL.Scheme != "https") ||
		parsedURL.User != nil || parsedURL.Fragment != "" ||
		strings.ContainsAny(record.URL, "|\r\n\t ") {
		return errors.New("crawl state: invalid canonical URL")
	}
	if record.Schema != 1 || record.Barren < 0 {
		return errors.New("crawl state: invalid schema or barren counter")
	}
	if record.NextDue != nil && (*record.NextDue < 0 || *record.NextDue > PendingScore) {
		return errors.New("crawl state: invalid next_due")
	}
	if record.Dead != nil && (record.Dead.Reason == "" ||
		strings.ContainsAny(record.Dead.Reason, "|\r\n") ||
		record.Dead.Until < 0 || record.Dead.Until >= PendingScore) {
		return errors.New("crawl state: invalid dead reason or expiry")
	}
	return nil
}

// Require the v1 wire types and all fields before decoding. The driver's
// ordinary struct decoder accepts missing fields and some numeric coercions;
// those must not silently turn a damaged checkpoint into due-now/zero values.
func decodeMongoCrawlRecord(raw bson.Raw) (mongoCrawlRecord, error) {
	var row mongoCrawlRecord
	if err := validateMongoFields(raw, "_id", "schema", "next_due", "barren", "dead"); err != nil {
		return row, err
	}
	if raw.Lookup("_id").Type != bsontype.String || raw.Lookup("schema").Type != bsontype.Int32 ||
		raw.Lookup("barren").Type != bsontype.Int64 {
		return row, errors.New("crawl state: invalid BSON URL/schema/barren type")
	}
	if t := raw.Lookup("next_due").Type; t != bsontype.Null && t != bsontype.Int64 {
		return row, errors.New("crawl state: next_due must be BSON int64 or null")
	}
	dead := raw.Lookup("dead")
	if dead.Type != bsontype.Null {
		if dead.Type != bsontype.EmbeddedDocument {
			return row, errors.New("crawl state: dead must be a document or null")
		}
		doc := dead.Document()
		if err := validateMongoFields(doc, "reason", "until"); err != nil {
			return row, err
		}
		if doc.Lookup("reason").Type != bsontype.String || doc.Lookup("until").Type != bsontype.Int64 {
			return row, errors.New("crawl state: invalid BSON dead fields")
		}
	}
	if err := bson.Unmarshal(raw, &row); err != nil {
		return row, fmt.Errorf("crawl state: decode checkpoint: %w", err)
	}
	return row, row.validate()
}

// validateMongoFields requires every named field exactly once, with no extras.
func validateMongoFields(raw bson.Raw, fieldNames ...string) error {
	elements, err := raw.Elements()
	if err != nil {
		return fmt.Errorf("crawl state: invalid BSON document: %w", err)
	}
	if len(elements) != len(fieldNames) {
		return errors.New("crawl state: missing or extra checkpoint fields")
	}
	remaining := make(map[string]bool, len(fieldNames))
	for _, name := range fieldNames {
		remaining[name] = true
	}
	for _, element := range elements {
		if !remaining[element.Key()] {
			return errors.New("crawl state: duplicate or unknown checkpoint field")
		}
		delete(remaining, element.Key())
	}
	return nil
}
