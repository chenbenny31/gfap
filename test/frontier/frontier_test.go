package frontier_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"gfap/internal/storage"

	"github.com/redis/go-redis/v9"
)

// This suite requires an empty, exclusively owned Redis Stack instance through
// GFAP_FRONTIER_TEST_SOCKET. It never defaults to the application's TCP endpoint,
// flushes a database, starts services, or contacts the crawl target. See the
// reproducible setup in .agent/tasks/frontier-failure-tests.md.
const (
	readyKey      = "crawler:ready"
	processingKey = "crawler:processing"
	leaseKey      = "crawler:lease"
	delayedKey    = "crawler:delayed"
	deadKey       = "crawler:dead"
	attemptsKey   = "crawler:attempts"
	strikesKey    = "crawler:strikes"
	listingKey    = "crawler:listing:next"
	barrenKey     = "crawler:listing:barren"
	sequenceKey   = "crawler:job:seq"
	bloomKey      = "crawler:bloom"
	ownerKey      = "gfap:frontier:test:owner"
	videoURL      = "https://frontier.invalid/watch?v=example"
	listingURL    = "https://frontier.invalid/user/example"
	seedURL       = "https://frontier.invalid/"
)

var frontierKeys = []string{
	readyKey, processingKey, leaseKey, delayedKey, deadKey, attemptsKey,
	strikesKey, listingKey, barrenKey, sequenceKey, bloomKey,
}

type harness struct {
	t      *testing.T
	ctx    context.Context
	r      *redis.Client
	f      storage.Frontier
	now    time.Time
	cfg    storage.FrontierConfig
	leased map[string]bool
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	socket := os.Getenv("GFAP_FRONTIER_TEST_SOCKET")
	if !filepath.IsAbs(socket) {
		t.Fatal("GFAP_FRONTIER_TEST_SOCKET must name an absolute Unix socket path for an empty, exclusively owned Redis Stack instance")
	}
	info, err := os.Lstat(socket)
	if err != nil {
		t.Fatalf("private Redis socket: %v", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		t.Fatalf("%s is not a Unix socket", socket)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	r := redis.NewClient(&redis.Options{
		Network: "unix", Addr: socket, DB: 15,
		DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second,
		WriteTimeout: 2 * time.Second, MaxRetries: -1,
	})
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("close Redis client: %v", err)
		}
	})
	must(t, r.Ping(ctx).Err())
	size, err := r.DBSize(ctx).Result()
	must(t, err)
	if size != 0 {
		t.Fatalf("refusing to alter nonempty scratch DB 15: %d keys", size)
	}
	owned, err := r.SetNX(ctx, ownerKey, t.Name(), 0).Result()
	must(t, err)
	if !owned {
		t.Fatal("another test owns scratch DB 15")
	}
	// Delete only the enumerated fixtures created after the empty-DB check and
	// ownership claim. Never FLUSHDB; unexpected keys remain visible next time.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		owner, err := r.Get(cleanupCtx, ownerKey).Result()
		if err != nil || owner != t.Name() {
			t.Errorf("scratch ownership changed; retaining fixtures: owner=%q err=%v", owner, err)
			return
		}
		keys := append(append([]string{}, frontierKeys...), ownerKey)
		if err := r.Del(cleanupCtx, keys...).Err(); err != nil {
			t.Errorf("remove owned test fixtures: %v", err)
		}
	})
	// Exercise the real RedisBloom admission command with a small fixture. The
	// production BloomInit/BloomVerify capacity contract is a separate suite.
	must(t, r.BFReserveWithArgs(ctx, bloomKey, &redis.BFReserveOptions{
		Capacity: 10000, Error: 0.00001, NonScaling: true,
	}).Err())
	h := &harness{
		t: t, ctx: ctx, r: r, now: time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC),
		cfg: storage.DefaultFrontierConfig(), leased: make(map[string]bool),
	}
	h.f = storage.NewFrontierWithClock(r, h.cfg, func(url string) storage.URLClass {
		if strings.Contains(url, "/watch?v=") {
			return storage.ClassVideo
		}
		if url == seedURL {
			return storage.ClassSeed
		}
		return storage.ClassListing
	}, func() time.Time { return h.now })
	return h
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func member(j *storage.Job) string { return j.ID + "|" + j.URL }

func (h *harness) admit(url string, want storage.AdmitResult) {
	h.t.Helper()
	var got storage.AdmitResult
	var err error
	if strings.Contains(url, "/watch?v=") {
		got, err = h.f.AdmitVideo(h.ctx, url)
	} else {
		got, err = h.f.AdmitListing(h.ctx, url)
	}
	must(h.t, err)
	if got != want {
		h.t.Fatalf("admit %s = %v, want %v", url, got, want)
	}
}

func (h *harness) lease(url string) *storage.Job {
	h.t.Helper()
	j, err := h.f.Lease(h.ctx)
	must(h.t, err)
	if j == nil || j.URL != url {
		h.t.Fatalf("lease = %+v, want URL %s", j, url)
	}
	if h.leased[j.ID] {
		h.t.Fatalf("job ID %s was leased more than once", j.ID)
	}
	h.leased[j.ID] = true
	h.score(leaseKey, member(j), h.now.Add(h.cfg.LeaseTimeout).Unix())
	return j
}

func (h *harness) score(key, item string, want int64) {
	h.t.Helper()
	got, err := h.r.ZScore(h.ctx, key, item).Result()
	must(h.t, err)
	if got != float64(want) {
		h.t.Fatalf("ZSCORE %s %s = %v, want %d", key, item, got, want)
	}
}

func (h *harness) noScore(key, item string) {
	h.t.Helper()
	got, err := h.r.ZScore(h.ctx, key, item).Result()
	if !errors.Is(err, redis.Nil) {
		h.t.Fatalf("ZSCORE %s %s = %v, %v; want absent", key, item, got, err)
	}
}

func (h *harness) field(key, url, want string) {
	h.t.Helper()
	got, err := h.r.HGet(h.ctx, key, url).Result()
	must(h.t, err)
	if got != want {
		h.t.Fatalf("HGET %s %s = %q, want %q", key, url, got, want)
	}
}

func (h *harness) noField(key, url string) {
	h.t.Helper()
	got, err := h.r.HGet(h.ctx, key, url).Result()
	if !errors.Is(err, redis.Nil) {
		h.t.Fatalf("HGET %s %s = %q, %v; want absent", key, url, got, err)
	}
}

func (h *harness) counts(ready, processing, delayed int64) {
	h.t.Helper()
	s, err := h.f.Stats(h.ctx)
	must(h.t, err)
	if s.Ready != ready || s.Processing != processing || s.Delayed != delayed {
		h.t.Fatalf("frontier counts = %+v, want ready=%d processing=%d delayed=%d", s, ready, processing, delayed)
	}
	leases, err := h.r.ZCard(h.ctx, leaseKey).Result()
	must(h.t, err)
	if leases != processing {
		h.t.Fatalf("leases = %d, want %d", leases, processing)
	}
}

func (h *harness) moved(op func(context.Context) (int, error), want int) {
	h.t.Helper()
	n, err := op(h.ctx)
	must(h.t, err)
	if n != want {
		h.t.Fatalf("reconciler moved %d jobs, want %d", n, want)
	}
}

func (h *harness) promoteAfter(j *storage.Job, delay time.Duration) *storage.Job {
	h.t.Helper()
	items, err := h.r.ZRangeWithScores(h.ctx, delayedKey, 0, -1).Result()
	must(h.t, err)
	due := h.now.Add(delay)
	if len(items) != 1 || items[0].Score != float64(due.Unix()) {
		h.t.Fatalf("delayed = %+v, want one job due at %d", items, due.Unix())
	}
	delayedMember, ok := items[0].Member.(string)
	if !ok || delayedMember == member(j) || !strings.HasSuffix(delayedMember, "|"+j.URL) {
		h.t.Fatalf("delayed member = %v, want a fresh ID for %s", items[0].Member, j.URL)
	}
	h.counts(0, 0, 1)
	if j.Class != storage.ClassVideo {
		h.score(listingKey, j.URL, storage.PendingScore)
		h.admit(j.URL, storage.Suppressed)
		h.moved(h.f.RunListingScheduler, 0)
	}
	h.now = due.Add(-time.Second)
	h.moved(h.f.Promote, 0)
	h.counts(0, 0, 1)
	h.now = due
	h.moved(h.f.Promote, 1)
	h.moved(h.f.Promote, 0)
	h.counts(1, 0, 0)
	next := h.lease(j.URL)
	if member(next) == delayedMember {
		h.t.Fatal("promotion reused the delayed job ID")
	}
	return next
}

func (h *harness) finish(j *storage.Job) {
	h.t.Helper()
	if j.Class == storage.ClassVideo {
		must(h.t, h.f.Ack(h.ctx, j))
	} else {
		must(h.t, h.f.CompleteListing(h.ctx, j, 1))
		base := h.cfg.ListingBaseTTL
		if j.Class == storage.ClassSeed {
			base = h.cfg.SeedTTL
		}
		h.score(listingKey, j.URL, h.now.Add(base).Unix())
		h.field(barrenKey, j.URL, "0")
	}
	h.noField(attemptsKey, j.URL)
	h.noField(strikesKey, j.URL)
}

func TestFrontierRateLimitBackoff(t *testing.T) {
	for _, url := range []string{videoURL, listingURL, seedURL} {
		t.Run(url, func(t *testing.T) {
			h := newHarness(t)
			h.admit(url, storage.New)
			h.admit(url, storage.Suppressed)
			h.counts(1, 0, 0)
			j := h.lease(url)
			for attempt := 1; attempt <= 20; attempt++ {
				disposition, err := h.f.Fail(h.ctx, j, storage.FailRateLimited)
				must(t, err)
				if disposition != storage.Delayed {
					t.Fatalf("rate limit attempt %d = %v, want delayed", attempt, disposition)
				}
				h.field(attemptsKey, url, strconv.Itoa(attempt))
				h.noField(strikesKey, url)
				h.noField(deadKey, url)
				delay := 45 * time.Minute
				if attempt == 1 {
					delay = 5 * time.Minute
				} else if attempt == 2 {
					delay = 15 * time.Minute
				}
				j = h.promoteAfter(j, delay)
			}
			h.finish(j)
			h.counts(0, 0, 0)
		})
	}
}

func TestFrontierStrikeLadder(t *testing.T) {
	for _, url := range []string{videoURL, listingURL, seedURL} {
		t.Run(url, func(t *testing.T) {
			h := newHarness(t)
			h.admit(url, storage.New)
			j := h.lease(url)
			for attempt, delay := range []time.Duration{time.Minute, 5 * time.Minute, 25 * time.Minute, 25 * time.Minute} {
				disposition, err := h.f.Fail(h.ctx, j, storage.FailStrikeable)
				must(t, err)
				if disposition != storage.Delayed {
					t.Fatalf("strike %d = %v, want delayed", attempt+1, disposition)
				}
				h.field(attemptsKey, url, strconv.Itoa(attempt+1))
				h.field(strikesKey, url, strconv.Itoa(attempt+1))
				h.noField(deadKey, url)
				j = h.promoteAfter(j, delay)
			}
			disposition, err := h.f.Fail(h.ctx, j, storage.FailStrikeable)
			must(t, err)
			if disposition != storage.Dead {
				t.Fatalf("fifth strike = %v, want dead", disposition)
			}
			until := int64(0)
			if j.Class == storage.ClassVideo {
				h.noScore(listingKey, url)
			} else {
				until = h.now.Add(7 * 24 * time.Hour).Unix()
				h.score(listingKey, url, until)
			}
			h.field(deadKey, url, fmt.Sprintf("strikes_exhausted|%d", until))
			h.noField(attemptsKey, url)
			h.noField(strikesKey, url)
			h.admit(url, storage.Suppressed)
			h.counts(0, 0, 0)
		})
	}
}

func TestFrontierMixedFailuresAndNackBudget(t *testing.T) {
	h := newHarness(t)
	h.admit(listingURL, storage.New)
	j := h.lease(listingURL)
	disposition, err := h.f.Fail(h.ctx, j, storage.FailRateLimited)
	must(t, err)
	if disposition != storage.Delayed {
		t.Fatalf("rate limit = %v, want delayed", disposition)
	}
	j = h.promoteAfter(j, 5*time.Minute)
	disposition, err = h.f.Fail(h.ctx, j, storage.FailStrikeable)
	must(t, err)
	if disposition != storage.Delayed {
		t.Fatalf("first strike = %v, want delayed", disposition)
	}
	h.field(attemptsKey, listingURL, "2")
	h.field(strikesKey, listingURL, "1")
	// The total attempt count selects the ladder position, even across classes.
	j = h.promoteAfter(j, 5*time.Minute)
	must(t, h.f.Nack(h.ctx, j))
	h.field(attemptsKey, listingURL, "2")
	h.field(strikesKey, listingURL, "1")
	h.score(listingKey, listingURL, storage.PendingScore)
	h.counts(1, 0, 0)
	j = h.lease(listingURL)
	h.finish(j)
	h.counts(0, 0, 0)
}

func TestFrontierTerminalQuarantine(t *testing.T) {
	for _, url := range []string{videoURL, listingURL, seedURL} {
		for _, reason := range []string{"404", "410"} {
			t.Run(url+"/"+reason, func(t *testing.T) {
				h := newHarness(t)
				h.admit(url, storage.New)
				j := h.lease(url)
				must(t, h.r.HSet(h.ctx, attemptsKey, url, 2).Err())
				must(t, h.r.HSet(h.ctx, strikesKey, url, 1).Err())
				until := time.Time{}
				untilUnix := int64(0)
				if j.Class != storage.ClassVideo {
					until = h.now.Add(7 * 24 * time.Hour)
					untilUnix = until.Unix()
				}
				must(t, h.f.Terminal(h.ctx, j, reason, until))
				h.field(deadKey, url, fmt.Sprintf("%s|%d", reason, untilUnix))
				h.noField(attemptsKey, url)
				h.noField(strikesKey, url)
				h.admit(url, storage.Suppressed)
				h.counts(0, 0, 0)
				if j.Class == storage.ClassVideo {
					h.noScore(listingKey, url)
					h.now = h.now.Add(365 * 24 * time.Hour)
					h.admit(url, storage.Suppressed)
					h.field(deadKey, url, reason+"|0")
					return
				}
				h.score(listingKey, url, untilUnix)
				// An older due index must not defeat the dead/quarantine check.
				must(t, h.r.ZAdd(h.ctx, listingKey, redis.Z{Score: float64(h.now.Unix()), Member: url}).Err())
				h.moved(h.f.RunListingScheduler, 0)
				h.score(listingKey, url, untilUnix)
				h.now = until.Add(-time.Second)
				h.admit(url, storage.Suppressed)
				h.moved(h.f.RunListingScheduler, 0)
				h.now = until
				if reason == "404" {
					h.moved(h.f.RunListingScheduler, 1)
				} else {
					h.admit(url, storage.DueRevisit)
				}
				h.noField(deadKey, url)
				h.score(listingKey, url, storage.PendingScore)
				h.admit(url, storage.Suppressed)
				h.counts(1, 0, 0)
				h.finish(h.lease(url))
				h.counts(0, 0, 0)
			})
		}
	}
}

func (h *harness) snapshot() map[string]string {
	h.t.Helper()
	state := make(map[string]string)
	for _, key := range frontierKeys {
		value, err := h.r.Dump(h.ctx, key).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		must(h.t, err)
		state[key] = value
	}
	return state
}

func TestFrontierStaleLeaseFencing(t *testing.T) {
	for _, operation := range []string{"Ack", "Nack", "FailRateLimited", "FailStrikeable", "Terminal", "CompleteListing"} {
		t.Run(operation, func(t *testing.T) {
			h := newHarness(t)
			url := listingURL
			if operation == "Ack" {
				url = videoURL
			}
			h.admit(url, storage.New)
			old := h.lease(url)
			must(t, h.r.HSet(h.ctx, attemptsKey, url, 2).Err())
			must(t, h.r.HSet(h.ctx, strikesKey, url, 1).Err())
			expiry := h.now.Add(h.cfg.LeaseTimeout)
			h.now = expiry.Add(-time.Second)
			h.moved(h.f.Reap, 0)
			h.now = expiry
			h.moved(h.f.Reap, 1)
			h.moved(h.f.Reap, 0)
			h.noScore(leaseKey, member(old))
			h.counts(1, 0, 0)
			current := h.lease(url)
			before := h.snapshot()
			var err error
			switch operation {
			case "Ack":
				err = h.f.Ack(h.ctx, old)
			case "Nack":
				err = h.f.Nack(h.ctx, old)
			case "FailRateLimited":
				_, err = h.f.Fail(h.ctx, old, storage.FailRateLimited)
			case "FailStrikeable":
				_, err = h.f.Fail(h.ctx, old, storage.FailStrikeable)
			case "Terminal":
				err = h.f.Terminal(h.ctx, old, "404", h.now.Add(h.cfg.QuarantineListing))
			case "CompleteListing":
				err = h.f.CompleteListing(h.ctx, old, 0)
			}
			if !errors.Is(err, storage.ErrStaleLease) {
				t.Fatalf("stale %s = %v, want ErrStaleLease", operation, err)
			}
			if !reflect.DeepEqual(before, h.snapshot()) {
				t.Fatalf("stale %s changed Redis frontier state", operation)
			}
			h.finish(current)
			h.counts(0, 0, 0)
		})
	}
}

func TestFrontierFreshIDsAcrossRequeues(t *testing.T) {
	h := newHarness(t)
	h.admit(listingURL, storage.New)
	j := h.lease(listingURL)
	must(t, h.f.Nack(h.ctx, j))
	h.noField(attemptsKey, listingURL)
	h.noField(strikesKey, listingURL)
	h.score(listingKey, listingURL, storage.PendingScore)
	j = h.lease(listingURL)
	disposition, err := h.f.Fail(h.ctx, j, storage.FailRateLimited)
	must(t, err)
	if disposition != storage.Delayed {
		t.Fatalf("rate limit = %v, want delayed", disposition)
	}
	j = h.promoteAfter(j, 5*time.Minute)
	h.now = h.now.Add(h.cfg.LeaseTimeout)
	h.moved(h.f.Reap, 1)
	h.score(listingKey, listingURL, storage.PendingScore)
	j = h.lease(listingURL)
	// Simulate a missing lease while retaining the processing member.
	removed, err := h.r.ZRem(h.ctx, leaseKey, member(j)).Result()
	must(t, err)
	if removed != 1 {
		t.Fatalf("orphan fixture removed %d lease entries, want 1", removed)
	}
	h.moved(h.f.Sweep, 1)
	h.moved(h.f.Sweep, 0)
	h.score(listingKey, listingURL, storage.PendingScore)
	j = h.lease(listingURL)
	if len(h.leased) != 5 {
		t.Fatalf("observed %d unique lease IDs, want 5", len(h.leased))
	}
	h.finish(j)
	h.counts(0, 0, 0)
}

func TestFrontierSweepPreservesLiveLease(t *testing.T) {
	h := newHarness(t)
	otherURL := listingURL + "/other"
	h.admit(listingURL, storage.New)
	h.admit(otherURL, storage.New)
	orphan := h.lease(listingURL)
	live := h.lease(otherURL)
	must(t, h.r.ZRem(h.ctx, leaseKey, member(orphan)).Err())
	h.moved(h.f.Sweep, 1)
	h.moved(h.f.Sweep, 0)
	h.counts(1, 1, 0)
	h.score(leaseKey, member(live), h.now.Add(h.cfg.LeaseTimeout).Unix())
	h.score(listingKey, listingURL, storage.PendingScore)
	recovered := h.lease(listingURL)
	if err := h.f.CompleteListing(h.ctx, orphan, 1); !errors.Is(err, storage.ErrStaleLease) {
		t.Fatalf("orphan completion = %v, want ErrStaleLease", err)
	}
	h.finish(live)
	h.finish(recovered)
	h.counts(0, 0, 0)
}

func TestFrontierDuplicateVideoRepresentation(t *testing.T) {
	h := newHarness(t)
	h.admit(videoURL, storage.New)
	// Model two distinct work instances retained for one URL after a partial
	// transition. Ordinary Bloom dedup is already exercised by the rate tests.
	id, err := h.r.Incr(h.ctx, sequenceKey).Result()
	must(t, err)
	must(t, h.r.LPush(h.ctx, readyKey, fmt.Sprintf("%d|%s", id, videoURL)).Err())
	first := h.lease(videoURL)
	second := h.lease(videoURL)
	h.counts(0, 2, 0)
	h.finish(first)
	h.counts(0, 1, 0)
	h.score(leaseKey, member(second), h.now.Add(h.cfg.LeaseTimeout).Unix())
	h.finish(second)
	h.counts(0, 0, 0)
	h.moved(h.f.Sweep, 0)
	h.moved(h.f.Reap, 0)
	j, err := h.f.Lease(h.ctx)
	must(t, err)
	if j != nil {
		t.Fatalf("unexpected remaining job: %+v", j)
	}
	idle, err := h.f.Idle(h.ctx)
	must(t, err)
	if !idle {
		t.Fatal("frontier not idle after both duplicate representations were acknowledged")
	}
}
