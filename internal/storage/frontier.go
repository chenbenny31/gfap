package storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// ============================================================================
// Keyspace
// ============================================================================
//
// crawler:job:seq         STRING  INCR source for JobIDs
// crawler:ready           LIST    "<jobID>|<url>", durable, awaiting lease
// crawler:processing      LIST    leased members
// crawler:lease           ZSET    member -> expiry (fencing token AND reaper index)
// crawler:delayed         ZSET    member -> next_attempt_at
// crawler:dead            HASH    url -> "<reason>|<until>"; until=0 => forever
// crawler:attempts        HASH    url -> failed attempts, all classes
// crawler:strikes         HASH    url -> terminal-eligible failures only
// crawler:listing:next    ZSET    url -> due_at | PendingScore
// crawler:listing:barren  HASH    url -> consecutive no-New-children count
// crawler:bloom           BF      video URLs, claimed at admission
//
// JobID is minted fresh on every requeue and leased at most once - a ZREM
// on the lease ZSET returning 0 means stale, no separate token needed.
// URL-keyed structures are per-URL facts; only lease/ready/processing/delayed
// are work-instance (member-keyed) state.
const (
	keyJobSeq        = "crawler:job:seq"
	keyReady         = "crawler:ready"
	keyProcessing    = "crawler:processing"
	keyLease         = "crawler:lease"
	keyDelayed       = "crawler:delayed"
	keyDead          = "crawler:dead"
	keyAttempts      = "crawler:attempts"
	keyStrikes       = "crawler:strikes"
	keyListingNext   = "crawler:listing:next"
	keyListingBarren = "crawler:listing:barren"
)

// PendingScore marks a listing as having a live job (9999-12-31, fixed, never
// computed). Released only by CompleteListing or a terminal transition.
const PendingScore int64 = 253402300799

// ============================================================================
// Types
// ============================================================================

type AdmitResult int

const (
	Suppressed AdmitResult = 0 // duplicate / not due / PENDING / dead / quarantined
	New        AdmitResult = 1 // never seen - only this counts toward barren
	DueRevisit AdmitResult = 2 // listing seen before, TTL expired
)

type FailClass int

const (
	FailRateLimited FailClass = iota // no strike, indefinite retry - not the URL's fault
	FailStrikeable                   // strikes, DEAD at MaxStrikes
)

type Disposition int

const (
	Delayed Disposition = iota
	Dead
)

// URLClass is a cheap pre-fetch check, distinct from worker.go's page-content
// classification (PageVideo/PageListing/...) done after fetch.
type URLClass int

const (
	ClassVideo URLClass = iota
	ClassListing
	ClassSeed
)

// URLClassifier is injected so frontier.go stays decoupled from crawler config.
type URLClassifier func(url string) URLClass

type Job struct {
	ID     string
	URL    string
	Class  URLClass
	member string // "<ID>|<URL>" - the exact frontier member
}

// ErrStaleLease: the job no longer owns a live lease (reaped, completed by a
// duplicate, or orphaned). Log DEBUG and drop the result.
var ErrStaleLease = errors.New("frontier: job no longer owns a live lease; drop the result")

type FrontierStats struct {
	Ready             int64
	Processing        int64
	Delayed           int64
	ListingsScheduled int64
	ListingsPending   int64
	Dead              int64
	Strikes           int64
}

// FrontierConfig holds tunable params. Zero values are NOT defaulted -
// use DefaultFrontierConfig() or populate every field explicitly.
type FrontierConfig struct {
	LeaseTimeout time.Duration
	MaxStrikes   int

	RateLimitBackoff []int // seconds, indexed by attempts (1-based), clamped to last
	StrikeBackoff    []int

	ListingBaseTTL    time.Duration
	SeedTTL           time.Duration
	ListingMaxTTL     time.Duration
	QuarantineListing time.Duration

	SchedulerBatchLimit int
}

func DefaultFrontierConfig() FrontierConfig {
	return FrontierConfig{
		LeaseTimeout:        600 * time.Second,
		MaxStrikes:          5,
		RateLimitBackoff:    []int{300, 900, 2700},
		StrikeBackoff:       []int{60, 300, 1500},
		ListingBaseTTL:      72 * time.Hour,
		SeedTTL:             24 * time.Hour,
		ListingMaxTTL:       21 * 24 * time.Hour,
		QuarantineListing:   7 * 24 * time.Hour,
		SchedulerBatchLimit: 100,
	}
}

// Reap/Sweep/Promote batch limits are fixed, not config-exposed (unlike
// SchedulerBatchLimit's loop-while-full draining behavior).
const (
	promoteBatchLimit = 100
	reapBatchLimit    = 100
	sweepBatchLimit   = 100
)

// ============================================================================
// Interface
// ============================================================================

type Frontier interface {
	AdmitVideo(ctx context.Context, url string) (AdmitResult, error)
	AdmitListing(ctx context.Context, url string) (AdmitResult, error)

	Lease(ctx context.Context) (*Job, error) // nil, nil when frontier empty

	Ack(ctx context.Context, j *Job) error
	Nack(ctx context.Context, j *Job) error
	Fail(ctx context.Context, j *Job, class FailClass) (Disposition, error)
	Terminal(ctx context.Context, j *Job, reason string, until time.Time) error
	CompleteListing(ctx context.Context, j *Job, newChildren int) error

	Reap(ctx context.Context) (int, error)
	Sweep(ctx context.Context) (int, error)
	Promote(ctx context.Context) (int, error)
	RunListingScheduler(ctx context.Context) (int, error)

	Idle(ctx context.Context) (bool, error)
	Stats(ctx context.Context) (FrontierStats, error)
}

// ============================================================================
// Lua scripts. No rollback on mid-script error - destination is always
// written before source is removed, so failure means duplicate, not loss.
// ============================================================================

const luaLease = `
-- KEYS: 1=ready 2=processing 3=lease
-- ARGV: 1=now 2=leaseTimeout
local item = redis.call('LMOVE', KEYS[1], KEYS[2], 'RIGHT', 'LEFT')
if not item then return nil end
redis.call('ZADD', KEYS[3], tonumber(ARGV[1]) + tonumber(ARGV[2]), item)
return item
`

const luaAdmitVideo = `
-- KEYS: 1=ready 2=bloom 3=dead 4=job:seq
-- ARGV: 1=url 2=now
local url, now = ARGV[1], tonumber(ARGV[2])

local d = redis.call('HGET', KEYS[3], url)
if d then
  local sep = string.find(d, '|', 1, true)
  local until_ts = tonumber(string.sub(d, sep + 1))
  if until_ts == 0 or until_ts > now then return 0 end
  redis.call('HDEL', KEYS[3], url)
end

local item = redis.call('INCR', KEYS[4]) .. '|' .. url
redis.call('LPUSH', KEYS[1], item)                    -- DESTINATION FIRST
local added = redis.call('BF.ADD', KEYS[2], url)
if added == 0 then
  redis.call('LREM', KEYS[1], 1, item)                -- duplicate: exact-member rollback
  return 0
end
return 1
`

const luaAdmitListing = `
-- KEYS: 1=ready 2=listing:next 3=dead 4=job:seq
-- ARGV: 1=url 2=now 3=PENDING
local url, now = ARGV[1], tonumber(ARGV[2])
local d = redis.call('HGET', KEYS[3], url)
if d then
  local until_ts = tonumber(string.sub(d, string.find(d, '|', 1, true) + 1))
  if until_ts == 0 or until_ts > now then return 0 end
  redis.call('HDEL', KEYS[3], url)
end
local prev = redis.call('ZSCORE', KEYS[2], url)
if prev and tonumber(prev) > now then return 0 end
redis.call('LPUSH', KEYS[1], redis.call('INCR', KEYS[4]) .. '|' .. url)   -- DESTINATION FIRST
redis.call('ZADD', KEYS[2], ARGV[3], url)                                 -- PENDING
if prev then return 2 end
return 1
`

const luaListingScheduler = `
-- KEYS: 1=listing:next 2=ready 3=job:seq 4=dead
-- ARGV: 1=now 2=limit 3=PENDING
local now = tonumber(ARGV[1])
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local n = 0
for _, url in ipairs(due) do
  local blocked = false
  local d = redis.call('HGET', KEYS[4], url)
  if d then
    local until_ts = tonumber(string.sub(d, string.find(d, '|', 1, true) + 1))
    if until_ts == 0 then
      redis.call('ZREM', KEYS[1], url)
      blocked = true
    elseif until_ts > now then
      redis.call('ZADD', KEYS[1], until_ts, url)
      blocked = true
    else
      redis.call('HDEL', KEYS[4], url)
    end
  end
  if not blocked then
    redis.call('LPUSH', KEYS[2], redis.call('INCR', KEYS[3]) .. '|' .. url)  -- DESTINATION FIRST
    redis.call('ZADD', KEYS[1], ARGV[3], url)                                -- PENDING
    n = n + 1
  end
end
return n
`

const luaAck = `
-- KEYS: 1=lease 2=processing 3=attempts 4=strikes
-- ARGV: 1=item 2=url
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 'STALE' end
redis.call('LREM', KEYS[2], 1, ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[2])
redis.call('HDEL', KEYS[4], ARGV[2])
return 'OK'
`

const luaCompleteListing = `
-- KEYS: 1=lease 2=processing 3=listing:next 4=listing:barren 5=dead 6=attempts 7=strikes
-- ARGV: 1=item 2=url 3=newChildren 4=now 5=baseTTL 6=maxTTL
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 'STALE' end
local url = ARGV[2]
local barren
if tonumber(ARGV[3]) > 0 then
  barren = 0
  redis.call('HSET', KEYS[4], url, 0)
else
  barren = redis.call('HINCRBY', KEYS[4], url, 1)
end
local ttl = tonumber(ARGV[5]) * (2 ^ barren)
if ttl > tonumber(ARGV[6]) then ttl = tonumber(ARGV[6]) end
redis.call('ZADD', KEYS[3], tonumber(ARGV[4]) + ttl, url)   -- DESTINATION FIRST; releases PENDING
redis.call('HDEL', KEYS[5], url)                            -- last observation wins
redis.call('LREM', KEYS[2], 1, ARGV[1])
redis.call('HDEL', KEYS[6], url)
redis.call('HDEL', KEYS[7], url)
return 'OK'
`

const luaNack = `
-- KEYS: 1=lease 2=ready 3=processing 4=job:seq
-- ARGV: 1=item 2=url
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 'STALE' end
redis.call('LPUSH', KEYS[2], redis.call('INCR', KEYS[4]) .. '|' .. ARGV[2])  -- DEST FIRST, new job
redis.call('LREM', KEYS[3], 1, ARGV[1])
return 'OK'
`

const luaFail = `
-- KEYS: 1=lease 2=processing 3=attempts 4=strikes 5=delayed 6=dead 7=job:seq 8=listing:next
-- ARGV: 1=item 2=url 3=incStrike(0|1) 4=maxStrikes 5=now 6=reason 7=deadUntil 8..=backoff secs
local item, url = ARGV[1], ARGV[2]
if redis.call('ZREM', KEYS[1], item) == 0 then return 'STALE' end

local attempts = redis.call('HINCRBY', KEYS[3], url, 1)
local strikes  = tonumber(redis.call('HGET', KEYS[4], url) or '0')
if ARGV[3] == '1' then
  strikes = redis.call('HINCRBY', KEYS[4], url, 1)
end

if ARGV[3] == '1' and strikes >= tonumber(ARGV[4]) then
  if tonumber(ARGV[7]) > 0 then
    redis.call('ZADD', KEYS[8], tonumber(ARGV[7]), url)           -- listing:next BEFORE dead
  end
  redis.call('HSET', KEYS[6], url, ARGV[6] .. '|' .. ARGV[7])     -- DESTINATION FIRST
  redis.call('LREM', KEYS[2], 1, item)
  redis.call('HDEL', KEYS[3], url)
  redis.call('HDEL', KEYS[4], url)
  return 'DEAD'
end

local n = #ARGV - 7
local i = attempts; if i > n then i = n end
local nextItem = redis.call('INCR', KEYS[7]) .. '|' .. url
redis.call('ZADD', KEYS[5], tonumber(ARGV[5]) + tonumber(ARGV[7 + i]), nextItem)  -- DEST FIRST
redis.call('LREM', KEYS[2], 1, item)
return 'DELAYED'
`

const luaTerminal = `
-- KEYS: 1=lease 2=processing 3=dead 4=attempts 5=strikes 6=listing:next
-- ARGV: 1=item 2=url 3=reason 4=until
if redis.call('ZREM', KEYS[1], ARGV[1]) == 0 then return 'STALE' end
if tonumber(ARGV[4]) > 0 then
  redis.call('ZADD', KEYS[6], tonumber(ARGV[4]), ARGV[2])         -- listing:next BEFORE dead
end
redis.call('HSET', KEYS[3], ARGV[2], ARGV[3] .. '|' .. ARGV[4])   -- DESTINATION FIRST
redis.call('LREM', KEYS[2], 1, ARGV[1])
redis.call('HDEL', KEYS[4], ARGV[2])
redis.call('HDEL', KEYS[5], ARGV[2])
return 'OK'
`

const luaPromote = `
-- KEYS: 1=delayed 2=ready 3=job:seq
-- ARGV: 1=now 2=limit
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, item in ipairs(due) do
  local url = string.sub(item, string.find(item, '|', 1, true) + 1)
  redis.call('LPUSH', KEYS[2], redis.call('INCR', KEYS[3]) .. '|' .. url)   -- DEST FIRST, new job
  redis.call('ZREM', KEYS[1], item)
end
return #due
`

const luaReap = `
-- KEYS: 1=lease 2=ready 3=processing 4=job:seq
-- ARGV: 1=now 2=limit
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local n = 0
for _, item in ipairs(expired) do
  if redis.call('ZREM', KEYS[1], item) == 1 then          -- claim
    local url = string.sub(item, string.find(item, '|', 1, true) + 1)
    redis.call('LPUSH', KEYS[2], redis.call('INCR', KEYS[4]) .. '|' .. url)  -- DEST FIRST, new job
    redis.call('LREM', KEYS[3], 1, item)
    n = n + 1
  end
end
return n
`

const luaSweep = `
-- KEYS: 1=processing 2=lease 3=ready 4=job:seq
-- ARGV: 1=limit
local items = redis.call('LRANGE', KEYS[1], -tonumber(ARGV[1]), -1)
local n = 0
for _, item in ipairs(items) do
  if not redis.call('ZSCORE', KEYS[2], item) then
    local url = string.sub(item, string.find(item, '|', 1, true) + 1)
    redis.call('LPUSH', KEYS[3], redis.call('INCR', KEYS[4]) .. '|' .. url)  -- DEST FIRST
    redis.call('LREM', KEYS[1], 1, item)
    n = n + 1
  end
end
return n
`

var (
	scriptLease            = redis.NewScript(luaLease)
	scriptAdmitVideo       = redis.NewScript(luaAdmitVideo)
	scriptAdmitListing     = redis.NewScript(luaAdmitListing)
	scriptListingScheduler = redis.NewScript(luaListingScheduler)
	scriptAck              = redis.NewScript(luaAck)
	scriptCompleteListing  = redis.NewScript(luaCompleteListing)
	scriptNack             = redis.NewScript(luaNack)
	scriptFail             = redis.NewScript(luaFail)
	scriptTerminal         = redis.NewScript(luaTerminal)
	scriptPromote          = redis.NewScript(luaPromote)
	scriptReap             = redis.NewScript(luaReap)
	scriptSweep            = redis.NewScript(luaSweep)
)

// ============================================================================
// Implementation
// ============================================================================

type redisFrontier struct {
	client   *redis.Client
	now      func() time.Time
	classify URLClassifier
	cfg      FrontierConfig
}

func NewFrontier(client *redis.Client, cfg FrontierConfig, classify URLClassifier) Frontier {
	return &redisFrontier{client: client, now: time.Now, classify: classify, cfg: cfg}
}

// NewFrontierWithClock injects a clock, for tests that advance time.
func NewFrontierWithClock(client *redis.Client, cfg FrontierConfig, classify URLClassifier, now func() time.Time) Frontier {
	return &redisFrontier{client: client, now: now, classify: classify, cfg: cfg}
}

// splitMember splits "<jobID>|<url>". JobIDs are digits-only and canonical
// URLs never contain '|' (percent-encoded), so the first '|' is exact.
func splitMember(member string) (jobID, url string) {
	i := strings.IndexByte(member, '|')
	if i < 0 {
		return member, ""
	}
	return member[:i], member[i+1:]
}

// unixOrZero maps the zero time.Time to literal 0 - load-bearing, since FAIL
// and TERMINAL use "until > 0" to distinguish quarantine from VIDEO's forever.
func unixOrZero(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.Unix()
}

// quarantineUntil: 0 for VIDEO, now+QuarantineListing for LISTING/SEED.
// Mirrors worker.go's untilFor - duplicated since Fail has no until param.
func (f *redisFrontier) quarantineUntil(class URLClass, now time.Time) int64 {
	if class == ClassVideo {
		return 0
	}
	return now.Add(f.cfg.QuarantineListing).Unix()
}

func backoffArgs(ladder []int) []interface{} {
	args := make([]interface{}, 0, len(ladder))
	for _, s := range ladder {
		args = append(args, s)
	}
	return args
}

// --- admission ---

func (f *redisFrontier) AdmitVideo(ctx context.Context, url string) (AdmitResult, error) {
	res, err := scriptAdmitVideo.Run(ctx, f.client,
		[]string{keyReady, bloomKey, keyDead, keyJobSeq},
		url, f.now().Unix(),
	).Int64()
	if err != nil {
		return Suppressed, err
	}
	return AdmitResult(res), nil
}

func (f *redisFrontier) AdmitListing(ctx context.Context, url string) (AdmitResult, error) {
	res, err := scriptAdmitListing.Run(ctx, f.client,
		[]string{keyReady, keyListingNext, keyDead, keyJobSeq},
		url, f.now().Unix(), PendingScore,
	).Int64()
	if err != nil {
		return Suppressed, err
	}
	return AdmitResult(res), nil
}

// --- lease / completion ---

func (f *redisFrontier) Lease(ctx context.Context) (*Job, error) {
	res, err := scriptLease.Run(ctx, f.client,
		[]string{keyReady, keyProcessing, keyLease},
		f.now().Unix(), int64(f.cfg.LeaseTimeout/time.Second),
	).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // frontier empty
	}
	if err != nil {
		return nil, err
	}
	member, ok := res.(string)
	if !ok {
		return nil, fmt.Errorf("frontier: unexpected LEASE result type %T", res)
	}
	id, url := splitMember(member)
	class := ClassListing
	if f.classify != nil {
		class = f.classify(url)
	}
	return &Job{ID: id, URL: url, Class: class, member: member}, nil
}

func (f *redisFrontier) Ack(ctx context.Context, j *Job) error {
	res, err := scriptAck.Run(ctx, f.client,
		[]string{keyLease, keyProcessing, keyAttempts, keyStrikes},
		j.member, j.URL,
	).Text()
	if err != nil {
		return err
	}
	if res == "STALE" {
		return ErrStaleLease
	}
	return nil
}

func (f *redisFrontier) Nack(ctx context.Context, j *Job) error {
	res, err := scriptNack.Run(ctx, f.client,
		[]string{keyLease, keyReady, keyProcessing, keyJobSeq},
		j.member, j.URL,
	).Text()
	if err != nil {
		return err
	}
	if res == "STALE" {
		return ErrStaleLease
	}
	return nil
}

func (f *redisFrontier) Fail(ctx context.Context, j *Job, class FailClass) (Disposition, error) {
	now := f.now()

	incStrike, ladder, reason := "0", f.cfg.RateLimitBackoff, "rate_limited"
	if class == FailStrikeable {
		incStrike, ladder, reason = "1", f.cfg.StrikeBackoff, "strikes_exhausted"
	}

	args := []interface{}{
		j.member, j.URL, incStrike, f.cfg.MaxStrikes, now.Unix(), reason,
		f.quarantineUntil(j.Class, now),
	}
	args = append(args, backoffArgs(ladder)...)

	res, err := scriptFail.Run(ctx, f.client,
		[]string{keyLease, keyProcessing, keyAttempts, keyStrikes, keyDelayed, keyDead, keyJobSeq, keyListingNext},
		args...,
	).Text()
	if err != nil {
		return Delayed, err
	}
	switch res {
	case "STALE":
		return Delayed, ErrStaleLease
	case "DEAD":
		return Dead, nil
	case "DELAYED":
		return Delayed, nil
	default:
		return Delayed, fmt.Errorf("frontier: unexpected FAIL result %q", res)
	}
}

func (f *redisFrontier) Terminal(ctx context.Context, j *Job, reason string, until time.Time) error {
	res, err := scriptTerminal.Run(ctx, f.client,
		[]string{keyLease, keyProcessing, keyDead, keyAttempts, keyStrikes, keyListingNext},
		j.member, j.URL, reason, unixOrZero(until),
	).Text()
	if err != nil {
		return err
	}
	if res == "STALE" {
		return ErrStaleLease
	}
	return nil
}

func (f *redisFrontier) CompleteListing(ctx context.Context, j *Job, newChildren int) error {
	now := f.now()
	baseTTL := f.cfg.ListingBaseTTL
	if j.Class == ClassSeed {
		baseTTL = f.cfg.SeedTTL
	}

	res, err := scriptCompleteListing.Run(ctx, f.client,
		[]string{keyLease, keyProcessing, keyListingNext, keyListingBarren, keyDead, keyAttempts, keyStrikes},
		j.member, j.URL, newChildren, now.Unix(),
		int64(baseTTL/time.Second), int64(f.cfg.ListingMaxTTL/time.Second),
	).Text()
	if err != nil {
		return err
	}
	if res == "STALE" {
		return ErrStaleLease
	}
	return nil
}

// --- reconciler primitives ---

func (f *redisFrontier) Reap(ctx context.Context) (int, error) {
	// lease scores are expiries - pass now directly, no subtraction
	return scriptReap.Run(ctx, f.client,
		[]string{keyLease, keyReady, keyProcessing, keyJobSeq},
		f.now().Unix(), reapBatchLimit,
	).Int()
}

func (f *redisFrontier) Sweep(ctx context.Context) (int, error) {
	return scriptSweep.Run(ctx, f.client,
		[]string{keyProcessing, keyLease, keyReady, keyJobSeq},
		sweepBatchLimit,
	).Int()
}

func (f *redisFrontier) Promote(ctx context.Context) (int, error) {
	return scriptPromote.Run(ctx, f.client,
		[]string{keyDelayed, keyReady, keyJobSeq},
		f.now().Unix(), promoteBatchLimit,
	).Int()
}

func (f *redisFrontier) RunListingScheduler(ctx context.Context) (int, error) {
	return scriptListingScheduler.Run(ctx, f.client,
		[]string{keyListingNext, keyReady, keyJobSeq, keyDead},
		f.now().Unix(), f.cfg.SchedulerBatchLimit, PendingScore,
	).Int()
}

// --- observability ---

func (f *redisFrontier) Idle(ctx context.Context) (bool, error) {
	stats, err := f.Stats(ctx)
	if err != nil {
		return false, err
	}
	return stats.Ready == 0 && stats.Processing == 0 && stats.Delayed == 0, nil
}

func (f *redisFrontier) Stats(ctx context.Context) (FrontierStats, error) {
	pipe := f.client.Pipeline()
	ready := pipe.LLen(ctx, keyReady)
	processing := pipe.LLen(ctx, keyProcessing)
	delayed := pipe.ZCard(ctx, keyDelayed)
	dead := pipe.HLen(ctx, keyDead)
	strikes := pipe.HLen(ctx, keyStrikes)
	scheduled := pipe.ZCount(ctx, keyListingNext, "-inf", strconv.FormatInt(PendingScore-1, 10))
	pending := pipe.ZCount(ctx, keyListingNext,
		strconv.FormatInt(PendingScore, 10), strconv.FormatInt(PendingScore, 10))

	if _, err := pipe.Exec(ctx); err != nil {
		return FrontierStats{}, err
	}

	return FrontierStats{
		Ready:             ready.Val(),
		Processing:        processing.Val(),
		Delayed:           delayed.Val(),
		Dead:              dead.Val(),
		Strikes:           strikes.Val(),
		ListingsScheduled: scheduled.Val(),
		ListingsPending:   pending.Val(),
	}, nil
}
