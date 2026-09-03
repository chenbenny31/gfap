package storage

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	bloomKey = "crawler:bloom"

	bloomCapacity  = 1_000_000_000 // ~2.8 GiB at 0.001% FPR
	bloomErrorRate = 0.00001
)

type Redis struct {
	client *redis.Client
}

// NewRedis connects to addr on the given DB index. Test mode uses its own
// DB so InitTest's FlushDB physically cannot reach production's bloom
// filter or frontier state.
func NewRedis(addr string, db int) (*Redis, error) {
	client := redis.NewClient(&redis.Options{Addr: addr, DB: db})
	if _, err := client.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}
	return &Redis{client: client}, nil
}

// Client exposes the raw client so storage.NewFrontier can share the connection.
func (r *Redis) Client() *redis.Client { return r.client }

// --- bloom filter ---

// BloomInit reserves the filter NONSCALING if it doesn't exist yet - plain
// BFReserve can't set that flag, and a scaling filter silently degrades FPR.
// created reports whether this call actually created the key (false when it
// already existed) - the caller's signal for whether a Bloom rebuild from
// MongoDB is warranted, as opposed to a BloomVerify failure (config
// mismatch, not data loss - that case must Stop(), not rebuild).
func (r *Redis) BloomInit(ctx context.Context) (created bool, err error) {
	err = r.client.BFReserveWithArgs(ctx, bloomKey, &redis.BFReserveOptions{
		Capacity:   bloomCapacity,
		Error:      bloomErrorRate,
		NonScaling: true,
	}).Err()
	if err != nil && strings.Contains(err.Error(), "exists") {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// bloomStats holds the BF.INFO fields we care about.
//
// Read field-by-field rather than with a single BFInfo call: go-redis parses
// every BF.INFO field with ReadInt, but RedisBloom reports a nil "Expansion
// rate" for NONSCALING filters, so BFInfo fails with redis.Nil against
// exactly the filter shape BloomInit creates. The per-field variants never
// touch that field.
type bloomStats struct {
	capacity      int64
	size          int64
	filters       int64
	itemsInserted int64
}

func (r *Redis) bloomStats(ctx context.Context) (bloomStats, error) {
	pipe := r.client.Pipeline()
	capacity := pipe.BFInfoCapacity(ctx, bloomKey)
	size := pipe.BFInfoSize(ctx, bloomKey)
	filters := pipe.BFInfoFilters(ctx, bloomKey)
	items := pipe.BFInfoItems(ctx, bloomKey)
	if _, err := pipe.Exec(ctx); err != nil {
		return bloomStats{}, err
	}
	return bloomStats{
		capacity:      capacity.Val().Capacity,
		size:          size.Val().Size,
		filters:       filters.Val().Filters,
		itemsInserted: items.Val().ItemsInserted,
	}, nil
}

// BloomVerify checks capacity, sub-filter count, and implied FPR (a config
// sanity check, not a runtime measurement) against the configured target.
func (r *Redis) BloomVerify(ctx context.Context) error {
	info, err := r.bloomStats(ctx)
	if err != nil {
		return fmt.Errorf("bloom filter missing or unreadable: %w", err)
	}
	if info.capacity != bloomCapacity {
		return fmt.Errorf(
			"bloom capacity is %d, want %d (auto-created default or stale reserve) - run: make reset-bloom",
			info.capacity, bloomCapacity)
	}
	if info.filters > 1 {
		return fmt.Errorf(
			"bloom has scaled to %d sub-filters, FPR is degraded - run: make reset-bloom",
			info.filters)
	}

	bitsPerElement := float64(info.size*8) / float64(info.capacity)
	impliedFPR := math.Pow(0.6185, bitsPerElement)
	const tolerance = 2.0
	if impliedFPR > bloomErrorRate*tolerance {
		return fmt.Errorf(
			"bloom implied FPR %.3e exceeds %.1fx tolerance of target %.3e - run: make reset-bloom",
			impliedFPR, tolerance, bloomErrorRate)
	}
	return nil
}

// BloomFillRatio returns capacity used, for bloomMonitor's fill alerts.
func (r *Redis) BloomFillRatio(ctx context.Context) (float64, error) {
	info, err := r.bloomStats(ctx)
	if err != nil {
		return 0, err
	}
	return float64(info.itemsInserted) / float64(info.capacity), nil
}

// BloomAddBatch pipelines BF.ADD for a slice of URLs - rebuilds the filter
// from Mongo after a Redis data-loss event. Callers must gate this on the
// key having actually been missing at startup (BloomInit took the create
// path, not the "exists" branch) or an explicit --rebuild-bloom flag - never
// call it unconditionally, and never in response to a BloomVerify failure
// (that's a config mismatch and must Stop() instead, not a data-loss signal).
func (r *Redis) BloomAddBatch(ctx context.Context, urls []string) error {
	pipe := r.client.Pipeline()
	for _, url := range urls {
		pipe.BFAdd(ctx, bloomKey, url)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// BloomReset deletes only the bloom filter - frontier and Mongo untouched.
func (r *Redis) BloomReset(ctx context.Context) error {
	return r.client.Del(ctx, bloomKey).Err()
}

// Snapshot asks redis to write an RDB in the background. Automatic save
// points are disabled in favour of this slow explicit cadence: every RDB
// re-serialises the whole bloom filter, so the stock rules spend hundreds of
// GB/day of writes to persist a few thousand changed bits.
func (r *Redis) Snapshot(ctx context.Context) error {
	return r.client.BgSave(ctx).Err()
}

func (r *Redis) FlushDB(ctx context.Context) error {
	return r.client.FlushDB(ctx).Err()
}

func (r *Redis) Close() {
	r.client.Close()
}
