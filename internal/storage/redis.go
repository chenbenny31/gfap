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

// NewRedis connects to addr on the default DB.
func NewRedis(addr string) (*Redis, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
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

// BloomVerify checks capacity, sub-filter count, and implied FPR (a config
// sanity check, not a runtime measurement) against the configured target.
func (r *Redis) BloomVerify(ctx context.Context) error {
	info, err := r.client.BFInfo(ctx, bloomKey).Result()
	if err != nil {
		return fmt.Errorf("bloom filter missing or unreadable: %w", err)
	}
	if info.Capacity != bloomCapacity {
		return fmt.Errorf(
			"bloom capacity is %d, want %d (auto-created default or stale reserve) - run: make reset-bloom",
			info.Capacity, bloomCapacity)
	}
	if info.Filters > 1 {
		return fmt.Errorf(
			"bloom has scaled to %d sub-filters, FPR is degraded - run: make reset-bloom",
			info.Filters)
	}

	bitsPerElement := float64(info.Size*8) / float64(info.Capacity)
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
	info, err := r.client.BFInfo(ctx, bloomKey).Result()
	if err != nil {
		return 0, err
	}
	return float64(info.ItemsInserted) / float64(info.Capacity), nil
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

func (r *Redis) FlushDB(ctx context.Context) error {
	return r.client.FlushDB(ctx).Err()
}

func (r *Redis) Close() {
	r.client.Close()
}
