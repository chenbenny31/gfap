package storage

import (
	"context"
	"testing"
)

const (
	testRedisAddr = "localhost:6379"
	scratchDB     = 15 // never production (0) or test mode (1)
)

func testRedis(t *testing.T) *Redis {
	t.Helper()
	r, err := NewRedis(testRedisAddr, scratchDB)
	if err != nil {
		t.Skipf("redis unavailable at %s: %v", testRedisAddr, err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

// TestBloomLifecycleNonScaling covers the NONSCALING/BF.INFO trap: RedisBloom
// reports a nil "Expansion rate" for a non-scaling filter, and go-redis's
// aggregate BFInfo parses every field with ReadInt, so it fails with
// redis.Nil against exactly the filter shape BloomInit creates. Before the
// per-field read, BloomVerify rejected a perfectly healthy filter as
// "missing or unreadable", which made the crawler unbootable.
func TestBloomLifecycleNonScaling(t *testing.T) {
	ctx := context.Background()
	r := testRedis(t)
	if err := r.FlushDB(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	created, err := r.BloomInit(ctx)
	if err != nil {
		t.Fatalf("BloomInit: %v", err)
	}
	if !created {
		t.Fatal("BloomInit reported created=false on an empty DB")
	}

	if err := r.BloomVerify(ctx); err != nil {
		t.Fatalf("BloomVerify on a freshly created NONSCALING filter: %v", err)
	}

	ratio, err := r.BloomFillRatio(ctx)
	if err != nil {
		t.Fatalf("BloomFillRatio: %v", err)
	}
	if ratio != 0 {
		t.Errorf("fill ratio of an empty filter = %v, want 0", ratio)
	}

	if err := r.BloomAddBatch(ctx, []string{"a", "b", "c"}); err != nil {
		t.Fatalf("BloomAddBatch: %v", err)
	}
	if ratio, err = r.BloomFillRatio(ctx); err != nil {
		t.Fatalf("BloomFillRatio after add: %v", err)
	}
	if want := 3.0 / bloomCapacity; ratio != want {
		t.Errorf("fill ratio after 3 inserts = %v, want %v", ratio, want)
	}
}

// TestBloomInitCreatedFlag pins the signal the Mongo-rebuild gate keys on:
// created is true only when this call actually reserved the filter.
func TestBloomInitCreatedFlag(t *testing.T) {
	ctx := context.Background()
	r := testRedis(t)
	if err := r.FlushDB(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	if created, err := r.BloomInit(ctx); err != nil || !created {
		t.Fatalf("first BloomInit: created=%v err=%v, want created=true", created, err)
	}
	if created, err := r.BloomInit(ctx); err != nil || created {
		t.Fatalf("second BloomInit: created=%v err=%v, want created=false", created, err)
	}
}

// TestBloomVerifyRejectsMisconfigured proves the guard still fires: a filter
// reserved with the wrong error rate has a degraded implied FPR and must be
// rejected rather than silently accepted.
func TestBloomVerifyRejectsMisconfigured(t *testing.T) {
	ctx := context.Background()
	r := testRedis(t)
	if err := r.FlushDB(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// 0.01 error rate instead of the configured 0.00001
	if err := r.client.BFReserve(ctx, bloomKey, 0.01, bloomCapacity).Err(); err != nil {
		t.Fatalf("BFReserve: %v", err)
	}
	if err := r.BloomVerify(ctx); err == nil {
		t.Fatal("BloomVerify accepted a filter reserved at error rate 0.01, want rejection")
	}
}
