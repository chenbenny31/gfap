package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"gfap/internal/metrics"
	"gfap/internal/storage"
)

// Reconciler periods.
const (
	listingSchedulerPeriod = 60 * time.Second
	promoterPeriod         = 10 * time.Second
	reaperPeriod           = 60 * time.Second
	sweeperPeriod          = 60 * time.Second
	bloomMonitorPeriod     = 5 * time.Minute
	heartbeatPeriod        = 5 * time.Minute
	snapshotPeriod         = 6 * time.Hour
)

// bloomFillAlerts are the fill-ratio thresholds bloomMonitor warns at, each
// one once, in ascending order.
var bloomFillAlerts = []float64{0.80, 0.90, 0.95}

// Run starts the five reconciler goroutines plus the heartbeat gauge
// updater on wg, stopping when ctx is cancelled. schedulerBatchLimit must
// match the value baked into frontier's FrontierConfig - RunListingScheduler
// doesn't expose it, so the caller passes the same number it used to build
// the frontier. stopFn is invoked once, from bloomMonitor, if the live
// filter's shape stops matching config: a broken Bloom halts the crawl
// rather than silently degrading admission dedup - the same policy as the
// worker's BF.ADD-error case, just caught here on a slower cadence.
func Run(ctx context.Context, frontier storage.Frontier, redis *storage.Redis, schedulerBatchLimit int, stopFn func(), wg *sync.WaitGroup) {
	wg.Add(7)
	go runListingScheduler(ctx, frontier, schedulerBatchLimit, wg)
	go runPromoter(ctx, frontier, wg)
	go runReaper(ctx, frontier, wg)
	go runSweeper(ctx, frontier, wg)
	go runBloomMonitor(ctx, redis, stopFn, wg)
	go runHeartbeat(ctx, frontier, wg)
	go runSnapshotter(ctx, redis, wg)
}

// runSnapshotter drives redis persistence on a slow explicit cadence, since
// automatic save points are off. What the gap between snapshots costs is
// duplicate work, not lost data: the bloom is reconciled from MongoDB at
// startup and the frontier re-derives itself by crawling.
func runSnapshotter(ctx context.Context, redis *storage.Redis, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(snapshotPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := redis.Snapshot(ctx); err != nil {
				metrics.ReconcileErrors.Inc()
				log.Printf("[WARN] snapshot: %v", err)
				continue
			}
			log.Println("[INFO] redis snapshot requested")
		}
	}
}

// everyTick runs fn immediately, then once per period until ctx is done.
// time.NewTicker does not fire until a full period has elapsed, which would
// otherwise leave a freshly started crawler idle for 60s - nothing promotes
// the due seed into ready - and unmonitored for the first 5 minutes.
func everyTick(ctx context.Context, period time.Duration, wg *sync.WaitGroup, fn func()) {
	defer wg.Done()
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		fn()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// logTick reports a reconciler tick only when it did something or failed. A
// no-op line every 10s would otherwise be ~13k lines/day of pure noise, and
// on a disk-I/O-metered VPS the log is a write stream like any other.
func logTick(name string, n int, err error) {
	if err != nil {
		log.Printf("[DEBUG] %s: n=%d err=%v", name, n, err)
		return
	}
	if n > 0 {
		log.Printf("[DEBUG] %s: n=%d", name, n)
	}
}

func runListingScheduler(ctx context.Context, f storage.Frontier, limit int, wg *sync.WaitGroup) {
	everyTick(ctx, listingSchedulerPeriod, wg, func() {
		// Drain in full pages - a page coming back at exactly the configured
		// limit means more may still be due right now.
		for {
			n, err := f.RunListingScheduler(ctx)
			logTick("listingScheduler", n, err)
			if err != nil {
				metrics.ReconcileErrors.Inc()
				return
			}
			metrics.ListingsAdmittedTotal.Add(float64(n))
			if n < limit {
				return
			}
		}
	})
}

func runPromoter(ctx context.Context, f storage.Frontier, wg *sync.WaitGroup) {
	everyTick(ctx, promoterPeriod, wg, func() {
		n, err := f.Promote(ctx)
		logTick("promoter", n, err)
		if err != nil {
			metrics.ReconcileErrors.Inc()
			return
		}
		metrics.PromotedTotal.Add(float64(n))
	})
}

func runReaper(ctx context.Context, f storage.Frontier, wg *sync.WaitGroup) {
	everyTick(ctx, reaperPeriod, wg, func() {
		n, err := f.Reap(ctx)
		logTick("reaper", n, err)
		if err != nil {
			metrics.ReconcileErrors.Inc()
			return
		}
		metrics.ReapedTotal.Add(float64(n))
	})
}

func runSweeper(ctx context.Context, f storage.Frontier, wg *sync.WaitGroup) {
	everyTick(ctx, sweeperPeriod, wg, func() {
		n, err := f.Sweep(ctx)
		logTick("sweeper", n, err)
		if err != nil {
			metrics.ReconcileErrors.Inc()
			return
		}
		metrics.SweptTotal.Add(float64(n))
	})
}

// runBloomMonitor asserts the filter's shape every tick (Stop() on
// violation - see Run's doc comment) and warns once per fill-ratio
// threshold crossed. Fill only grows monotonically under NONSCALING (no
// shrink path exists), so "highest threshold already warned" never resets.
func runBloomMonitor(ctx context.Context, redis *storage.Redis, stopFn func(), wg *sync.WaitGroup) {
	warned := 0 // index into bloomFillAlerts of the highest threshold already logged
	everyTick(ctx, bloomMonitorPeriod, wg, func() {
		if err := redis.BloomVerify(ctx); err != nil {
			log.Printf("[ERROR] bloomMonitor: %v - stopping crawl", err)
			stopFn()
			return
		}
		ratio, err := redis.BloomFillRatio(ctx)
		if err != nil {
			metrics.ReconcileErrors.Inc()
			log.Printf("[DEBUG] bloomMonitor fill check: %v", err)
			return
		}
		metrics.BloomFillRatio.Set(ratio)
		for warned < len(bloomFillAlerts) && ratio >= bloomFillAlerts[warned] {
			log.Printf("[WARN] bloom filter at %.0f%% capacity", bloomFillAlerts[warned]*100)
			warned++
		}
	})
}

// runHeartbeat mirrors frontier depth and Idle() into gauges every tick.
// Idle() has no other caller now that idleMonitor's re-seed-on-idle role is
// gone - the listing scheduler handles revisiting on its own clock instead.
func runHeartbeat(ctx context.Context, f storage.Frontier, wg *sync.WaitGroup) {
	everyTick(ctx, heartbeatPeriod, wg, func() {
		stats, err := f.Stats(ctx)
		if err != nil {
			metrics.ReconcileErrors.Inc()
			log.Printf("[DEBUG] heartbeat stats: %v", err)
			return
		}
		metrics.FrontierReady.Set(float64(stats.Ready))
		metrics.FrontierProcessing.Set(float64(stats.Processing))
		metrics.FrontierDelayed.Set(float64(stats.Delayed))
		metrics.ListingsScheduled.Set(float64(stats.ListingsScheduled))
		metrics.ListingsPending.Set(float64(stats.ListingsPending))
		metrics.FrontierDead.Set(float64(stats.Dead))
		metrics.FrontierStrikes.Set(float64(stats.Strikes))

		if idle, err := f.Idle(ctx); err == nil {
			if idle {
				metrics.FrontierIdle.Set(1)
			} else {
				metrics.FrontierIdle.Set(0)
			}
		}
	})
}
