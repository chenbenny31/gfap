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
	wg.Add(6)
	go runListingScheduler(ctx, frontier, schedulerBatchLimit, wg)
	go runPromoter(ctx, frontier, wg)
	go runReaper(ctx, frontier, wg)
	go runSweeper(ctx, frontier, wg)
	go runBloomMonitor(ctx, redis, stopFn, wg)
	go runHeartbeat(ctx, frontier, wg)
}

func runListingScheduler(ctx context.Context, f storage.Frontier, limit int, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(listingSchedulerPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Drain in full pages - a page coming back at exactly the
			// configured limit means more may still be due right now.
			for {
				n, err := f.RunListingScheduler(ctx)
				log.Printf("[DEBUG] listingScheduler: n=%d err=%v", n, err)
				if err != nil {
					metrics.ReconcileErrors.Inc()
					break
				}
				metrics.ListingsAdmittedTotal.Add(float64(n))
				if n < limit {
					break
				}
			}
		}
	}
}

func runPromoter(ctx context.Context, f storage.Frontier, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(promoterPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := f.Promote(ctx)
			log.Printf("[DEBUG] promoter: n=%d err=%v", n, err)
			if err != nil {
				metrics.ReconcileErrors.Inc()
				continue
			}
			metrics.PromotedTotal.Add(float64(n))
		}
	}
}

func runReaper(ctx context.Context, f storage.Frontier, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(reaperPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := f.Reap(ctx)
			log.Printf("[DEBUG] reaper: n=%d err=%v", n, err)
			if err != nil {
				metrics.ReconcileErrors.Inc()
				continue
			}
			metrics.ReapedTotal.Add(float64(n))
		}
	}
}

func runSweeper(ctx context.Context, f storage.Frontier, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(sweeperPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := f.Sweep(ctx)
			log.Printf("[DEBUG] sweeper: n=%d err=%v", n, err)
			if err != nil {
				metrics.ReconcileErrors.Inc()
				continue
			}
			metrics.SweptTotal.Add(float64(n))
		}
	}
}

// runBloomMonitor asserts the filter's shape every tick (Stop() on
// violation - see Run's doc comment) and warns once per fill-ratio
// threshold crossed. Fill only grows monotonically under NONSCALING (no
// shrink path exists), so "highest threshold already warned" never resets.
func runBloomMonitor(ctx context.Context, redis *storage.Redis, stopFn func(), wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(bloomMonitorPeriod)
	defer ticker.Stop()
	warned := 0 // index into bloomFillAlerts of the highest threshold already logged
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := redis.BloomVerify(ctx); err != nil {
				log.Printf("[ERROR] bloomMonitor: %v - stopping crawl", err)
				stopFn()
				return
			}
			ratio, err := redis.BloomFillRatio(ctx)
			if err != nil {
				metrics.ReconcileErrors.Inc()
				log.Printf("[DEBUG] bloomMonitor fill check: %v", err)
				continue
			}
			metrics.BloomFillRatio.Set(ratio)
			for warned < len(bloomFillAlerts) && ratio >= bloomFillAlerts[warned] {
				log.Printf("[WARN] bloom filter at %.0f%% capacity", bloomFillAlerts[warned]*100)
				warned++
			}
		}
	}
}

// runHeartbeat mirrors frontier depth and Idle() into gauges every tick.
// Idle() has no other caller now that idleMonitor's re-seed-on-idle role is
// gone - the listing scheduler handles revisiting on its own clock instead.
func runHeartbeat(ctx context.Context, f storage.Frontier, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(heartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := f.Stats(ctx)
			if err != nil {
				metrics.ReconcileErrors.Inc()
				log.Printf("[DEBUG] heartbeat stats: %v", err)
				continue
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
		}
	}
}
