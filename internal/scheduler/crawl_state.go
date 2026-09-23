package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"gfap/internal/metrics"
	"gfap/internal/storage"
)

const crawlStateCheckpointPeriod = 10 * time.Minute

// RunCrawlStateCheckpoint starts the production checkpoint reconciler after Restore
// succeeds. everyTick serializes passes and joins shutdown through the caller's
// WaitGroup. Test-mode crawling does not register this reconciler.
func RunCrawlStateCheckpoint(ctx context.Context, store *storage.CrawlStateStore, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		var lastSuccess time.Time
		var lastStats storage.CheckpointStats
		everyTick(ctx, crawlStateCheckpointPeriod, wg, func() {
			start := time.Now()
			stats, err := store.Checkpoint(ctx)
			if err != nil {
				metrics.ReconcileErrors.Inc()
				last := "none"
				if !lastSuccess.IsZero() {
					last = lastSuccess.UTC().Format(time.RFC3339)
				}
				log.Printf("[WARN] crawl state checkpoint incomplete: err=%v partial=%+v last_success=%s last_counts=%+v", err, stats, last, lastStats)
				return
			}
			lastSuccess, lastStats = time.Now(), stats
			log.Printf("[INFO] crawl state checkpoint complete: observed=%d batches=%d upserted=%d modified=%d duration=%s completed=%s",
				stats.Observed, stats.Batches, stats.Upserted, stats.Modified,
				time.Since(start).Round(time.Millisecond), lastSuccess.UTC().Format(time.RFC3339))
		})
	}()
}
