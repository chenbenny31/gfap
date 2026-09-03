package main

import (
	"context"
	"flag"
	"fmt"
	"gfap/internal/metrics"
	"gfap/internal/scheduler"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"gfap/internal/config"
	"gfap/internal/crawler"
	"gfap/internal/storage"
)

// Redis DB indexes. Test mode is isolated from production so a test-mode
// FlushDB cannot reach the live bloom filter or frontier.
const (
	prodRedisDB = 0
	testRedisDB = 1

	bloomReconcileBatch = 5000
)

func main() {
	testMode := flag.Bool("test", false, "run bounded test crawl")
	freshMode := flag.Bool("fresh", false, "first run: seed from seeds.txt")
	flag.Parse()

	cfg := config.Load()

	logFile, err := os.OpenFile("crawler.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer logFile.Close()

	if *testMode {
		log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	} else {
		log.SetOutput(logFile)
	}

	redisDB := prodRedisDB
	if *testMode {
		redisDB = testRedisDB
	}
	redis, err := storage.NewRedis(cfg.RedisAddr, redisDB)
	if err != nil {
		log.Fatal(err)
	}
	defer redis.Close()

	mongo, err := storage.NewMongo(cfg.MongoURI, cfg.MongoDB, cfg.MongoCol)
	if err != nil {
		log.Fatal(err)
	}
	defer mongo.Close()

	classify := crawler.NewURLClassifier(cfg, readSeedURLs("seeds.txt", cfg.BaseUrl))
	frontier := storage.NewFrontier(redis.Client(), frontierConfig(cfg), classify)
	c := crawler.New(cfg, redis, mongo, frontier)

	ctx, cancel := context.WithCancel(context.Background())
	go metrics.Serve(cfg.MetricsPort, cancel)
	// handle SIGTERM (k8s pod deletion) and SIGINT
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		received := <-sig
		log.Printf("[INFO] Signal received: %s, stopping crawler\n", received)
		cancel()
	}()
	if err := c.Login(); err != nil {
		log.Fatalf("[FATAL] login failed: %v", err)
	}
	log.Println("[INFO] crawler logged in successfully")

	if *testMode {
		log.Println("[INFO] Running in test mode")
		c.InitTest()
		if _, err := redis.BloomInit(ctx); err != nil { // Bloom needed after InitTest's FlushDB
			log.Fatalf("[ERROR] Bloom init failed: %v\n", err)
		}
		c.RunTest(cfg.TestUrl)
		if err := c.SaveTest(); err != nil {
			log.Printf("[ERROR] Failed to save crawler data: %v", err)
		}
		res := fmt.Sprintf("Visited %d videos, target %d\n", c.Count(), c.TargetCount())
		log.Print(res)
		fmt.Print(res)
		return
	}

	created, err := redis.BloomInit(ctx)
	if err != nil {
		log.Fatalf("[ERROR] Bloom init failed: %v\n", err)
	}
	if err := redis.BloomVerify(ctx); err != nil {
		log.Fatalf("[ERROR] Bloom verify failed: %v\n", err)
	}
	if created {
		log.Println("[WARN] bloom filter was missing at startup - rebuilding from MongoDB")
	}
	reconcileBloomFromMongo(ctx, redis, mongo)
	if err := frontier.SeedListing(ctx, cfg.BaseUrl); err != nil {
		log.Fatalf("[ERROR] failed to seed listing schedule: %v\n", err)
	}

	log.Println("[INFO] Running in production mode")
	c.Resume()
	if *freshMode {
		c.Seed("seeds.txt")
	}

	var wg sync.WaitGroup
	scheduler.Run(ctx, frontier, redis, cfg.SchedulerBatchLimit, cancel, &wg)
	c.Run(ctx, cancel, &wg)
	<-ctx.Done()
	wg.Wait()
	log.Printf("[INFO] Crawler stopped - %d videos, %d targets\n", c.Count(), c.TargetCount())
}

func frontierConfig(cfg *config.Config) storage.FrontierConfig {
	return storage.FrontierConfig{
		LeaseTimeout:        time.Duration(cfg.LeaseTimeoutSec) * time.Second,
		MaxStrikes:          cfg.MaxStrikes,
		RateLimitBackoff:    cfg.RateLimitBackoffSec,
		StrikeBackoff:       cfg.StrikeBackoffSec,
		ListingBaseTTL:      time.Duration(cfg.ListingBaseTTLHours) * time.Hour,
		SeedTTL:             time.Duration(cfg.SeedTTLHours) * time.Hour,
		ListingMaxTTL:       time.Duration(cfg.ListingMaxTTLHours) * time.Hour,
		QuarantineListing:   time.Duration(cfg.QuarantineListingHours) * time.Hour,
		SchedulerBatchLimit: cfg.SchedulerBatchLimit,
	}
}

// readSeedURLs returns baseURL plus every non-empty line in path, for the
// URL classifier's ClassSeed membership check. Read unconditionally (not
// gated on -fresh) since classification must be stable across restarts
// regardless of whether this particular run admits the seeds.
func readSeedURLs(path, baseURL string) []string {
	urls := []string{baseURL}
	data, err := os.ReadFile(path)
	if err != nil {
		return urls
	}
	for _, line := range strings.Split(string(data), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			urls = append(urls, line)
		}
	}
	return urls
}

// reconcileBloomFromMongo re-adds every stored video URL to the filter.
//
// Runs on every startup, not only when the filter was missing. RDB snapshots
// are taken a few times a day, so a crash restores a bloom that is stale
// rather than absent - and the gap is exactly the videos MongoDB already
// holds. BF.ADD is idempotent, so anything already present costs nothing.
func reconcileBloomFromMongo(ctx context.Context, redis *storage.Redis, mongo *storage.Mongo) {
	start := time.Now()
	n := 0
	err := mongo.EachVideoURL(ctx, bloomReconcileBatch, func(urls []string) error {
		n += len(urls)
		return redis.BloomAddBatch(ctx, urls)
	})
	if err != nil {
		log.Fatalf("[ERROR] bloom reconcile failed after %d urls: %v\n", n, err)
	}
	log.Printf("[INFO] bloom reconciled against %d MongoDB documents in %s\n",
		n, time.Since(start).Round(time.Millisecond))
}
