package crawler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gfap/internal/auth"
	"gfap/internal/config"
	"gfap/internal/model"
	"gfap/internal/scheduler"
	"gfap/internal/storage"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	maxTestVideos = 200             // test mode only
	testDeadline  = 2 * time.Minute // hard stop for a test run
)

type Crawler struct {
	cfg      *config.Config
	redis    *storage.Redis
	mongo    *storage.Mongo
	frontier storage.Frontier
	jar      http.CookieJar
	targets  []model.Video
	mu       sync.Mutex
	count    int

	// test only; observations are protected by mu
	debug        bool
	testActive   map[string]int
	testParsed   []int
	testErr      error
	testStored   []int
	testRetired  []bool
	testFinished int
}

func New(cfg *config.Config, redis *storage.Redis, mongo *storage.Mongo, frontier storage.Frontier) *Crawler {
	return &Crawler{
		cfg:      cfg,
		redis:    redis,
		mongo:    mongo,
		frontier: frontier,
		jar:      auth.NewJar(),
	}
}

func (c *Crawler) Count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count
}

func (c *Crawler) TargetCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.targets)
}

// Resume reloads in-memory stats (count/targets) from the MongoDB corpus on
// startup. It no longer touches Bloom - a lost/rebuilt filter is main.go's
// concern (gated on BloomInit's create-vs-exists result), not every resume.
func (c *Crawler) Resume() {
	ctx := context.Background()
	n, err := c.mongo.Count(ctx)
	if err != nil {
		log.Printf("[ERROR] Resume failed: %v\n", err)
		return
	}
	targets, err := c.mongo.FindTargets(ctx)
	if err != nil {
		log.Printf("[ERROR] Resume failed: %v\n", err)
		return
	}
	c.targets = append(c.targets, targets...)
	c.count = int(n)
	log.Printf("[INFO] Resumed %d videos, %d targets\n", c.count, len(c.targets))
}

func (c *Crawler) Clear() {
	ctx := context.Background()
	n, _ := c.mongo.Count(ctx)
	log.Printf("[DANGER] dropping MongoDB corpus (%d docs) and flushing redis\n", n)
	c.mongo.Drop(ctx)
	c.redis.FlushDB(ctx)
	log.Println("[INFO] Cleared MongoDB and Redis")
}

// canonicalize returns the canonical form of a vidlli url, or "" to skip
func (c *Crawler) canonicalize(raw string) string {
	// strip fragment
	if i := strings.Index(raw, "#"); i >= 0 {
		raw = raw[:i]
	}

	// should be on the same host
	if !strings.HasPrefix(raw, c.cfg.BaseUrl) {
		return ""
	}

	// video page: keep only ?=v<id>
	if idx := strings.Index(raw, c.cfg.VideoPattern); idx >= 0 {
		rest := raw[idx+len(c.cfg.VideoPattern):]
		if rest == "" {
			return ""
		}
		if amp := strings.Index(rest, "&"); amp >= 0 {
			rest = rest[:amp]
		}
		return c.cfg.BaseUrl + c.cfg.VideoPattern + rest
	}

	// non-video page
	return strings.TrimRight(raw, "/")
}

// Seed admits every URL in path (one per line) directly, for -fresh startup.
func (c *Crawler) Seed(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[WARN] Crawler seeding failed: %v\n", err)
		return
	}
	ctx := context.Background()
	count := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if _, err := c.admitURL(ctx, line); err != nil {
			log.Printf("[WARN] seed admit failed for %s: %v\n", strings.TrimSpace(line), err)
			continue
		}
		count++
	}
	log.Printf("[INFO] seeded %d URLs from %s\n", count, path)
}

func (c *Crawler) Login() error {
	client := auth.NewClient(c.jar, "")
	return client.Login(c.cfg.LoginURL, c.cfg.Username, c.cfg.Password)
}

// --- test only ---

// RunTest performs the bounded parsing phase. It returns an error if the
// phase is cancelled, times out, or drains before reaching its video target.
// Workers and reconcilers are stopped and joined before it returns.
func (c *Crawler) RunTest(parent context.Context, seedURL string) error {
	if c.cfg.Workers < 2 {
		return errors.New("test requires at least two workers")
	}
	if c.cfg.RateLimit < 30*time.Second {
		return errors.New("test requires at least 30 seconds between worker requests")
	}
	for i := 0; i < c.cfg.Workers; i++ {
		if i >= len(c.cfg.StaticProxyURLs) || strings.TrimSpace(c.cfg.StaticProxyURLs[i]) == "" {
			return fmt.Errorf("test worker %d has no proxy", i)
		}
	}

	c.debug = true
	c.mu.Lock()
	c.testActive = make(map[string]int)
	c.testParsed = make([]int, c.cfg.Workers)
	c.testErr = nil
	c.testStored = make([]int, c.cfg.Workers)
	c.testRetired = make([]bool, c.cfg.Workers)
	c.testFinished = 0
	c.mu.Unlock()
	ctx, cancel := context.WithTimeout(parent, testDeadline)
	defer cancel()

	admitted, err := c.admitURL(ctx, seedURL)
	if err != nil {
		return fmt.Errorf("test seed admission: %w", err)
	}
	if admitted == storage.Suppressed {
		return errors.New("test seed was not admitted")
	}
	duplicate, err := c.admitURL(ctx, seedURL)
	if err != nil {
		return fmt.Errorf("test duplicate seed admission: %w", err)
	}
	if duplicate != storage.Suppressed {
		return errors.New("test duplicate seed was admitted twice")
	}

	var wg sync.WaitGroup
	scheduler.Run(ctx, c.frontier, c.redis, c.cfg.SchedulerBatchLimit, cancel, &wg)
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer func() {
				c.mu.Lock()
				c.testFinished++
				c.mu.Unlock()
				wg.Done()
			}()
			offset := time.Duration(workerID) * c.cfg.RateLimit / time.Duration(c.cfg.Workers)
			sleepCtx(ctx, offset)
			if ctx.Err() != nil {
				return
			}
			c.workerTest(ctx, workerID, cancel)
		}(i)
	}

	var runErr error
	for c.Count() < maxTestVideos {
		if c.hasTestFailure() {
			break
		}
		c.mu.Lock()
		finished := c.testFinished
		c.mu.Unlock()
		if finished == c.cfg.Workers {
			if c.Count() < maxTestVideos {
				runErr = errors.New("all test workers stopped before reaching the video target")
			}
			break
		}
		if ctx.Err() != nil {
			runErr = ctx.Err()
			break
		}
		idle, err := c.frontier.Idle(ctx)
		if err != nil {
			runErr = fmt.Errorf("test frontier inspection: %w", err)
			break
		}
		if idle {
			if count := c.Count(); count < maxTestVideos {
				runErr = fmt.Errorf("test frontier drained after %d videos; need %d", count, maxTestVideos)
			}
			break
		}
		sleepCtx(ctx, 100*time.Millisecond)
	}
	if runErr == nil {
		runErr = ctx.Err()
	}
	cancel()
	wg.Wait()

	if err := errors.Join(runErr, parent.Err(), c.validateTestWorkers()); err != nil {
		return err
	}
	verifyCtx, verifyCancel := context.WithTimeout(parent, 30*time.Second)
	defer verifyCancel()
	stored, err := c.mongo.Count(verifyCtx)
	if err != nil {
		return fmt.Errorf("test Mongo verification: %w", err)
	}
	if stored < maxTestVideos || stored != int64(c.Count()) {
		return fmt.Errorf("test stored %d unique videos for %d recorded writes; need at least %d",
			stored, c.Count(), maxTestVideos)
	}
	log.Printf("[INFO] Bounded crawl complete - %d videos, %d targets\n", c.Count(), c.TargetCount())
	return nil
}

// Test observations use the existing mutex and never change frontier decisions.
func (c *Crawler) recordTestStart(url string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.testActive[url] > 0 && c.testErr == nil {
		c.testErr = fmt.Errorf("test detected concurrent processing of %q", url)
	}
	c.testActive[url]++
}

func (c *Crawler) recordTestFinish(workerID int, url string, kind pageKind) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.testActive[url]--
	if c.testActive[url] == 0 {
		delete(c.testActive, url)
	}
	if kind == pageVideo || kind == pageListing {
		c.testParsed[workerID]++
	}
}

func (c *Crawler) hasTestFailure() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.testErr != nil
}

func (c *Crawler) validateTestWorkers() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := c.testErr
	if len(c.testActive) != 0 {
		result = errors.Join(result, errors.New("test finished with unfinished processing observations"))
	}
	for workerID, parsed := range c.testParsed {
		status := "video_stored"
		switch {
		case c.testRetired[workerID]:
			status = "rate_limited"
			result = errors.Join(result, fmt.Errorf("test worker %d stopped after consecutive rate limits", workerID))
		case c.testStored[workerID] == 0:
			status = "unverified"
			result = errors.Join(result, fmt.Errorf("test worker %d did not store a video", workerID))
		}
		log.Printf("[INFO] Test worker=%d proxy_slot=%d parsed_pages=%d stored_videos=%d result=%s",
			workerID, workerID, parsed, c.testStored[workerID], status)
	}
	return result
}

func (c *Crawler) SaveTest(parent context.Context) (err error) {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()

	targets, err := c.mongo.FindTargets(ctx)
	if err != nil {
		return err
	}
	f, err := os.Create(c.cfg.OutputFile)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, f.Close())
	}()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(targets)
}

func (c *Crawler) InitTest(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()

	if err := c.redis.FlushDB(ctx); err != nil {
		return fmt.Errorf("test Redis reset: %w", err)
	}
	log.Println("[INFO] Init test completed: Redis cleared")
	return nil
}
