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
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	maxTestVideos = 200              // test mode only
	testDeadline  = 10 * time.Minute // hard stop for a test run
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
	testRetired  []string
	testFailures []map[string]int
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
	base, err := url.Parse(c.cfg.BaseUrl)
	if err != nil {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil ||
		u.Scheme != base.Scheme ||
		!strings.EqualFold(u.Host, base.Host) {
		return ""
	}

	u.Fragment = ""
	u.Host = base.Host
	path := strings.TrimRight(u.Path, "/")

	switch path {
	case "/watch":
		id := u.Query().Get("v")
		if id == "" || strings.ContainsAny(id,
			" \t\r\n/?&#%\\") {
			return ""
		}
		return c.cfg.BaseUrl + c.cfg.VideoPattern +
			url.QueryEscape(id)

	case "", "/videos", "/channels", "/results",
		"/community", "/special_videos", "/contests":
		// Public discovery pages.

	default:
		if !strings.HasPrefix(path, "/user/") &&
			!strings.HasPrefix(path, "/contest/") {
			return ""
		}
	}

	u.Path = path
	u.RawPath = ""
	return u.String()
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
	c.testRetired = make([]string, c.cfg.Workers)
	c.testFailures = make([]map[string]int, c.cfg.Workers)
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

func (c *Crawler) recordTestFinish(workerID int, url string, out pageOutcome) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.testActive[url]--
	if c.testActive[url] == 0 {
		delete(c.testActive, url)
	}
	if out.kind == pageVideo || out.kind == pageListing {
		c.testParsed[workerID]++
	}
	if out.kind == pageRetryable || out.kind == pageInvalidResponse || out.kind == pageUnknown {
		if c.testFailures[workerID] == nil {
			c.testFailures[workerID] = make(map[string]int)
		}
		reason := out.reason
		if reason == "" {
			reason = "unknown_response"
		}
		c.testFailures[workerID][reason]++
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
		case c.testRetired[workerID] != "":
			status = c.testRetired[workerID]
			result = errors.Join(result, fmt.Errorf("test worker %d stopped: %s", workerID, status))
		case c.testStored[workerID] == 0:
			status = "unverified"
			result = errors.Join(result, fmt.Errorf("test worker %d did not store a video", workerID))
		}
		log.Printf("[INFO] Test worker=%d proxy_slot=%d parsed_pages=%d stored_videos=%d result=%s",
			workerID, workerID, parsed, c.testStored[workerID], status)
	}
	return result
}

// TestWorkerSummary reports evidence from a completed test without changing its result.
func (c *Crawler) TestWorkerSummary() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var rows strings.Builder
	verified, unverified, retired, withErrors, writes := 0, 0, 0, 0, 0
	for id, parsed := range c.testParsed {
		stored := c.testStored[id]
		writes += stored
		status := "video_stored"
		if c.testRetired[id] != "" {
			status = c.testRetired[id]
			retired++
		} else if stored == 0 {
			status = "unverified"
			unverified++
		} else {
			verified++
		}
		failures := c.testFailures[id]
		if len(failures) != 0 {
			withErrors++
		}
		if status == "video_stored" && len(failures) == 0 {
			continue
		}
		keys := make([]string, 0, len(failures))
		for reason := range failures {
			keys = append(keys, reason)
		}
		sort.Strings(keys)
		fmt.Fprintf(&rows, "\nworker=%d proxy_slot=%d result=%s parsed_pages=%d stored_videos=%d", id, id, status, parsed, stored)
		if len(keys) == 0 {
			rows.WriteString(" fetch_parse_errors=none")
		}
		for _, reason := range keys {
			fmt.Fprintf(&rows, " %s=%d", reason, failures[reason])
		}
	}
	return fmt.Sprintf("[INFO] Final test worker summary: workers=%d verified=%d unverified=%d retired=%d workers_with_fetch_parse_errors=%d reported_video_writes=%d%s",
		len(c.testParsed), verified, unverified, retired, withErrors, writes, rows.String())
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
