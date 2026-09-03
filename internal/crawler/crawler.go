package crawler

import (
	"context"
	"encoding/json"
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
	maxTestVideos = 5                // test mode only
	testDeadline  = 15 * time.Minute // hard stop for a test run
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

	// test only
	debug bool
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

// RunTest drives a bounded crawl from seedURL, stopping at maxTestVideos, an
// idle frontier, or testDeadline - whichever comes first. The reconcilers run
// here too: without the promoter, a job that Fails into DELAYED would never
// come back, and since Idle() counts DELAYED the run would neither finish nor
// go idle.
func (c *Crawler) RunTest(seedURL string) {
	c.debug = true
	ctx, cancel := context.WithTimeout(context.Background(), testDeadline)
	defer cancel()

	if _, err := c.admitURL(ctx, seedURL); err != nil {
		log.Printf("[ERROR] RunTest: seed admission failed: %v\n", err)
		return
	}

	var wg sync.WaitGroup
	scheduler.Run(ctx, c.frontier, c.redis, c.cfg.SchedulerBatchLimit, cancel, &wg)
	for i := 0; i < c.cfg.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			offset := time.Duration(workerID) * c.cfg.RateLimit / time.Duration(c.cfg.Workers)
			time.Sleep(offset)
			c.workerTest(ctx, workerID, cancel)
		}(i)
	}

	for c.Count() < maxTestVideos && ctx.Err() == nil {
		if idle, err := c.frontier.Idle(ctx); err != nil || idle {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	log.Printf("[INFO] Test finished - %d videos, %d targets\n", c.Count(), c.TargetCount())
}

func (c *Crawler) SaveTest() error {
	ctx := context.Background()
	targets, err := c.mongo.FindTargets(ctx)
	if err != nil {
		return err
	}
	f, err := os.Create(c.cfg.OutputFile)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(targets)
}

func (c *Crawler) InitTest() {
	ctx := context.Background()
	c.redis.FlushDB(ctx)
	log.Println("[INFO] Init test completed: Redis cleared")
}
