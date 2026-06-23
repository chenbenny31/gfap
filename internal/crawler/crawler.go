package crawler

import (
	"context"
	"encoding/json"
	"gfap/internal/auth"
	"gfap/internal/config"
	"gfap/internal/metrics"
	"gfap/internal/model"
	"gfap/internal/storage"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/time/rate"
)

type processResult int

const (
	idleTimeout   = time.Minute * 10
	maxRetries    = 3
	maxTestVideos = 50 // test mode only
)

const (
	resultOK processResult = iota
	resultRateLimited
	resultSkipped
	resultError
)

type Crawler struct {
	cfg      *config.Config
	redis    *storage.Redis
	mongo    *storage.Mongo
	jar      http.CookieJar // shared across all workers
	targets  []model.Video
	mu       sync.Mutex
	count    int
	queue    chan string
	inFlight atomic.Int64

	// production only
	stopChan chan struct{}
	stopOnce sync.Once
	// DISABLE global retryAfter logic
	//rateMu     sync.Mutex
	//retryAfter time.Time

	// test only
	debug bool
}

func New(cfg *config.Config, redis *storage.Redis, mongo *storage.Mongo) *Crawler {
	return &Crawler{
		cfg:      cfg,
		redis:    redis,
		mongo:    mongo,
		jar:      auth.NewJar(),
		queue:    make(chan string, cfg.QueueSize),
		stopChan: make(chan struct{}),
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

func (c *Crawler) enqueue(url string) {
	url = c.canonicalize(url)
	if url == "" {
		return
	}

	ctx := context.Background()
	// check bloom before enqueuing
	if url != c.cfg.BaseUrl {
		exists, err := c.redis.BloomExists(ctx, url)
		if err == nil && exists {
			return // already seen
		}
	}
	c.inFlight.Add(1)
	metrics.QueueSize.Set(float64(c.inFlight.Load()))
	select {
	case c.queue <- url:
	default:
		if err := c.redis.PushOverflow(context.Background(), url); err != nil {
			c.inFlight.Add(-1)
			metrics.QueueSize.Set(float64(c.inFlight.Load()))
		}
	}
}

func (c *Crawler) drainOverflow(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		url, err := c.redis.PopOverflow(ctx)
		if err != nil || url == "" {
			time.Sleep(time.Second)
			continue
		}

		select {
		case c.queue <- url:
		case <-ctx.Done():
			return
		}
	}
}

func (c *Crawler) process(workerId int, url string, client *auth.Client) processResult {
	url = c.canonicalize(url)
	if url == "" {
		return resultSkipped
	}

	ctx := context.Background()
	var err error // err is re-used

	var resp *http.Response
	var doc *goquery.Document

	for try := 1; try <= maxRetries; try++ {
		start := time.Now()
		resp, err = client.Get(url)
		metrics.FetchDuration.Observe(time.Since(start).Seconds())
		if err == nil {
			if resp.StatusCode != http.StatusOK {
				resp.Body.Close()
				if strings.Contains(url, c.cfg.VideoPattern) {
					backoff := time.Duration(try) * 60 * time.Second
					log.Printf("[WARN] Worker-%d: %s returned %d, backing off %s\n", workerId, url, resp.StatusCode, backoff)
					//c.rateMu.Lock()
					//c.retryAfter = time.Now().Add(backoff)
					//c.rateMu.Unlock()
					time.Sleep(backoff)
					return resultRateLimited
				}
				return resultError
			}
			doc, err = goquery.NewDocumentFromReader(resp.Body)
			resp.Body.Close()
			if err == nil {
				if strings.Contains(doc.Find("title").Text(), "Rate Limited") {
					backoff := time.Duration(try) * 30 * time.Second
					log.Printf("[WARN] Worker-%d: Rate limited on %s, backing off %s\n", workerId, url, backoff)
					//c.rateMu.Lock()
					//c.retryAfter = time.Now().Add(backoff)
					//c.rateMu.Unlock()
					time.Sleep(backoff)
					return resultRateLimited
				}
				break
			}
		}

		if try < maxRetries {
			log.Printf("[WARN] Worker-%d: Retry %d/%d for %s: %v\n", workerId, try, maxRetries, url, err)
			time.Sleep(time.Duration(try) * time.Second)
		}
	}

	if doc == nil {
		if strings.Contains(url, c.cfg.VideoPattern) {
			if err := c.redis.PushOverflow(context.Background(), url); err == nil {
				c.inFlight.Add(1) // balance worker's bottom Add(-1)
			}
		}
		return resultError
	}

	if err != nil {
		metrics.Errors.Inc()
		log.Printf("[ERROR] Failed to add url %s: %v\n", url, err)
		return resultSkipped
	}

	// bloom add only after confirmed good response
	if url != c.cfg.BaseUrl { // BaseUrl is used in re-seeding when exhaust queue
		var added bool
		added, err = c.redis.BloomAdd(ctx, url)
		if err != nil || !added {
			return resultSkipped
		}
	}

	metrics.PagesProcessed.Inc()

	if strings.Contains(url, c.cfg.VideoPattern) {
		c.mu.Lock()
		c.count++
		c.mu.Unlock()

		title := strings.TrimSuffix(doc.Find("title").Text(), c.cfg.TitleSuffix)
		date := strings.TrimSpace(doc.Find("date").First().Text())
		durStr := doc.Find(`meta[property="video:duration"]`).AttrOr("content", "")
		dur, _ := strconv.Atoi(durStr)

		// debug
		if c.debug {
			log.Printf("[DEBUG] workerId=%d url=%s title=%s date=%s dur=%d", workerId, url, title, date, dur)
		}

		v := model.Video{URL: url, Title: title, Date: date, Duration: dur}
		v.Match(c.cfg.CutoffDate)
		metrics.VideoFound.Inc()

		if v.IsTarget {
			c.mu.Lock()
			c.targets = append(c.targets, v)
			log.Printf("[FOUND] %d, %s - %s\n", len(c.targets), v.URL, v.Title)
			c.mu.Unlock()
			metrics.TargetsFound.Inc()
		}

		if err := c.mongo.Upsert(ctx, v); err != nil {
			log.Printf("[ERROR] Upsert failed for %s: %v\n", url, err)
		}
	}

	doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		if strings.HasPrefix(href, "/") && !strings.HasPrefix(href, "//") {
			href = c.cfg.BaseUrl + href
		}

		c.enqueue(href)
	})

	return resultOK
}

func (c *Crawler) Resume() {
	ctx := context.Background()
	videos, err := c.mongo.FindAll(ctx)
	if err != nil {
		log.Printf("[ERROR] Resume failed: %v\n", err)
		return
	}

	urls := make([]string, 0, len(videos))
	for _, v := range videos {
		urls = append(urls, v.URL)
		if v.IsTarget {
			c.targets = append(c.targets, v)
		}
	}

	const batchSize = 1000
	for i := 0; i < len(urls); i += batchSize {
		end := i + batchSize
		if end > len(urls) {
			end = len(urls)
		}
		if err := c.redis.BloomAddBatch(ctx, urls[i:end]); err != nil {
			log.Printf("[WARN] BloomAddBatch failed at offset %d: %v\n", i, err)
		}
	}

	c.count = len(videos)
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

// --- production only ---

func (c *Crawler) worker(workerId int, staticProxyURL string) {
	client := auth.NewClient(c.jar, staticProxyURL)
	consecFails := 0
	consecErrors := 0
	limiter := rate.NewLimiter(rate.Every(c.cfg.RateLimit), 1)
	for url := range c.queue {
		//c.rateMu.Lock()
		//wait := time.Until(c.retryAfter)
		//c.rateMu.Unlock()
		//if wait > 0 {
		//	time.Sleep(wait)
		//	limiter.Reserve()
		//}
		limiter.Wait(context.Background())
		result := c.process(workerId, url, client)
		switch result {
		case resultRateLimited:
			consecFails++
			consecErrors = 0
			if consecFails >= 10 {
				sleep := time.Duration(consecFails) * 5 * time.Minute
				log.Printf("[WARN] Worker-%d: 10 consecutive rate limits, backpressure\n", workerId)
				time.Sleep(sleep)
				// limiter.Reserve()
			}

		case resultError:
			consecErrors++
			consecFails = 0
			if consecErrors >= 20 {
				sleep := time.Duration(consecErrors) * 30 * time.Second
				log.Printf("[WARN] Worker-%d: 20 consecutive errors, backpressure\n", workerId)
				time.Sleep(sleep)
				limiter.Reserve()
			}
		default:
			consecFails, consecErrors = 0, 0
		}
		c.inFlight.Add(-1) // now un-conditional
		metrics.QueueSize.Set(float64(c.inFlight.Load()))
	}
}

func (c *Crawler) idleMonitor(ctx context.Context, baseUrl string) {
	ticker := time.NewTicker(idleTimeout)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c.inFlight.Load() == 0 {
				log.Printf("[INFO] Queue idle - re-seeding with %s\n", baseUrl)
				c.enqueue(baseUrl)
			}
		}
	}
}

func (c *Crawler) Stop() {
	c.stopOnce.Do(func() {
		log.Println("[INFO] Stop requested")
		close(c.stopChan)
	})
}

func (c *Crawler) Run(url string) {
	ctx, cancel := context.WithCancel(context.Background())
	go c.drainOverflow(ctx)
	go c.idleMonitor(ctx, url)
	for i := 0; i < c.cfg.Workers; i++ {
		proxyURL := ""
		if i < len(c.cfg.StaticProxyURLs) {
			proxyURL = c.cfg.StaticProxyURLs[i]
		}
		go c.worker(i, proxyURL)
	}
	c.enqueue(url)
	<-c.stopChan
	cancel()
	close(c.queue)
	log.Printf("[INFO] Crawler stopped - %d videos, %d targets\n", c.Count(), c.TargetCount())
}

func (c *Crawler) Seed(path string) {
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("[WARN] Crawler seeding failed: %v\n", err)
		return
	}
	count := 0
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			c.enqueue(line)
			count++
		}
	}
	log.Printf("[INFO] seeded %d URLs from %s\n", count, path)
}

func (c *Crawler) Login() error {
	client := auth.NewClient(c.jar, "")
	return client.Login(c.cfg.LoginURL, c.cfg.Username, c.cfg.Password)
}

// --- test only ---

func (c *Crawler) workerTest(workerId int) {
	proxyURL := ""
	if workerId < len(c.cfg.StaticProxyURLs) {
		proxyURL = c.cfg.StaticProxyURLs[workerId]
	}
	client := auth.NewClient(c.jar, proxyURL)
	limiter := rate.NewLimiter(rate.Every(c.cfg.RateLimit), 1)
	for url := range c.queue {
		if c.Count() < maxTestVideos {
			limiter.Wait(context.Background())
			c.process(workerId, url, client)
		}
		c.inFlight.Add(-1)
		metrics.QueueSize.Set(float64(c.inFlight.Load()))
	}
}

func (c *Crawler) RunTest(url string) {
	c.debug = true
	ctx, cancel := context.WithCancel(context.Background())
	go c.drainOverflow(ctx)
	for i := 0; i < c.cfg.Workers; i++ {
		go func(workerId int) {
			offset := time.Duration(workerId) * c.cfg.RateLimit / time.Duration(c.cfg.Workers)
			time.Sleep(offset)
			c.workerTest(workerId)
		}(i)
	}
	c.enqueue(url)
	for c.inFlight.Load() > 0 { // wait until nothing is in-flight
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	close(c.queue)
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
