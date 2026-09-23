package crawler

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"gfap/internal/auth"
	"gfap/internal/config"
	"gfap/internal/metrics"
	"gfap/internal/model"
	"gfap/internal/storage"

	"github.com/PuerkitoBio/goquery"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

const (
	// completionTimeout bounds every frontier resolution call (Ack/Nack/
	// Fail/Terminal/CompleteListing), independent of the worker's root
	// context - a slow Mongo call or a cancelled root ctx must never
	// prevent a leased job from being released.
	completionTimeout   = 3 * time.Second
	workerIdleSleep     = 1 * time.Second
	proxyWarningEvery   = 5
	workerRateLimitStop = 5
)

type pageKind int

const (
	pageVideo pageKind = iota
	pageListing
	pageNotFound
	pageRetryable
	pageUnknown
	pageCancelled
)

type pageOutcome struct {
	kind      pageKind
	video     model.Video       // pageVideo
	links     []string          // pageVideo, pageListing
	reason    string            // HTTP status or fixed failure label; never raw errors
	failClass storage.FailClass // pageRetryable
}

// resolve runs one frontier state transition under a fresh bounded context.
func (c *Crawler) resolve(op func(context.Context) error) {
	fctx, cancel := context.WithTimeout(context.Background(), completionTimeout)
	defer cancel()
	if err := op(fctx); err != nil {
		if errors.Is(err, storage.ErrStaleLease) {
			return
		}
		metrics.ResolveErrors.Inc()
		log.Printf("[ERROR] resolve: %v", err)
	}
}

// untilFor returns the terminal quarantine deadline for a URL class: VIDEO
// is gone forever (zero time -> until=0 in the frontier encoding),
// LISTING/SEED get a quarantine so a since-fixed page can be revisited.
func untilFor(class storage.URLClass, quarantine time.Duration, now time.Time) time.Time {
	if class == storage.ClassVideo {
		return time.Time{}
	}
	return now.Add(quarantine)
}

// NewURLClassifier builds a storage.URLClassifier from crawl config and the
// startup seed set (BaseUrl plus every seeds.txt entry). A seed gets
// ClassSeed instead of ClassListing purely for CompleteListing's TTL choice
// (SeedTTL vs the longer ListingBaseTTL) - it has no other effect.
func NewURLClassifier(cfg *config.Config, seedURLs []string) storage.URLClassifier {
	seeds := make(map[string]struct{}, len(seedURLs))
	for _, u := range seedURLs {
		seeds[u] = struct{}{}
	}
	return func(url string) storage.URLClass {
		if strings.Contains(url, cfg.VideoPattern) {
			return storage.ClassVideo
		}
		if _, ok := seeds[url]; ok {
			return storage.ClassSeed
		}
		return storage.ClassListing
	}
}

// maybeFatalAdmission stops the crawl on a server/script-side admission
// error (Bloom at capacity, module gone) - retrying would just re-admit,
// re-error, forever. A network error or context cancellation is not fatal;
// the caller's Nack already handles those.
func maybeFatalAdmission(err error, stopFn func()) {
	var rerr redis.Error
	if errors.As(err, &rerr) {
		log.Printf("[ERROR] admission failed fatally: %v - stopping crawl", err)
		stopFn()
	}
}

// admitURL canonicalizes raw and routes it to AdmitVideo or AdmitListing.
// Returns Suppressed, nil for a URL that canonicalizes to nothing - not an
// error, just nothing to admit.
func (c *Crawler) admitURL(ctx context.Context, raw string) (storage.AdmitResult, error) {
	url := c.canonicalize(raw)
	if url == "" {
		return storage.Suppressed, nil
	}
	if strings.Contains(url, c.cfg.VideoPattern) {
		return c.frontier.AdmitVideo(ctx, url)
	}
	return c.frontier.AdmitListing(ctx, url)
}

// enqueueChildren admits every discovered link. Per the child contract: any
// admission error (not a Suppressed result) aborts immediately - the caller
// must Nack the parent, and a retry re-admitting the whole set is safe
// since AdmitVideo/AdmitListing are idempotent for already-admitted URLs.
// Returns the count of AdmitResult == New, which CompleteListing's barren
// tracking needs.
func (c *Crawler) enqueueChildren(ctx context.Context, links []string) (int, error) {
	newCount := 0
	for _, raw := range links {
		res, err := c.admitURL(ctx, raw)
		if err != nil {
			return newCount, err
		}
		if res == storage.New {
			newCount++
		}
	}
	return newCount, nil
}

func extractLinks(doc *goquery.Document, baseURL string) []string {
	var links []string
	doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		if strings.HasPrefix(href, "/") && !strings.HasPrefix(href, "//") {
			href = baseURL + href
		}
		links = append(links, href)
	})
	return links
}

// isRateLimitTitle preserves the existing title check and recognizes common
// variants. Do not scan arbitrary body text: normal content can mention limits.
func isRateLimitTitle(title string) bool {
	title = strings.ToLower(strings.Join(strings.Fields(title), " "))
	for _, phrase := range []string{
		"rate limited", "rate limiting", "rate limit exceeded", "too many requests",
	} {
		if strings.Contains(title, phrase) {
			return true
		}
	}
	return false
}

// process fetches and classifies url in a single attempt - no in-process
// retry or backoff. All retry timing lives in the frontier's backoff
// ladders (Fail's RateLimitBackoff/StrikeBackoff), driven by whatever
// FailClass this returns.
func (c *Crawler) process(ctx context.Context, url string, client *auth.Client) pageOutcome {
	start := time.Now()
	resp, err := client.Get(ctx, url)
	metrics.FetchDuration.Observe(time.Since(start).Seconds())
	if err != nil {
		if ctx.Err() != nil {
			return pageOutcome{kind: pageCancelled}
		}
		return pageOutcome{
			kind: pageRetryable, failClass: storage.FailStrikeable,
			reason: "request_failed",
		}
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusTooManyRequests:
		return pageOutcome{
			kind: pageRetryable, failClass: storage.FailRateLimited,
			reason: "http_429",
		}
	case http.StatusNotFound, http.StatusGone:
		return pageOutcome{kind: pageNotFound, reason: strconv.Itoa(resp.StatusCode)}
	case http.StatusOK:
		// falls through to parsing below
	default:
		return pageOutcome{
			kind: pageRetryable, failClass: storage.FailStrikeable,
			reason: "http_" + strconv.Itoa(resp.StatusCode),
		}
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		if ctx.Err() != nil {
			return pageOutcome{kind: pageCancelled}
		}
		return pageOutcome{kind: pageUnknown, reason: "parse_failed"}
	}
	title := doc.Find("title").Text()
	if isRateLimitTitle(title) {
		return pageOutcome{
			kind: pageRetryable, failClass: storage.FailRateLimited,
			reason: "http_200_rate_limit_title",
		}
	}

	metrics.PagesProcessed.Inc()
	links := extractLinks(doc, c.cfg.BaseUrl)

	if !strings.Contains(url, c.cfg.VideoPattern) {
		return pageOutcome{kind: pageListing, links: links}
	}

	date := strings.TrimSpace(doc.Find("date").First().Text())
	durStr := doc.Find(`meta[property="video:duration"]`).AttrOr("content", "")
	dur, _ := strconv.Atoi(durStr)
	v := model.Video{
		URL:      url,
		Title:    strings.TrimSuffix(title, c.cfg.TitleSuffix),
		Date:     date,
		Duration: dur,
	}
	v.Match(c.cfg.CutoffDate)
	metrics.VideoFound.Inc()
	if c.debug {
		log.Printf("[DEBUG] url=%s title=%s date=%s dur=%d", url, v.Title, v.Date, v.Duration)
	}
	return pageOutcome{kind: pageVideo, video: v, links: links}
}

// recordVideo updates in-memory stats/targets for a video already durably
// upserted to Mongo - c.count/c.targets never count one that isn't stored.
func (c *Crawler) recordVideo(v model.Video) {
	c.mu.Lock()
	c.count++
	c.mu.Unlock()

	if !v.IsTarget {
		return
	}
	c.mu.Lock()
	c.targets = append(c.targets, v)
	n := len(c.targets)
	c.mu.Unlock()
	log.Printf("[FOUND] %d, %s - %s\n", n, v.URL, v.Title)
	metrics.TargetsFound.Inc()
}

// handleOutcome resolves the leased job per out.kind - the switch in §7's
// worker loop, factored out so both worker() and workerTest() share it.
func (c *Crawler) handleOutcome(ctx context.Context, job *storage.Job, out pageOutcome, stopFn func()) bool {
	switch out.kind {
	case pageVideo:
		if err := c.mongo.Upsert(ctx, out.video); err != nil {
			c.resolve(func(fctx context.Context) error { return c.frontier.Nack(fctx, job) })
			return false
		}
		c.recordVideo(out.video)
		if _, err := c.enqueueChildren(ctx, out.links); err != nil {
			c.resolve(func(fctx context.Context) error { return c.frontier.Nack(fctx, job) })
			maybeFatalAdmission(err, stopFn)
			return false
		}
		c.resolve(func(fctx context.Context) error { return c.frontier.Ack(fctx, job) })
		return true
	case pageListing:
		n, err := c.enqueueChildren(ctx, out.links)
		if err != nil {
			c.resolve(func(fctx context.Context) error { return c.frontier.Nack(fctx, job) })
			maybeFatalAdmission(err, stopFn)
			return false
		}
		c.resolve(func(fctx context.Context) error { return c.frontier.CompleteListing(fctx, job, n) })
	case pageNotFound:
		quarantine := time.Duration(c.cfg.QuarantineListingHours) * time.Hour
		c.resolve(func(fctx context.Context) error {
			return c.frontier.Terminal(fctx, job, out.reason, untilFor(job.Class, quarantine, time.Now()))
		})
	case pageRetryable:
		c.resolve(func(fctx context.Context) error {
			_, err := c.frontier.Fail(fctx, job, out.failClass)
			return err
		})
	case pageUnknown:
		c.resolve(func(fctx context.Context) error {
			_, err := c.frontier.Fail(fctx, job, storage.FailStrikeable)
			return err
		})
	case pageCancelled:
		c.resolve(func(fctx context.Context) error { return c.frontier.Nack(fctx, job) })
	}
	return false
}

// sleepCtx sleeps for d or returns early if ctx is cancelled - used for the
// worker's idle poll so shutdown doesn't wait out a full idle interval.
func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// workerFailures belongs to one worker and its fixed proxy assignment.
// It observes fetch/parse outcomes only, not Mongo or Redis failures.
type workerFailures struct {
	consecutive            int
	rateLimited            int
	consecutiveRateLimited int
}

func (f *workerFailures) observe(workerID int, route string, out pageOutcome) {
	if out.kind != pageCancelled {
		if out.kind == pageRetryable && out.failClass == storage.FailRateLimited {
			f.consecutiveRateLimited++
		} else {
			f.consecutiveRateLimited = 0
		}
	}
	switch out.kind {
	case pageCancelled:
		return
	case pageVideo, pageListing, pageNotFound:
		f.consecutive = 0
		f.rateLimited = 0
		return
	case pageRetryable, pageUnknown:
		f.consecutive++
		if out.kind == pageRetryable && out.failClass == storage.FailRateLimited {
			f.rateLimited++
		}
	}
	if f.consecutive > 0 && f.consecutive%proxyWarningEvery == 0 {
		log.Printf("[WARN] worker=%d route=%s consecutive_failures=%d rate_limited=%d last_reason=%s suspected_proxy_network_or_site_problem=true",
			workerID, route, f.consecutive, f.rateLimited, out.reason)
	}
}

// --- production ---

func (c *Crawler) worker(ctx context.Context, workerID int, proxyURL string, stopFn func(), wg *sync.WaitGroup) {
	defer wg.Done()
	sleepCtx(ctx, time.Duration(workerID)*c.cfg.RateLimit/time.Duration(c.cfg.Workers))
	c.workerLoop(ctx, workerID, proxyURL, stopFn, 0)
}

// workerLoop is shared by production and the bounded parsing test.
// videoLimit == 0 means production has no video-count stop condition.
func (c *Crawler) workerLoop(ctx context.Context, workerID int, proxyURL string, stopFn func(), videoLimit int) {
	if ctx.Err() != nil {
		return
	}
	route := "direct"
	if proxyURL != "" {
		route = "proxy_slot_" + strconv.Itoa(workerID)
	}
	var failures workerFailures
	client := auth.NewClient(c.jar, proxyURL)
	defer client.CloseIdleConnections()
	limiter := rate.NewLimiter(rate.Every(c.cfg.RateLimit), 1)

	for {
		if ctx.Err() != nil || (videoLimit > 0 && c.Count() >= videoLimit) {
			return
		}

		job, err := c.frontier.Lease(ctx)
		if err != nil {
			metrics.LeaseErrors.Inc()
			sleepCtx(ctx, workerIdleSleep)
			continue
		}
		if job == nil {
			sleepCtx(ctx, workerIdleSleep)
			continue
		}

		if err := limiter.Wait(ctx); err != nil {
			c.resolve(func(fctx context.Context) error { return c.frontier.Nack(fctx, job) })
			return
		}

		if c.debug {
			c.recordTestStart(job.URL)
			log.Printf("[DEBUG] event=process_start pid=%d worker=%d route=%s job=%q url=%q at_ns=%d",
				os.Getpid(), workerID, route, job.ID, job.URL, time.Now().UnixNano())
		}
		out := c.process(ctx, job.URL, client)
		if c.debug {
			c.recordTestFinish(workerID, job.URL, out.kind)
			log.Printf("[DEBUG] event=process_finish pid=%d worker=%d route=%s job=%q url=%q at_ns=%d kind=%d reason=%q",
				os.Getpid(), workerID, route, job.ID, job.URL, time.Now().UnixNano(), out.kind, out.reason)
		}
		failures.observe(workerID, route, out)
		stored := c.handleOutcome(ctx, job, out, stopFn)
		if c.debug && stored {
			c.mu.Lock()
			c.testStored[workerID]++
			c.mu.Unlock()
		}
		if failures.consecutiveRateLimited >= workerRateLimitStop {
			log.Printf("[ERROR] worker=%d route=%s stopping after %d consecutive rate limits; last_reason=%s",
				workerID, route, failures.consecutiveRateLimited, out.reason)
			if c.debug {
				c.mu.Lock()
				c.testRetired[workerID] = true
				c.mu.Unlock()
			}
			return
		}
	}
}

// Run starts workers and registers their supervisor on the shared WaitGroup.
// When every worker exits, cancel the root context so reconcilers also stop.
func (c *Crawler) Run(ctx context.Context, stopFn func(), wg *sync.WaitGroup) {
	var workers sync.WaitGroup
	wg.Add(1)
	for i := 0; i < c.cfg.Workers; i++ {
		proxyURL := ""
		if i < len(c.cfg.StaticProxyURLs) {
			proxyURL = c.cfg.StaticProxyURLs[i]
		}
		workers.Add(1)
		go c.worker(ctx, i, proxyURL, stopFn, &workers)
	}
	go func() {
		defer wg.Done()
		workers.Wait()
		if ctx.Err() == nil {
			log.Print("[ERROR] All workers stopped; shutting down crawler")
		}
		stopFn()
	}()
}

// --- test only ---

func (c *Crawler) workerTest(ctx context.Context, workerID int, stopFn func()) {
	proxyURL := ""
	if workerID < len(c.cfg.StaticProxyURLs) {
		proxyURL = c.cfg.StaticProxyURLs[workerID]
	}
	c.workerLoop(ctx, workerID, proxyURL, stopFn, maxTestVideos)
}
