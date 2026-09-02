package metrics

import (
	"log"
	"net"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	PagesProcessed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pages_processed",
		Help: "The total number of pages processed",
	})
	VideoFound = promauto.NewCounter(prometheus.CounterOpts{
		Name: "video_found",
		Help: "The total number of video pages found",
	})
	TargetsFound = promauto.NewCounter(prometheus.CounterOpts{
		Name: "targets_found",
		Help: "The total number of target videos found",
	})
	Errors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "errors",
		Help: "The total number of fetch errors",
	})
	FetchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "fetch_duration_seconds",
		Help:    "HTTP fetch latency in seconds",
		Buckets: []float64{0.1, 0.5, 1, 2, 5},
	})

	// --- frontier / reconciler metrics ---

	LeaseErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lease_errors_total",
		Help: "The total number of Frontier.Lease errors",
	})
	ResolveErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "resolve_errors_total",
		Help: "The total number of non-stale errors resolving a leased job (Ack/Nack/Fail/Terminal/CompleteListing)",
	})
	ReconcileErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "reconcile_errors_total",
		Help: "The total number of errors across all reconciler ticks (listingScheduler/promoter/reaper/sweeper/bloomMonitor/heartbeat)",
	})
	ReapedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "reaped_total",
		Help: "The total number of jobs reclaimed by the reaper from expired leases",
	})
	SweptTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "swept_total",
		Help: "The total number of orphaned jobs reclaimed by the sweeper",
	})
	PromotedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "promoted_total",
		Help: "The total number of delayed jobs promoted back to ready",
	})
	ListingsAdmittedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "listings_admitted_total",
		Help: "The total number of due listings admitted to ready by the listing scheduler",
	})

	FrontierReady = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "frontier_ready",
		Help: "Current length of the ready list",
	})
	FrontierProcessing = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "frontier_processing",
		Help: "Current length of the processing list",
	})
	FrontierDelayed = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "frontier_delayed",
		Help: "Current size of the delayed set",
	})
	ListingsScheduled = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "listings_scheduled",
		Help: "Current number of listings with a future due time (not currently in flight)",
	})
	ListingsPending = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "listings_pending",
		Help: "Current number of listings with a live job in ready/processing/delayed",
	})
	FrontierDead = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "frontier_dead",
		Help: "Current number of URLs in the dead set",
	})
	FrontierStrikes = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "frontier_strikes",
		Help: "Current number of URLs with at least one recorded strike",
	})
	FrontierIdle = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "frontier_idle",
		Help: "1 if the frontier has nothing ready, processing, or delayed; 0 otherwise",
	})
	BloomFillRatio = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "bloom_fill_ratio",
		Help: "Fraction of configured Bloom filter capacity currently inserted",
	})
)

// Serve starts the metrics HTTP server on the given port.
// stopFn is called when POST /stop is received, triggering graceful shutdown.
//
// /stop is destructive and unauthenticated by design (a single POST ends
// the crawl), so it's restricted to loopback callers - only a process on
// the same host/pod can trigger it. /metrics stays reachable on every
// interface since Prometheus/k8s scrape it across the network. This has no
// effect on production shutdown: SIGTERM (pod deletion) is the real
// shutdown path there; /stop only serves local `make stop`, which already
// runs on the same host it's curling.
func Serve(port string, stopFn func()) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !isLoopback(r.RemoteAddr) {
			log.Printf("[WARN] /stop rejected from non-loopback %s", r.RemoteAddr)
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		log.Println("[INFO] Stop requested via /stop")
		stopFn()
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("stopping\n"))
	})
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// isLoopback reports whether addr (an http.Request.RemoteAddr "host:port"
// string) resolves to a loopback address.
//
// Sound only with no on-host proxy in front of this server: a mesh sidecar
// (Istio/Envoy) or node-local ingress would itself connect over loopback,
// reopening /stop to anything that can reach the sidecar. gfap runs as a
// singleton pod with no mesh, so this holds today - if a sidecar is ever
// introduced, switch to a genuinely separate listener bound to 127.0.0.1
// on its own port instead of this RemoteAddr check.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
