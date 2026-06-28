# gfap

A continuous, self-re-seeding Go web crawler for discovering lost media on VidLii.com. Targets videos uploaded before December 31, 2021 with CJK (Han/Hiragana/Katakana/Hangul) or non-Latin characters in their titles and duration ≥ 10 minutes.

## Architecture

```text
[Start] seeds.txt (fresh) or MongoDB Resume (fallback)
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│                 URL Management & Queue                  │
│                                                         │
│   ┌────────────────┐ (overflow push) ┌────────────────┐ │
│   │ Memory Channel │ ──────────────► │   Redis List   │ │
│   │   (c.queue)    │ ◄────────────── │crawler:overflow│ │
│   └───────┬────────┘  (drain back)   └────────────────┘ │
└───────────┼─────────────────────────────────────────────┘
            │
            ▼ Dequeue URL → Canonicalize → Bloom check
┌─────────────────────────────────────────────────────────┐
│            Worker Pool (20 goroutines)                  │
│                                                         │
│   1. Per-worker rate limiter (golang.org/x/time/rate)   │
│            │                                            │
│   2. HTTP Fetch via static residential proxy            │
│            │  → 10 consecutive 429s → back off          │
│            │  → 20 consecutive errors → back off        │
│            │                                            │
│   3. Status check + rate-limit title detection          │
│            │                                            │
│   4. Bloom Filter de-dup (Redis BF.ADD)                 │
│            │  baseURL bypasses filter                   │
│   5. HTML Parser (goquery)                              │
└──────┬──────────────────────────────────────────┬───────┘
       │                                          │
       ▼ (extracted links → canonicalize)  ▼ (video metadata)
[Push back to Queue]        ┌───────────────────────────┐
                            │   Three-condition match   │
                            │                           │
                            │ 1. MatchDate  (pre-2022)  │
                            │ 2. HasCJKChar or          │
                            │    HasNonEnglishChar      │
                            │ 3. MatchDuration (≥10min) │
                            └────────────┬──────────────┘
                                         │
                                         ▼
                            ┌───────────────────────────┐
                            │        Persistence        │
                            │                           │
                            │ Upsert all videos         │
                            │ (all flags stored)        │
                            └───────────────────────────┘

[Idle Monitor] queue exhausted → re-seed baseURL every 10 min → crawl indefinitely
[Observability] Prometheus: pages_processed, video_found, targets_found, queue_size, errors
```

## Technical Highlights

* **Bloom Filter De-duplication:** Replaces per-URL Redis `SetNX` keys (~20GB at scale) with a single non-scaling Bloom filter (`crawler:bloom`, 1B capacity, 0.001% FPR, ~3GB). Sized for one-shot discovery — at the expected ~50M actual inserts, effective FPR drops below 10⁻²⁰. Bloom is pre-reserved `NONSCALING` at startup so it never silently auto-scales with degraded FPR. Verified at startup via `BloomVerify()` — mismatched capacity is a hard fatal. Persists across restarts via Redis AOF + RDB snapshots.
* **URL Canonicalization:** All URLs are normalized before Bloom and Mongo see them — fragments stripped, video URLs reduced to `/watch?v=<id>` (dropping `&t=`, `&list=`, `&index=`, `&from=`, `&ref=`), trailing slashes normalized on non-video pages. Collapses variant strings per page to a single key so Bloom and Mongo dedup on the same identifier and the rate budget isn't burned on duplicate fetches.
* **Bloom-before-Enqueue:** `enqueue()` checks `BF.EXISTS` before adding to the queue — already-seen URLs are dropped before they ever enter the channel or overflow list, keeping the queue lean and skipping the fetch entirely.
* **Elastic Overflow Queue:** A buffered Go channel handles in-memory URL distribution. When full, excess URLs push to a Redis List (`crawler:overflow`) and drain back in the background — preventing OOM during link explosions. Diagnosed a 1.1M-goroutine leak via Prometheus `go_goroutines` spike — traced to unbounded goroutine spawning when the channel saturated; redesigned this path, reducing peak memory from 4GB to 192MB. `inFlight` accounting is balanced across all paths: `+1` on enqueue, `-1` unconditionally at worker bottom, `+1` on overflow re-push so retried URLs remain counted.
* **Graceful Shutdown:** Workers select on both `stopChan` and the queue — no `close(c.queue)` (multiple senders). All `time.Sleep` backoffs replaced with `select{ctx.Done/time.After}` so workers wake immediately on SIGTERM instead of blocking up to 60s. `sync.WaitGroup` ensures `Run()` waits for every goroutine before returning. SIGTERM and SIGINT both invoke `c.Stop()` via `signal.Notify`; `/stop` HTTP endpoint remains as an alternative.
* **Per-worker Proxy Isolation:** Each of 20 workers is assigned a dedicated static residential proxy IP at startup. Rate limits and errors are tracked per-worker (`consecFails`, `consecErrors`) and back off independently — one flagged IP doesn't pause the other 19. After 10 consecutive rate limits or 20 consecutive errors, the affected worker backs off with exponential sleep before resuming on its own IP.
* **Shared Cookie Jar:** Login is performed once before the crawl starts. All 20 workers share a single `http.CookieJar`, verified to support concurrent sessions on VidLii, eliminating per-worker authentication overhead.
* **Fetch-before-Bloom Ordering:** HTTP status and rate-limit title are checked before `BF.ADD`. Non-200 and rate-limited video pages push to overflow without entering the filter — ensuring retryability without a `BF.REMOVE` operation.
* **Three-condition Targeting:** Every video page is upserted to MongoDB regardless of match status — reported `video:duration` metadata can be wrong, so the full corpus stays queryable for re-evaluation. Per-video match flags (`match_date`, `has_cjk_char`, `match_duration`, `has_non_english_char`) are stored independently. `IsTarget = MatchDate && MatchDuration && (HasCJKChar || HasNonEnglishChar)`.
* **Continuous Self-re-seeding:** The base URL bypasses Bloom filter de-dup so `idleMonitor` re-discovers new links every 10 minutes as VidLii adds content, without manual restarts.
* **Persistent State Layering:** Bloom persists via Redis AOF + RDB snapshots; MongoDB is the canonical artifact store. Loss of Redis triggers re-crawling but no data loss; `Resume()` rebuilds Bloom from Mongo on cold start. Loss of Mongo loses the harvested video corpus — `make fresh` is the only path that drops Mongo and requires typed confirmation.
* **Fault Tolerance:** Failed fetches retry up to 3 times with linear backoff. Rate-limited responses back off per-worker and are never entered into the Bloom filter.

---

## Requirements

* **Go:** 1.25+
* **Docker & Docker Compose**
* **Webshare static residential proxies** (20 IPs) configured in `.env`
* **RAM:** 6GB minimum, 8GB recommended (Bloom ~3GB, Mongo working set ~1–2GB, Prometheus + crawler + OS overhead ~1GB)
* **Disk:** ~10GB (Mongo grows with corpus; Redis snapshots add a few GB)

---

## First-time Setup

```bash
make infra-start   # starts containers and auto-reserves 1B Bloom if missing
```

Verify Bloom is correctly reserved:
```bash
docker exec gfap-redis-1 redis-cli BF.INFO crawler:bloom
# Capacity should be 1000000000, Number of filters should be 1
```

Configure proxies in `.env`:
```
STATIC_PROXY_URLS=http://user:pass@ip:port/,...  # 20 comma-separated
VIDLII_USERNAME=your-username
VIDLII_PASSWORD=your-password
```

---

## Quick Start

```bash
# 1. Start backend services
make infra-start

# 2. First run — seed from seeds.txt (237 known URLs)
make fresh

# 3. Monitor
make status
make logs
make metrics
```

---

## Commands Reference

| Command | Description |
| --- | --- |
| `make infra-start` | Start Redis Stack, MongoDB, and Prometheus; auto-reserve Bloom if missing |
| `make infra-stop` | Stop Docker services |
| `make infra-logs` | View Docker container logs |
| `make build` | Compile the crawler binary |
| `make fresh` | First run — drops corpus and seeds from seeds.txt (**irreversible**, requires confirmation) |
| `make resume` | Resume production crawl from last checkpoint |
| `make reset-bloom` | Delete Bloom filter only — safe for MongoDB, use before changing bloom params |
| `make test` | Bounded test crawl (50 videos, test URL) |
| `make stop` | Graceful crawler shutdown via HTTP |
| `make metrics` | Print live Prometheus metrics |
| `make logs` | Tail crawler.log |
| `make status` | Show Docker and crawler process status |
| `make restart` | Rebuild and restart crawler |
| `make clean` | Delete all data, logs, and volumes (**irreversible**, requires confirmation) |
| `make k8s-up` | Build image, load into kind cluster, apply manifests |
| `make k8s-down` | Delete kind cluster |
| `make k8s-verify` | Check pods, crawler metrics, Prometheus scrape |

---

## Project Structure

```text
cmd/crawler/        — entry point (main.go)
internal/
  ├── config/       — crawler configuration + proxy URL list (loaded from .env)
  ├── crawler/      — worker pool, queue, idle monitor, bloom dedup,
  │                   URL canonicalizer, per-worker rate limiter and backoff
  ├── storage/      — Redis (Bloom + overflow) & MongoDB clients
  ├── metrics/      — Prometheus instrumentation + /stop endpoint
  ├── model/        — Video struct, match flags, CJK/non-Latin detection
  └── auth/         — HTTP client with shared cookie jar + proxy transport
k8s/                — Kubernetes manifests (kind local cluster)
  ├── kustomization.yaml
  ├── namespace.yaml
  ├── crawler-configmap.yaml
  ├── crawler-secret.example.yaml
  ├── crawler-deployment.yaml
  ├── redis-deployment.yaml
  ├── mongo-deployment.yaml
  ├── prometheus-configmap.yaml
  └── prometheus-deployment.yaml
seeds.txt           — 237 known VidLii URLs for cold start
.env                — proxy credentials (gitignored)
```

---

## Monitoring

* **Prometheus:** `http://localhost:9090`
* **Raw metrics:** `http://localhost:2112/metrics`

Key metrics: `pages_processed`, `video_found`, `targets_found`, `queue_size`, `fetch_duration_seconds`, `errors`

Sanity check after a run — `video_found` and `db.videos.countDocuments()` should agree within a small margin. Divergence indicates silent Mongo write failures or a canonicalization regression.

---

## Local Kubernetes (kind)

A local-test integration using [kind](https://kind.sigs.k8s.io/) and vanilla upstream Kubernetes objects. Demonstrates Deployment, Service, liveness probes, resource requests/limits, PVC-backed stateful services, and Prometheus scrape — without Helm, operators, or HPA.

gfap is a stateful singleton: it's a non-scalable Deployment with `Recreate` strategy, not behind an HPA. Scaling replicas would double-crawl VidLii and break per-worker proxy isolation. Readiness probe is omitted — nothing routes client traffic to the crawler so readiness is semantically weak here.

**Prerequisites:**
```bash
brew install kind kubectl   # macOS
# or https://kind.sigs.k8s.io and https://kubernetes.io/docs/tasks/tools/
```

**Setup:**
```bash
cp k8s/crawler-secret.example.yaml k8s/crawler-secret.yaml
# fill in VIDLII_USERNAME, VIDLII_PASSWORD, STATIC_PROXY_URLS
make k8s-up
```

`k8s-up` creates the kind cluster, builds and loads the image, applies the namespace, secret, and all manifests, then waits for rollout. An initContainer reserves the 1B NONSCALING Bloom filter before the crawler starts — idempotent on pod restart.

**Verify:**
```bash
kubectl get pods -n gfap
kubectl logs -n gfap -l app=gfap-crawler -c bloom-init
kubectl port-forward -n gfap svc/gfap-crawler 2112:2112 &
curl http://localhost:2112/metrics | grep -E "pages_processed|video_found"
```

**Graceful shutdown test:**
```bash
kubectl delete pod -n gfap -l app=gfap-crawler
kubectl logs -n gfap -l app=gfap-crawler --previous | tail -20
# expect clean shutdown log, no panic
```

**Tear down:**
```bash
make k8s-down
```

---

## VPS Deployment

**Debian/Ubuntu:**
```bash
sudo apt update && sudo apt install -y docker.io docker-compose golang-go git make
```

**RHEL/Fedora:**
```bash
sudo dnf install -y docker docker-compose golang git make
sudo systemctl enable --now docker
```

```bash
git clone https://github.com/chenbenny31/gfap.git && cd gfap
cp .env.example .env  # fill in proxy credentials
make infra-start
make fresh
```

---

## License

GPL-3.0. See [LICENSE](LICENSE) for full text.
