# gfap

A continuous Go crawler for discovering lost media on VidLii.com. Every parsed video is stored in MongoDB. Targets have an upload date on or before December 31, 2023, a duration of at least 10 minutes, and CJK or other non-Latin letters in their titles.

## Quick start

Prepare `.env`, Go dependencies, and Docker images as described below. Run from the project root:

```sh
make infra-start   # production Redis, MongoDB and Prometheus
make test          # separate, disposable test Redis and MongoDB
make run           # production crawler in the background
```

Testing does not require production services to be running. Check the test's exit status and per-worker results before starting production. Reaching its time limit is a failure, not a pass; see [current validation limits](#current-validation-limits).

## Requirements and configuration

- Go 1.25.7 or newer, matching [go.mod](go.mod).
- Make and Docker Compose. Infrastructure targets invoke `docker-compose`; the test invokes `docker compose`.
- Locally available `redis/redis-stack:latest` and `mongo:7` images for testing. The test does not pull images.
- Prepared Go dependencies: `make test` uses the local toolchain, disables network module acquisition, and builds with read-only module files.
- Memory for a billion-entry Bloom filter (approximately 2.8 GiB), the frontier and MongoDB. Running production and test together requires memory for both Redis instances.

Example `.env` with placeholder credentials:

```dotenv
STATIC_PROXY_URLS=http://USER:PASSWORD@HOST1:PORT,http://USER:PASSWORD@HOST2:PORT
VIDLII_USERNAME=your-username
VIDLII_PASSWORD=your-password
WORKERS=20
RATE_LIMIT_SEC=30
```

Supply **100 comma-separated proxy URLs** for the test; the two entries above illustrate the format. URL-encode reserved characters in credentials. Keep `.env` private; the working-tree ignore rules exclude it.

| Setting | Production | Test |
| --- | --- | --- |
| Workers | `WORKERS`, default 20 | Fixed at 100 |
| Request spacing | `RATE_LIMIT_SEC`, default 15 seconds | At least 30 seconds per worker; slower settings preserved |
| Proxies | One fixed list entry per worker | First 100 entries; missing entries fail validation |
| Redis | `REDIS_ADDR`, default `localhost:6379`, DB 0 | `127.0.0.1:16379`, DB 1 |
| MongoDB | `MONGO_URI`, default `mongodb://localhost:27017`, database `vidlii` | `mongodb://127.0.0.1:37017`, database `gfap_test` |
| Metrics port | 2112 | 2113 |
| Log | `crawler.log` | `crawler.test.log` |

Test configuration ignores production storage endpoint overrides. Production startup rejects the reserved test storage ports and database name. Test and production use separate containers and volumes, not just different Redis database indexes.

Production workers without an assigned proxy use a direct connection. Supply at least as many entries as `WORKERS` when every worker must use a proxy. Loading 100 entries does not change the production worker count. Use distinct endpoints when testing 100 different proxies.

Login runs once over a direct connection, then workers share the cookie jar. The current login check accepts HTTP 200; that alone does not prove authentication or session validity across proxy IPs.

## Local proxy and crawler test

```sh
make test
```

The target builds a temporary binary, starts [docker-compose.test.yml](docker-compose.test.yml), and runs the real crawler with:

- **100 workers**, staggered starts, and at least 30 seconds between requests per worker.
- **200 stored videos or a ten-minute crawl deadline**, whichever stops the crawl first. In-flight writes can overshoot the target. Startup, final checkpoint and cleanup add time outside that deadline.
- `TEST_URL` as the initial page, defaulting to `https://www.vidlii.com/user/rinkomania`.
- Duplicate seed-admission checks and observations that fail on concurrent processing of the same canonical URL.
- A requirement for every worker to fetch and store at least one video. Listing-page success alone does not verify video access.

Workers continue after their first video. A rate-limited worker can retire while others continue. After workers join, each logs its zero-based `proxy_slot`, parsed-page count, stored-video count, and result: `video_stored`, `rate_limited`, or `unverified`. An unverified proxy has not demonstrated video access in this workload; it is not necessarily unusable.

A full pass requires the video target, coverage of every worker, no retired workers or detected overlap, and a Mongo unique-record count matching recorded writes. Timeout, insufficient work, missing coverage, or a storage/checkpoint failure produces a nonzero exit. Ten minutes permits about twenty request starts per worker at this spacing. Reaching 200 videos can stop the test earlier with some workers still unverified; a longer deadline does not guarantee coverage.

The final crawler log block summarizes verified, unverified and retired workers and reported video writes. It lists every unverified/retired worker and any worker with fetch/parse errors, including those that later stored a video. Error counts use fixed labels such as `http_429`, `request_failed` and `uk_access_notice`; cancellations and ordinary 404/410 responses are excluded. These counts do not diagnose the underlying network error or classify storage failures as proxy failures.

After workers and reconcilers join, the test attempts a final crawl-state checkpoint with a separate 30-second timeout, including when the crawl failed. A successful run exports matching videos to `targets.test.json`. Logs remain available on failure; an existing JSON export may belong to an earlier successful run.

The Makefile removes test containers, their dedicated volumes and network, and the temporary binary on normal completion, failure, or handled interruption. Logs and exports are retained. Forced termination or a machine crash can leave resources behind; the next test discards the fixed test resources before starting. Run only one test at a time per host. Production Redis configuration and volumes are not reset by this workflow.

## Architecture

```text
Startup: verify Bloom -> reconcile stored video URLs -> restore crawl state
                                      |
                                      v
             Redis frontier: ready -> processing + lease
                    ^                     |
          due listings / delayed     proxy workers
                    ^                     |
          scheduler and recovery <- outcome / discovered URLs
                                          |
                                          v
                                MongoDB video upserts

Redis listing/dead state -> periodic checkpoint -> MongoDB crawler_state_v1
```

See [DESIGN.md](DESIGN.md) for frontier contracts and [internal/storage/frontier.go](internal/storage/frontier.go) for their implementation.

- **Redis owns queued work.** Ready jobs, processing jobs, leases and delayed retries replace the old in-memory channel and overflow queue. Atomic scripts implement admission and job transitions.
- **Deduplication happens at admission.** Video URLs are claimed in a non-scaling Bloom filter. Listings use a schedule with a pending marker while a job exists. Canonicalization strips fragments and reduces video URLs to their video identifier.
- **Workers make one fetch attempt per leased job.** They store metadata, admit discovered links, and resolve the job. Reconciliation recovers expired leases and orphaned jobs. This does not guarantee that a worker still processing after lease expiry cannot overlap a replacement worker.
- **Listings are revisited on schedule.** The base page is scheduled at startup. Seed revisit TTL defaults to 24 hours, other listings to 72 hours, with longer intervals for barren listings. An empty queue does not immediately re-add the main page.
- **Matching is independent of storage.** All parsed videos are upserted by canonical URL; individual match flags remain queryable. See [internal/model/video.go](internal/model/video.go).

## Rate limits and worker shutdown

HTTP 429 and recognized rate-limit page titles returned with HTTP 200 use the rate-limit retry path. Matching is case-insensitive and recognizes `Rate Limited`, `Rate Limiting`, `Rate Limit Exceeded`, and `Too Many Requests`. Arbitrary body text is not scanned.

- **Per URL:** default rate-limit delays are 5, 15, then 45 minutes. These responses do not add ordinary failure strikes. Other retryable failures use a separate strike/backoff policy.
- **Per worker:** five consecutive recognized rate-limit responses retire that worker. Other completed outcomes reset that streak; cancellation does not count. Generic consecutive failures produce warnings at multiples of five.
- **Before retirement:** the current outcome goes through frontier resolution, then the worker closes idle HTTP connections and returns. Resolution errors are logged; successful lease release is not guaranteed if Redis fails.
- **Production:** healthy workers continue. When all workers exit, their supervisor cancels the scheduler and main joins shutdown. There is no automatic proxy rotation or worker replacement.

SIGINT, SIGTERM, and a local `POST /stop` also cancel the crawl. To replace proxies, stop the process, wait for it to exit, edit `.env`, and restart against the saved stores.

## Persistence and recovery

MongoDB stores videos in `videos` and listing schedules, barren counts, and dead/quarantine state in `crawler_state_v1`. Production crawl-state checkpoints run immediately after startup and every ten minutes, using acknowledged, journaled writes.

Startup reconciles stored video URLs into Bloom and restores crawl state before admitting new work. Cold restore supports recovery after complete frontier loss; it does **not** restore queued jobs, leases, attempts, or strikes. Lost queued work must be rediscovered through listing visits.

A valid nonempty Redis frontier is preserved on restart. Newer Mongo state is not merged over an older nonempty Redis snapshot; a later checkpoint can copy that older Redis state back into Mongo.

The supplied Redis Compose configuration disables AOF and automatic save points. The crawler requests an RDB snapshot every six hours. `make snapshot` requests a snapshot and prints its result; `make infra-stop` runs that target before stopping production services. Check the reported save status before relying on it.

**Current shutdown limitation:** production joins workers and periodic reconcilers but does not perform a final crawl-state checkpoint afterward. The bounded test does perform that checkpoint. Stopping the crawler alone also does not request a final Redis snapshot.

## Commands

| Command | Behavior |
| --- | --- |
| `make infra-start` | Start production services; reserve Bloom if missing |
| `make infra-stop` | Request a Redis snapshot, then stop production services |
| `make infra-logs` | Show production service logs |
| `make snapshot` | Request an RDB snapshot and print save status |
| `make build` | Build `./crawler` |
| `make run` | Build and start production in the background; refuses an existing process named crawler |
| `make test` | Run the isolated 100-worker test and clean up its resources |
| `make stop` | Request production shutdown through the local HTTP endpoint |
| `make logs` | Follow `crawler.log` |
| `make metrics` | Print production metrics |
| `make status` | Show production services and crawler processes |
| `make resume` | Legacy background launcher; signals existing crawler processes first |
| `make restart` | Legacy rebuild/background launcher with a fixed one-second shutdown delay |
| `make k8s-up` | Build/load the image and apply the local kind manifests |
| `make k8s-down` | Delete the local kind cluster |
| `make k8s-verify` | Run the existing Kubernetes checks |

The legacy `make fresh` target contains a `nohub` typo and an outdated data-deletion warning. The actual `-fresh` flag adds URLs from `seeds.txt`; it does not drop MongoDB. To request that behavior directly:

```sh
make build
./crawler -fresh
```

Stop the previous crawler before starting another instance. The legacy `resume`/`restart` helpers do not reliably wait for it to finish. The old `reset-bloom` and `clean` targets have been removed; some existing Bloom error messages still refer to `reset-bloom`.

## Monitoring

- Production metrics: `http://localhost:2112/metrics`.
- Test metrics while running: `http://localhost:2113/metrics`.
- Production Prometheus: `http://localhost:9090`.

Useful metrics include `pages_processed`, `video_found`, `targets_found`, `fetch_duration_seconds`, `frontier_ready`, `frontier_processing`, `frontier_delayed`, `bloom_fill_ratio`, `lease_errors_total`, `resolve_errors_total`, and `reconcile_errors_total`.

`video_found` counts parsed video responses before Mongo writes; it is not a unique stored-record count. Use Mongo counts and test worker summaries to assess storage and proxy coverage.

## Current validation limits

The September 22, 2026 local run started all 100 workers and made 400 requests within the two-minute crawl window. Worker summaries recorded 48 stored videos through 40 proxies; 60 proxies remained unverified. Five responses were HTTP 429. No concurrent processing of the same URL was observed. The final checkpoint completed and test containers, volumes and network were removed.

That run **failed** the 200-video/all-proxy criteria. It did not exercise five-consecutive-rate-limit retirement or authenticated access. An offline build and a bounded crawl do not establish production readiness or universal deduplication guarantees.

## Project structure

```text
cmd/crawler/                  Startup, mode routing and shutdown
internal/auth/                HTTP clients, login and shared cookies
internal/config/              Environment loading and fixed test settings
internal/crawler/             Parsing, workers and integrated test checks
internal/storage/             Redis frontier/Bloom, Mongo and crawl-state recovery
internal/scheduler/           Reconciliation, listing scheduling and checkpoints
internal/metrics/             Metrics and local stop endpoint
internal/model/               Video records and target matching
docker-compose.yml            Production services
docker-compose.test.yml       Disposable test services
k8s/                          Local kind deployment manifests
seeds.txt                     Optional startup URLs for -fresh
```

## Local Kubernetes

The existing kind deployment uses one crawler replica with `Recreate` strategy. Multiple crawler processes sharing one frontier are outside the current ownership model.

With kind and kubectl available, prepare the secret file from the example, fill in its credentials, then use the existing targets:

```sh
cp k8s/crawler-secret.example.yaml k8s/crawler-secret.yaml
make k8s-up
make k8s-verify
# When finished:
make k8s-down
```

The isolated `make test` workflow runs locally with Docker Compose; it does not validate Kubernetes deployment or production shutdown durability.

## License

GPL-3.0. See [LICENSE](LICENSE).
