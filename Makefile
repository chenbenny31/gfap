METRICS_PORT = 2112
BINARY = ./crawler

.PHONY: help infra-start infra-stop infra-logs build run fresh resume test stop metrics logs status restart snapshot

help:
	@echo "gfap — commands:"
	@echo "  make infra-start  start Docker services (Redis/MongoDB/Prometheus)"
	@echo "  make infra-stop   snapshot redis, then stop Docker services"
	@echo "  make snapshot     force a redis RDB snapshot now"
	@echo "  make infra-logs   log Docker services"
	@echo "  make build        build crawler binary"
	@echo "  make run          start production crawler in the background"
	@echo "  make fresh        first run — drops corpus, seeds from seeds.txt"
	@echo "  make resume       resume production crawl"
	@echo "  make test         isolated test crawl with automatic cleanup"
	@echo "  make stop         graceful crawler shutdown"
	@echo "  make metrics      print Prometheus metrics"
	@echo "  make logs         tail crawler log"
	@echo "  make status       show service and crawler status"
	@echo "  make restart      rebuild and restart crawler"
	@echo "  make k8s-up      build image, load into kind, apply manifests"
	@echo "  make k8s-down    delete kind cluster"
	@echo "  make k8s-verify  check pods, metrics, prometheus scrape"

infra-start:
	docker-compose up -d
	@sleep 3
	@docker exec gfap-redis-1 redis-cli EXISTS crawler:bloom | grep -q 1 || docker exec gfap-redis-1 redis-cli BF.RESERVE crawler:bloom 0.00001 1000000000 NONSCALING
	@echo "Bloom: $$(docker exec gfap-redis-1 redis-cli BF.INFO crawler:bloom | grep -A1 Capacity | tail -1) capacity"
	@echo "Prometheus: http://localhost:9090"
	@echo "Metrics: http://localhost:$(METRICS_PORT)/metrics"

# Automatic save points are disabled (see docker-compose.yml), and that also
# means redis does NOT save on clean shutdown - snapshot before stopping.
infra-stop: snapshot
	docker-compose down

snapshot:
	@docker exec gfap-redis-1 redis-cli BGSAVE
	@echo "waiting for rdb..."
	@until [ "$$(docker exec gfap-redis-1 redis-cli INFO persistence | grep -c 'rdb_bgsave_in_progress:0')" = "1" ]; do sleep 1; done
	@docker exec gfap-redis-1 redis-cli INFO persistence | grep -E "rdb_last_bgsave_status|rdb_last_save_time"

infra-logs:
	docker-compose logs -f

build:
	go build -o $(BINARY) cmd/crawler/main.go

run:
	@if pgrep -x crawler >/dev/null; then \
		echo "Crawler already running; use make stop first" >&2; exit 1; \
	fi
	@$(MAKE) build
	@nohup $(BINARY) </dev/null >>crawler.log 2>&1 & \
	pid=$$!; sleep 1; \
	kill -0 "$$pid" 2>/dev/null || { \
		echo "Crawler exited; see crawler.log" >&2; exit 1; \
	}; \
	echo "Crawler started (PID $$pid); logs: make logs; stop: make stop"

fresh: build
	@echo "WARNING: drops MongoDB corpus and flushes Redis, irreversible."
	@read -p "Type 'fresh' to continue: " a && [ "$$a" = "fresh" ] || { echo aborted; exit 1; }
	-@pkill -x crawler
	@nohub $(BINARY) -fresh > /dev/null 2>&1 & echo "Crawler started (fresh)"

resume: build
	-@pkill -x crawler
	@nohup $(BINARY) > /dev/null 2>&1 & echo "Crawler resumed"

test:
	@set -eu; \
	compose() { docker compose --env-file /dev/null -p gfap-test -f docker-compose.test.yml "$$@"; }; \
	binary=""; pid=""; started=0; \
	cleanup() { \
		status=$$?; trap - EXIT; trap '' INT TERM HUP; \
		if [ -n "$$pid" ]; then \
			if kill -0 "$$pid" 2>/dev/null; then kill -TERM "$$pid" || status=1; fi; \
			if wait "$$pid"; then :; else \
				child_status=$$?; \
				if [ "$$status" -eq 0 ]; then status=$$child_status; fi; \
			fi; \
		fi; \
		if [ "$$started" -eq 1 ]; then \
			compose down --volumes || { echo "Test resource cleanup failed" >&2; status=1; }; \
		fi; \
		if [ -n "$$binary" ]; then \
			rm -f -- "$$binary" || { echo "Test binary cleanup failed" >&2; status=1; }; \
		fi; \
		exit "$$status"; \
	}; \
	trap cleanup EXIT; \
	trap 'exit 130' INT; trap 'exit 143' TERM; trap 'exit 129' HUP; \
	binary=$$(mktemp /tmp/gfap-test.XXXXXX); \
	env GOTOOLCHAIN=local GOPROXY=off GONOPROXY=none GOVCS='*:off' GOWORK=off \
		go build -mod=readonly -buildvcs=false -o "$$binary" ./cmd/crawler; \
	started=1; \
	compose down --volumes; \
	compose up -d; \
	ready=0; attempt=0; \
	while [ "$$attempt" -lt 30 ]; do \
		if [ "$$(compose exec -T redis-test redis-cli --raw ping 2>/dev/null)" = PONG ] && \
			compose exec -T mongo-test mongosh --quiet \
				--eval 'quit(db.adminCommand({ping: 1}).ok ? 0 : 1)' >/dev/null 2>&1; then \
			ready=1; break; \
		fi; \
		attempt=$$((attempt + 1)); sleep 1; \
	done; \
	if [ "$$ready" -ne 1 ]; then echo "Test storage did not become ready" >&2; exit 1; fi; \
	"$$binary" -test & pid=$$!; \
	if wait "$$pid"; then result=0; else result=$$?; fi; \
	pid=""; exit "$$result"

stop:
	curl -s -X POST http://localhost:$(METRICS_PORT)/stop

metrics:
	curl -s http://localhost:2112/metrics

logs:
	tail -f crawler.log

status:
	@docker-compose ps
	@echo ""
	@pgrep -x crawler > /dev/null \
		&& echo "Crawler: running (PID $$(pgrep -x crawler))" \
		|| echo "Crawler: not running"

restart: build
	-@pkill -x crawler
	@sleep 1
	@nohup $(BINARY) > /dev/null 2>&1 & echo "Crawler restarted"

.PHONY: k8s-up k8s-down k8s-verify

k8s-up:
	@which kind > /dev/null || { echo "kind not found: https://kind.sigs.k8s.io/docs/user/quick-start/#installation"; exit 1; }
	@kind get clusters | grep -q gfap || kind create cluster --name gfap
	docker build -t gfap-crawler:dev .
	kind load docker-image gfap-crawler:dev --name gfap
	kubectl apply -f k8s/namespace.yaml
	@echo "Applying secret — ensure k8s/crawler-secret.yaml exists (copy from example and fill values)"
	@test -f k8s/crawler-secret.yaml || { echo "ERROR: k8s/crawler-secret.yaml missing"; exit 1; }
	kubectl apply -f k8s/crawler-secret.yaml
	kubectl apply -k k8s/
	kubectl rollout status deployment/gfap-crawler -n gfap --timeout=120s

k8s-down:
	kind delete cluster --name gfap

k8s-verify:
	@echo "--- pods ---"
	kubectl get pods -n gfap
	@echo "--- bloom init log ---"
	kubectl logs -n gfap -l app=gfap-crawler -c bloom-init --tail=10
	@echo "--- crawler metrics ---"
	kubectl port-forward -n gfap svc/gfap-crawler 2112:2112 &
	sleep 2
	curl -s http://localhost:2112/metrics | grep -E "pages_processed|video_found|targets_found"
	@kill %1 2>/dev/null || true
	@echo "--- prometheus ---"
	kubectl port-forward -n gfap svc/prometheus 9090:9090 &
	sleep 2
	curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool | grep -A2 gfap-crawler
	@kill %1 2>/dev/null || true
