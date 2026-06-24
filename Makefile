METRICS_PORT = 2112
BINARY = ./crawler

.PHONY: help infra-start infra-stop infra-logs build fresh resume test stop metrics logs reset-bloom

help:
	@echo "gfap — commands:"
	@echo "  make infra-start  start Docker services (Redis/MongoDB/Prometheus)"
	@echo "  make infra-stop   stop Docker services"
	@echo "  make infra-logs   log Docker services"
	@echo "  make build        build crawler binary"
	@echo "  make fresh        first run — drops corpus, seeds from seeds.txt"
	@echo "  make resume       resume production crawl"
	@echo " make reset-bloom   delete bloom filter only, safe for MongoDB"
	@echo "  make test         bounded test crawl"
	@echo "  make stop         graceful crawler shutdown"
	@echo "  make metrics      print Prometheus metrics"
	@echo "  make logs         tail crawler log"
	@echo "  make status       show service and crawler status"
	@echo "  make restart      rebuild and restart crawler"
	@echo "  make clean        stop everything and remove all data"

infra-start:
	docker-compose up -d
	@sleep 3
	@docker exec gfap-redis-1 redis-cli EXISTS crawler:bloom | grep -q 1 || docker exec gfap-redis-1 redis-cli BF.RESERVE crawler:bloom 0.00001 1000000000 NONSCALING
	@echo "Bloom: $$(docker exec gfap-redis-1 redis-cli BF.INFO crawler:bloom | grep -A1 Capacity | tail -1) capacity"
	@echo "Prometheus: http://localhost:9090"
	@echo "Metrics: http://localhost:$(METRICS_PORT)/metrics"

infra-stop:
	docker-compose down

infra-logs:
	docker-compose logs -f

build:
	go build -o $(BINARY) cmd/crawler/main.go

fresh: build
	@echo "WARNING: drops MongoDB corpus and flushes Redis, irreversible."
	@read -p "Type 'fresh' to continue: " a && [ "$$a" = "fresh" ] || { echo aborted; exit 1; }
	-@pkill -x crawler
	@nohub $(BINARY) -fresh > /dev/null 2>&1 & echo "Crawler started (fresh)"

resume: build
	-@pkill -x crawler
	@nohup $(BINARY) > /dev/null 2>&1 & echo "Crawler resumed"

reset-bloom:
	docker exec gfap-redis-1 redis-cli DEL crawler:bloom
	@echo "Bloom filter deleted. Run make resume to re-init and re-seed from MongoDB."

test:
	go run cmd/crawler/main.go -test

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

clean:
	@echo "WARNING: deletes all data"
	-@pkill -x crawler
	-docker exec gfap-redis-1 redis-cli FLUSHALL
	docker-compose down -v
