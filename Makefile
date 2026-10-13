.PHONY: run infra migrate test bench

infra: ## start postgres + redis and wait until healthy
	docker compose up -d --wait postgres redis

migrate: infra ## apply SQL migrations in order (all are idempotent)
	@for f in migrations/*.sql; do \
		echo "applying $$f"; \
		docker compose exec -T postgres psql -q -U scheduler -d scheduler -v ON_ERROR_STOP=1 < $$f || exit 1; \
	done

run: migrate ## start everything: infra + api + 2 schedulers (leader + standby) + 3 workers
	@trap 'kill 0' INT TERM; \
	go run ./cmd/api & \
	NODE_ID=scheduler-1 go run ./cmd/scheduler & \
	NODE_ID=scheduler-2 go run ./cmd/scheduler & \
	WORKER_ID=worker-1 go run ./cmd/worker & \
	WORKER_ID=worker-2 go run ./cmd/worker & \
	WORKER_ID=worker-3 go run ./cmd/worker & \
	wait

test: ## run all tests (integration tests skip if postgres/redis are down; run `make migrate` first)
	go test -p 1 ./... # -p 1: integration tests share one Postgres; parallel packages would steal each other's due jobs

bench: ## polling vs timing wheel (M2); the Postgres bench skips if the DB is down
	go test ./internal/scheduler -bench=. -benchmem -run='^$$'
