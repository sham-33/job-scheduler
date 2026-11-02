BIN := bin

.PHONY: help redis-up redis-down redis-cli build test vet tidy run-worker

help: ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

redis-up: ## Start Redis in Docker and wait until it is healthy
	docker compose up -d --wait redis

redis-down: ## Stop Redis (data is kept in the redis-data volume)
	docker compose down

redis-cli: ## Open a redis-cli shell in the container
	docker compose exec redis redis-cli

build: ## Build every binary into ./bin
	go build -o $(BIN)/ ./cmd/...

test: ## Run all tests with the race detector (Redis tests skip if Redis is down)
	go test -race ./...

vet: ## Run go vet
	go vet ./...

tidy: ## Sync go.mod and go.sum with the imports
	go mod tidy

run-worker: ## Run a worker against REDIS_ADDR (default localhost:6379)
	go run ./cmd/worker
