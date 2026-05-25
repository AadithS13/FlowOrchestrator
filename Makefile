.PHONY: up down logs ps deps build \
        run-order run-payment run-inventory run-notification \
        topics list-topics migrate

# ─── Infrastructure ───────────────────────────────────────────────────────────

up:
	docker compose up -d
	@echo "Kafka UI  → http://localhost:8090"
	@echo "Prometheus → http://localhost:9090"
	@echo "Postgres  → localhost:5432  (postgres / postgres)"

down:
	docker compose down -v

logs:
	docker compose logs -f

ps:
	docker compose ps

# ─── Go deps ─────────────────────────────────────────────────────────────────

deps:
	go mod tidy
	go mod download

build:
	go build ./...

# ─── Run services locally ────────────────────────────────────────────────────

run-order:
	go run ./cmd/order-service/...

run-payment:
	go run ./cmd/payment-service/...

run-inventory:
	go run ./cmd/inventory-service/...

run-notification:
	go run ./cmd/notification-service/...

# ─── Kafka helpers ───────────────────────────────────────────────────────────

list-topics:
	docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092

# ─── DB helpers ──────────────────────────────────────────────────────────────

psql:
	docker compose exec postgres psql -U postgres -d floworch
