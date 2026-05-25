# FlowOrchestrator

> **Kafka-based async workflow orchestration engine in Go.**
> Demonstrates production-grade patterns: event-driven microservices, workflow state machines, exponential-backoff retries, dead-letter queues, idempotency, and full observability.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            FlowOrchestrator                                 │
│                                                                             │
│  ┌──────────────┐  POST /orders  ┌──────────────────────────────────────┐  │
│  │  HTTP Client │───────────────▶│          Order Service :8081          │  │
│  └──────────────┘                │  • Validates + persists order (PG)   │  │
│                                  │  • Publishes ORDER_CREATED            │  │
│                                  │  • State: PENDING→PAYMENT_PROCESSING  │  │
│                                  └──────────────┬───────────────────────┘  │
│                                                 │ order.created             │
│                                    ┌────────────▼─────────────┐            │
│                                    │     [Kafka Broker]        │            │
│                                    │  • order.created          │            │
│                                    │  • payment.events         │            │
│                                    │  • inventory.events       │            │
│                                    │  • notification.events    │            │
│                                    │  • retry.events           │            │
│                                    │  • dlq.events             │            │
│                                    └──┬──────────┬────────────┘            │
│                          order.created│          │payment.events            │
│                   ┌────────────────────┘          └──────────────────┐     │
│                   ▼                                                   ▼     │
│  ┌────────────────────────────┐           ┌──────────────────────────────┐ │
│  │     Payment Service :8082  │           │   Inventory Service :8083    │ │
│  │  • Idempotency check       │           │  • Idempotency check         │ │
│  │  • 70% success / 30% fail  │           │  • Reserves stock (simul.)   │ │
│  │  • Exponential backoff     │           │  • State: →INVENTORY_PROC    │ │
│  │  • DLQ after 3 attempts    │           │  • Publishes INVENTORY_RESVD │ │
│  │  • Configurable fail rate  │           └──────────────────────────────┘ │
│  └────────────────────────────┘                                             │
│         │ payment.events                payment.events + inventory.events   │
│         │                        ┌──────────────────────────────────────┐  │
│         └───────────────────────▶│    Notification Service :8084        │  │
│                                  │  • Listens on 2 topics simultaneously │  │
│                                  │  • EMAIL on payment success/fail      │  │
│                                  │  • SMS on inventory reserved          │  │
│                                  │  • Final state: →COMPLETED            │  │
│                                  └──────────────────────────────────────┘  │
│                                                                             │
│  ┌──────────────────────────────────────────────────────────────────────┐  │
│  │                     PostgreSQL                                        │  │
│  │  orders │ workflow_states │ idempotency_keys │ retry_events │ dlq    │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────────────────┐   │
│  │   Prometheus    │  │     Grafana      │  │       Kafka UI           │   │
│  │  :9090          │  │  :3000           │  │  :8090                   │   │
│  └─────────────────┘  └─────────────────┘  └──────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Workflow State Machine

```
                        ┌─────────┐
                        │ PENDING │
                        └────┬────┘
                             │ ORDER_CREATED
                             ▼
                  ┌──────────────────────┐
                  │  PAYMENT_PROCESSING  │◀──── retry (backoff)
                  └──────┬──────────────┘
               success   │       │ failure
              ───────────┘       └──────────────────────────┐
              ▼                                              ▼
  ┌───────────────────┐                         ┌───────────────────┐
  │  PAYMENT_SUCCESS  │                         │  PAYMENT_FAILED   │
  └─────────┬─────────┘                         └────────┬──────────┘
            │                                            │ max retries
            ▼                                            ▼
 ┌─────────────────────┐                          ┌──────────┐
 │ INVENTORY_PROCESSING│                          │   DLQ    │
 └──────────┬──────────┘                          └──────────┘
            │
            ▼
 ┌─────────────────────┐
 │  INVENTORY_RESERVED │
 └──────────┬──────────┘
            │
            ▼
      ┌───────────┐
      │ COMPLETED │
      └───────────┘
```

Every transition is **validated** — a stale retry can never roll back a completed order.

---

## Event Flow

| Step | Producer | Topic | Consumer(s) |
|---|---|---|---|
| 1 | Order Service | `order.created` | Payment Service |
| 2 | Payment Service | `payment.events` | Inventory, Notification |
| 3 | Inventory Service | `inventory.events` | Notification |
| 4 | Any service | `retry.events` | Retry Scheduler |
| 5 | Retry Scheduler | `dlq.events` | DLQ Consumer / Alerts |

**Partition key:** `order_id` on every topic — guarantees all events for one order land on the same partition, preserving ordering within an order's lifecycle.

---

## Retry Architecture

```
Event fails
    │
    ├── attempt < 3 ──▶ write retry_events (next_retry_at = now + backoff)
    │                   Retry scheduler polls every 5s: FOR UPDATE SKIP LOCKED
    │                   Republish to original topic with attempt_count++
    │
    └── attempt = 3 ──▶ write dlq_events
                        Publish to dlq.events (for alerting)
                        State → DLQ

Backoff formula: min(1s × 2^attempt, 30s) ± 10% jitter

  Attempt 0 → fails → retry in ~1s
  Attempt 1 → fails → retry in ~2s
  Attempt 2 → fails → retry in ~4s
  Attempt 3 → max retries exceeded → DLQ
```

**Why `FOR UPDATE SKIP LOCKED`?**
If you run multiple replicas of the payment service, two instances polling `retry_events` simultaneously would both pick up the same row. `SKIP LOCKED` makes each replica skip rows already locked by another — zero duplicate retries, no coordination needed.

---

## Idempotency

Every consumer checks a key before processing:

```
key = "{service}:{event_id}"
e.g. "payment-service:550e8400-e29b-41d4-a716-446655440000"

INSERT INTO idempotency_keys (key, ...) ON CONFLICT DO NOTHING
  → 0 rows affected = duplicate → skip
  → 1 row affected  = fresh     → process
```

This is a **single atomic operation** — no SELECT then INSERT race condition. If `PAYMENT_SUCCESS` is retried 3 times, the customer is charged exactly once.

---

## Observability

### Prometheus Metrics

| Metric | Type | Labels |
|---|---|---|
| `floworch_events_processed_total` | Counter | service, event_type, status |
| `floworch_event_duration_seconds` | Histogram | service, event_type |
| `floworch_retry_scheduled_total` | Counter | service, event_type, attempt |
| `floworch_dlq_total` | Counter | service, event_type |
| `floworch_workflow_state_orders` | Gauge | state |
| `floworch_http_request_duration_seconds` | Histogram | method, path, status |

### Structured Logging (zerolog)

Every log line carries:
```json
{
  "level": "info",
  "service": "payment-service",
  "correlation_id": "abc-123",
  "order_id": "xyz-456",
  "attempt": 1,
  "time": "11:05:23",
  "message": "✅ PAYMENT_SUCCESS"
}
```

`correlation_id` is set on order creation and propagated through every event — you can `grep correlation_id=abc-123` across all service logs to see the entire journey of a single order.

---

## Failure Simulation API

Control the payment failure rate **live** without restarting anything:

```bash
# Force 100% failure — watch retries then DLQ fill up
curl -X POST http://localhost:8082/config/payment-failure-rate \
     -H "Content-Type: application/json" \
     -d '{"rate": 100}'

# Recover — watch DLQ orders stay stuck (already exhausted), new orders succeed
curl -X POST http://localhost:8082/config/payment-failure-rate \
     -d '{"rate": 0}'

# Back to realistic 30%
curl -X POST http://localhost:8082/config/payment-failure-rate \
     -d '{"rate": 30}'

# Check current rate
curl http://localhost:8082/config/payment-failure-rate
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Go 1.22 |
| Message broker | Apache Kafka (Confluent 7.5) |
| Database | PostgreSQL 16 |
| Metrics | Prometheus + Grafana |
| Logging | zerolog (structured JSON) |
| Kafka client | segmentio/kafka-go |
| Containerisation | Docker Compose |

---

## Running Locally

### Prerequisites
- Go 1.22+
- Docker Desktop

### 1. Start infrastructure

```bash
make up
```

Starts: Kafka, Zookeeper, PostgreSQL, Prometheus, Grafana, Kafka UI.

### 2. Run migrations

```bash
# Migrations run automatically via docker-entrypoint-initdb.d
# If your DB is local postgres, run the SQL in DBeaver or:
psql -U postgres -d floworch -f migrations/001_orders.sql
# ... repeat for 002–005
```

### 3. Start services (4 terminals)

```bash
go run ./cmd/order-service/...
go run ./cmd/payment-service/...
go run ./cmd/inventory-service/...
go run ./cmd/notification-service/...
```

### 4. Fire an order

```bash
curl -s -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "cust-001",
    "items": [
      {"product_id": "prod-1", "name": "Laptop", "quantity": 1, "price": 999.99},
      {"product_id": "prod-2", "name": "Mouse",  "quantity": 2, "price": 29.99}
    ]
  }' | jq .
```

### 5. Track the order

```bash
curl -s http://localhost:8081/orders/<order_id> | jq .
```

---

## Demo Scenarios

### Happy path
```bash
curl -X POST .../config/payment-failure-rate -d '{"rate":0}'
# POST /orders → watch all 4 terminals light up → order COMPLETED in ~300ms
```

### Retry + recovery
```bash
curl -X POST .../config/payment-failure-rate -d '{"rate":100}'
# POST /orders → fails → retries at 1s, 2s, 4s → DLQ
curl -X POST .../config/payment-failure-rate -d '{"rate":0}'
# POST new order → succeeds immediately
```

### Watch in Kafka UI
Open http://localhost:8090 → Topics → click any topic → Messages tab.
You can see every event flowing through the system in real time.

### Watch in Grafana
Open http://localhost:3000 (admin/admin) → Dashboards → FlowOrchestrator.
Retry rate and DLQ count spike when failure rate = 100%.

---

## Scaling Decisions

| Decision | Reason |
|---|---|
| Partition by `order_id` | Preserves event ordering within a single order across retries |
| `FOR UPDATE SKIP LOCKED` | Allows N replicas of any service to poll `retry_events` safely |
| `ON CONFLICT DO NOTHING` for idempotency | Single atomic operation — no SELECT+INSERT race |
| Serializable isolation for state transitions | Prevents two services transitioning the same order simultaneously |
| Append-only `workflow_states` | Never lose audit history; enables full order replay |
| Separate `retry.events` Kafka topic | Retry processing never blocks or starves main topic consumers |

---

## Trade-offs

| Trade-off | Choice made | Alternative |
|---|---|---|
| Retry storage | PostgreSQL `retry_events` | Kafka delay topics (simpler ops) |
| Service discovery | None — all local | Consul / k8s DNS |
| Schema evolution | `version: "v1"` in every event | Confluent Schema Registry |
| Distributed tracing | Correlation ID in logs | OpenTelemetry + Jaeger |
| Auth | None | JWT middleware on Order Service |

---

## Failure Scenarios

| Scenario | System behaviour |
|---|---|
| Payment gateway times out | Retry with exponential backoff; DLQ after 3 attempts |
| Kafka broker down | Producer/consumer block with retries; order stays PENDING |
| Postgres down | All services fail fast; Kafka offsets not committed; reprocess on restart |
| Duplicate Kafka delivery | Idempotency key prevents double-charge |
| Stale retry after order completes | State machine rejects invalid transition; logged as warning |
| Two replicas processing same retry | `FOR UPDATE SKIP LOCKED` — one wins, other skips |
| Notification service crashes mid-send | Idempotency key not yet marked; notification retried on restart |

---

## Project Structure

```
FlowOrchestrator/
├── cmd/
│   ├── order-service/         # HTTP API — POST /orders, GET /orders/:id
│   ├── payment-service/       # Kafka consumer + retry + DLQ + failure config API
│   ├── inventory-service/     # Kafka consumer — stock reservation
│   └── notification-service/  # Kafka consumer — email/SMS simulation
├── internal/
│   ├── kafka/                 # Generic producer + consumer wrappers
│   ├── workflow/              # State machine + engine (validated transitions)
│   ├── retry/                 # Policy, backoff calculator, DB scheduler
│   ├── idempotency/           # Atomic check-and-mark store
│   ├── logger/                # zerolog wrapper with correlation ID propagation
│   ├── observability/         # Prometheus metric definitions
│   └── db/                    # PostgreSQL connection pool
├── pkg/events/                # Shared event contracts (OrderCreated, Payment, etc.)
├── migrations/                # SQL schema (001–005)
├── grafana/                   # Dashboard JSON + provisioning config
├── docker-compose.yml         # Kafka, Postgres, Prometheus, Grafana, Kafka UI
├── prometheus.yml             # Scrape config
└── Makefile
```
