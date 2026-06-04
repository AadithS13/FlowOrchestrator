# Screenshots to capture

Drop PNG/GIF files in this folder, then update the README image paths.

## 1. happy-path.png
4-terminal split showing all services lighting up when an order succeeds.
- Set failure rate to 0: `curl -X POST http://localhost:8082/config/payment-failure-rate -d '{"rate":0}'`
- Fire an order
- Screenshot all 4 terminals

## 2. retry-dlq.png
Payment-service terminal showing the 3-attempt retry sequence then DLQ.
- Set failure rate to 100: `curl -X POST http://localhost:8082/config/payment-failure-rate -d '{"rate":100}'`
- Fire an order
- Screenshot the payment-service terminal

## 3. grafana-dashboard.png
Grafana at http://localhost:3000 → Dashboards → FlowOrchestrator
- Run load test first: `go run ./cmd/loadtest/... -n 100 -c 10`
- Screenshot the dashboard showing events/sec, retry rate, DLQ count, latency

## 4. kafka-ui.png
Kafka UI at http://localhost:8090 → Topics
- Click any topic (order.created or dlq.events)
- Screenshot the Messages tab with live events

## 5. dbeaver-workflow.png (optional)
DBeaver showing workflow_states rows for one order in full retry sequence.
```sql
SELECT from_state, to_state, event_type, service, created_at
FROM workflow_states
ORDER BY created_at DESC
LIMIT 20;
```
