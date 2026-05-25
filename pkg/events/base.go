package events

import "time"

// Event type constants — the single source of truth used by all services.
const (
	EventOrderCreated      = "ORDER_CREATED"
	EventPaymentSuccess    = "PAYMENT_SUCCESS"
	EventPaymentFailed     = "PAYMENT_FAILED"
	EventInventoryReserved = "INVENTORY_RESERVED"
	EventInventoryFailed   = "INVENTORY_FAILED"
	EventNotificationSent  = "NOTIFICATION_SENT"
)

// BaseEvent is the envelope every domain event embeds.
// Fields here drive retry routing, idempotency, and observability —
// they must be present on every message published to Kafka.
type BaseEvent struct {
	EventID        string    `json:"event_id"`         // unique per publish (uuid)
	EventType      string    `json:"event_type"`       // one of the constants above
	OrderID        string    `json:"order_id"`         // partition key
	CorrelationID  string    `json:"correlation_id"`   // constant across all events for one order
	IdempotencyKey string    `json:"idempotency_key"`  // "{service}:{event_id}"
	AttemptCount   int       `json:"attempt_count"`    // 0 on first publish; incremented by retry scheduler
	Timestamp      time.Time `json:"timestamp"`
	Version        string    `json:"version"` // "v1" — bump when schema changes
}
