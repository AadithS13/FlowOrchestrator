package kafka

// Topic names — single source of truth. Import this package instead of hardcoding strings.
const (
	TopicOrderCreated       = "order.created"
	TopicPaymentEvents      = "payment.events"
	TopicInventoryEvents    = "inventory.events"
	TopicNotificationEvents = "notification.events"
	TopicRetryEvents        = "retry.events"
	TopicDLQEvents          = "dlq.events"
)

// ConsumerGroups — one per service so each service gets every message independently.
const (
	GroupPaymentService      = "payment-service"
	GroupInventoryService    = "inventory-service"
	GroupNotificationService = "notification-service"
	GroupRetryScheduler      = "retry-scheduler"
	GroupDLQConsumer         = "dlq-consumer"
)
