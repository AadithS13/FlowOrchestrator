package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// EventsProcessed counts every event handled, labelled by outcome.
	// status: "success" | "duplicate" | "error" | "dlq"
	EventsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "floworch_events_processed_total",
		Help: "Total Kafka events processed.",
	}, []string{"service", "event_type", "status"})

	// EventDuration measures end-to-end handler latency.
	EventDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "floworch_event_duration_seconds",
		Help:    "Kafka event processing duration.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"service", "event_type"})

	// RetryScheduled counts events routed to the retry queue.
	RetryScheduled = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "floworch_retry_scheduled_total",
		Help: "Events scheduled for retry.",
	}, []string{"service", "event_type", "attempt"})

	// DLQEvents counts events that exhausted all retries.
	DLQEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "floworch_dlq_total",
		Help: "Events moved to the dead-letter queue.",
	}, []string{"service", "event_type"})

	// WorkflowState tracks live order counts per state (gauge so it can go down).
	WorkflowState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "floworch_workflow_state_orders",
		Help: "Current number of orders in each workflow state.",
	}, []string{"state"})

	// HTTPRequestDuration tracks latency for the Order Service HTTP API.
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "floworch_http_request_duration_seconds",
		Help:    "HTTP request duration for the Order Service.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "path", "status"})
)
