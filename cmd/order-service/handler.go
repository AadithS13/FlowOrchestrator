package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/FlowOrchestrator/floworch/internal/kafka"
	"github.com/FlowOrchestrator/floworch/internal/logger"
	"github.com/FlowOrchestrator/floworch/internal/observability"
	"github.com/FlowOrchestrator/floworch/internal/workflow"
	"github.com/FlowOrchestrator/floworch/pkg/events"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const serviceName = "order-service"

type OrderHandler struct {
	log      zerolog.Logger
	db       *sql.DB
	producer *kafka.Producer
	engine   *workflow.Engine
}

type CreateOrderRequest struct {
	CustomerID string             `json:"customer_id"`
	Items      []events.OrderItem `json:"items"`
}

type CreateOrderResponse struct {
	OrderID       string    `json:"order_id"`
	Status        string    `json:"status"`
	Amount        float64   `json:"amount"`
	CorrelationID string    `json:"correlation_id"`
	CreatedAt     time.Time `json:"created_at"`
}

func (h *OrderHandler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.CustomerID == "" {
		jsonError(w, "customer_id is required", http.StatusBadRequest)
		return
	}
	if len(req.Items) == 0 {
		jsonError(w, "items cannot be empty", http.StatusBadRequest)
		return
	}

	var total float64
	for _, item := range req.Items {
		if item.Quantity <= 0 || item.Price <= 0 {
			jsonError(w, "each item must have quantity > 0 and price > 0", http.StatusBadRequest)
			return
		}
		total += float64(item.Quantity) * item.Price
	}

	orderID       := uuid.New().String()
	correlationID := uuid.New().String()
	now           := time.Now()

	// Enrich logger with correlation fields
	log := logger.FromContext(
		logger.WithCorrelation(r.Context(), correlationID, orderID),
		h.log,
	)

	itemsJSON, _ := json.Marshal(req.Items)
	_, err := h.db.ExecContext(r.Context(),
		`INSERT INTO orders (id, customer_id, amount, items, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		orderID, req.CustomerID, total, itemsJSON, workflow.StatePending, now, now,
	)
	if err != nil {
		log.Error().Err(err).Msg("db insert failed")
		jsonError(w, "failed to create order", http.StatusInternalServerError)
		return
	}

	eventID := uuid.New().String()
	evt := events.OrderCreatedEvent{
		BaseEvent: events.BaseEvent{
			EventID:        eventID,
			EventType:      events.EventOrderCreated,
			OrderID:        orderID,
			CorrelationID:  correlationID,
			IdempotencyKey: fmt.Sprintf("%s:%s", serviceName, eventID),
			AttemptCount:   0,
			Timestamp:      now,
			Version:        "v1",
		},
		CustomerID: req.CustomerID,
		Amount:     total,
		Items:      req.Items,
	}

	if err := h.producer.Publish(r.Context(), kafka.TopicOrderCreated, orderID, evt); err != nil {
		log.Error().Err(err).Msg("kafka publish failed")
		jsonError(w, "order created but event publish failed", http.StatusAccepted)
		return
	}

	if err := h.engine.Transition(r.Context(), workflow.TransitionInput{
		OrderID:   orderID,
		ToState:   workflow.StatePaymentProcessing,
		EventType: events.EventOrderCreated,
		Service:   serviceName,
	}); err != nil {
		log.Warn().Err(err).Msg("workflow transition error (non-fatal)")
	}

	observability.EventsProcessed.WithLabelValues(serviceName, events.EventOrderCreated, "success").Inc()
	observability.HTTPRequestDuration.WithLabelValues("POST", "/orders", "201").Observe(time.Since(start).Seconds())

	log.Info().
		Str("customer_id", req.CustomerID).
		Float64("amount", total).
		Int("items", len(req.Items)).
		Msg("ORDER_CREATED published")

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(CreateOrderResponse{
		OrderID:       orderID,
		Status:        string(workflow.StatePaymentProcessing),
		Amount:        total,
		CorrelationID: correlationID,
		CreatedAt:     now,
	})
}

type OrderStateResponse struct {
	OrderID    string          `json:"order_id"`
	Status     string          `json:"status"`
	Amount     float64         `json:"amount"`
	CustomerID string          `json:"customer_id"`
	Items      json.RawMessage `json:"items"`
	History    []WorkflowEvent `json:"history"`
	CreatedAt  time.Time       `json:"created_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

type WorkflowEvent struct {
	FromState string    `json:"from_state"`
	ToState   string    `json:"to_state"`
	EventType string    `json:"event_type"`
	Service   string    `json:"service"`
	At        time.Time `json:"at"`
}

func (h *OrderHandler) GetOrder(w http.ResponseWriter, r *http.Request) {
	orderID := strings.TrimPrefix(r.URL.Path, "/orders/")
	if orderID == "" {
		jsonError(w, "order id required", http.StatusBadRequest)
		return
	}

	var resp OrderStateResponse
	err := h.db.QueryRowContext(r.Context(),
		`SELECT id, customer_id, amount, items, status, created_at, updated_at
		 FROM orders WHERE id = $1`, orderID,
	).Scan(&resp.OrderID, &resp.CustomerID, &resp.Amount,
		&resp.Items, &resp.Status, &resp.CreatedAt, &resp.UpdatedAt)
	if err == sql.ErrNoRows {
		jsonError(w, "order not found", http.StatusNotFound)
		return
	}
	if err != nil {
		jsonError(w, "internal error", http.StatusInternalServerError)
		return
	}

	rows, err := h.db.QueryContext(r.Context(),
		`SELECT COALESCE(from_state,''), to_state, COALESCE(event_type,''), COALESCE(service,''), created_at
		 FROM workflow_states WHERE order_id = $1 ORDER BY created_at ASC`, orderID,
	)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var e WorkflowEvent
			rows.Scan(&e.FromState, &e.ToState, &e.EventType, &e.Service, &e.At)
			resp.History = append(resp.History, e)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
