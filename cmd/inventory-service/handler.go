package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/FlowOrchestrator/floworch/internal/idempotency"
	"github.com/FlowOrchestrator/floworch/internal/kafka"
	"github.com/FlowOrchestrator/floworch/internal/observability"
	"github.com/FlowOrchestrator/floworch/internal/retry"
	"github.com/FlowOrchestrator/floworch/internal/workflow"
	"github.com/FlowOrchestrator/floworch/pkg/events"
	"github.com/google/uuid"
	segkafka "github.com/segmentio/kafka-go"
)

const serviceName = "inventory-service"

type InventoryHandler struct {
	producer  *kafka.Producer
	engine    *workflow.Engine
	idem      *idempotency.Store
	scheduler *retry.Scheduler
}

func (h *InventoryHandler) Handle(ctx context.Context, msg segkafka.Message) error {
	var evt events.PaymentEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		log.Printf("decode error: %v — skipping", err)
		return nil
	}

	// Only act on PAYMENT_SUCCESS — ignore PAYMENT_FAILED silently.
	if evt.Status != events.PaymentStatusSuccess {
		log.Printf("ignoring %s for order %s", evt.EventType, evt.OrderID)
		return nil
	}

	log.Printf("received PAYMENT_SUCCESS | order=%s attempt=%d", evt.OrderID, evt.AttemptCount)

	// ── Idempotency ───────────────────────────────────────────────────────
	idemKey := fmt.Sprintf("%s:%s", serviceName, evt.EventID)
	if err := h.idem.CheckAndMark(ctx, idemKey, evt.OrderID, evt.EventType); err != nil {
		if errors.Is(err, idempotency.ErrDuplicate) {
			log.Printf("duplicate event %s — skipping", evt.EventID)
			observability.EventsProcessed.WithLabelValues(serviceName, evt.EventType, "duplicate").Inc()
			return nil
		}
		return fmt.Errorf("idempotency: %w", err)
	}

	// ── Advance state: PAYMENT_SUCCESS → INVENTORY_PROCESSING ────────────
	if err := h.engine.Transition(ctx, workflow.TransitionInput{
		OrderID:   evt.OrderID,
		ToState:   workflow.StateInventoryProcessing,
		EventType: evt.EventType,
		Service:   serviceName,
	}); err != nil {
		log.Printf("transition error (non-fatal): %v", err)
	}

	// ── Reserve stock ─────────────────────────────────────────────────────
	time.Sleep(time.Duration(30+rand.Intn(70)) * time.Millisecond) // simulate latency
	reservationID := uuid.New().String()

	outEvt := events.InventoryEvent{
		BaseEvent: events.BaseEvent{
			EventID:        uuid.New().String(),
			EventType:      events.EventInventoryReserved,
			OrderID:        evt.OrderID,
			CorrelationID:  evt.CorrelationID,
			IdempotencyKey: fmt.Sprintf("%s:%s:reserved", serviceName, evt.EventID),
			AttemptCount:   0,
			Timestamp:      time.Now(),
			Version:        "v1",
		},
		Status:        events.InventoryStatusReserved,
		ReservationID: reservationID,
	}

	if err := h.producer.Publish(ctx, kafka.TopicInventoryEvents, evt.OrderID, outEvt); err != nil {
		return fmt.Errorf("publish INVENTORY_RESERVED: %w", err)
	}

	if err := h.engine.Transition(ctx, workflow.TransitionInput{
		OrderID:   evt.OrderID,
		ToState:   workflow.StateInventoryReserved,
		EventType: events.EventInventoryReserved,
		Service:   serviceName,
	}); err != nil {
		log.Printf("transition error (non-fatal): %v", err)
	}

	observability.EventsProcessed.WithLabelValues(serviceName, events.EventInventoryReserved, "success").Inc()
	log.Printf("📦 INVENTORY_RESERVED | order=%s reservation=%s", evt.OrderID, reservationID)
	return nil
}
