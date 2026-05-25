package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/FlowOrchestrator/floworch/internal/idempotency"
	"github.com/FlowOrchestrator/floworch/internal/kafka"
	"github.com/FlowOrchestrator/floworch/internal/observability"
	"github.com/FlowOrchestrator/floworch/internal/workflow"
	"github.com/FlowOrchestrator/floworch/pkg/events"
	"github.com/google/uuid"
	segkafka "github.com/segmentio/kafka-go"
)

const serviceName = "notification-service"

type NotificationHandler struct {
	producer *kafka.Producer
	engine   *workflow.Engine
	idem     *idempotency.Store
}

// HandlePayment processes PAYMENT_SUCCESS and PAYMENT_FAILED events.
func (h *NotificationHandler) HandlePayment(ctx context.Context, msg segkafka.Message) error {
	var evt events.PaymentEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return nil // malformed — skip
	}

	idemKey := fmt.Sprintf("%s:pay:%s", serviceName, evt.EventID)
	if err := h.idem.CheckAndMark(ctx, idemKey, evt.OrderID, evt.EventType); err != nil {
		if errors.Is(err, idempotency.ErrDuplicate) {
			return nil
		}
		return err
	}

	switch evt.Status {
	case events.PaymentStatusSuccess:
		h.sendNotification(ctx, evt.OrderID, evt.CorrelationID, evt.EventID,
			"EMAIL", "Your payment was successful! 🎉",
			"payment_success")

	case events.PaymentStatusFailed:
		h.sendNotification(ctx, evt.OrderID, evt.CorrelationID, evt.EventID,
			"EMAIL", fmt.Sprintf("Payment failed: %s. We'll retry automatically.", evt.FailureReason),
			"payment_failed")
	}

	return nil
}

// HandleInventory processes INVENTORY_RESERVED events → marks order COMPLETED.
func (h *NotificationHandler) HandleInventory(ctx context.Context, msg segkafka.Message) error {
	var evt events.InventoryEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return nil
	}

	if evt.Status != events.InventoryStatusReserved {
		return nil
	}

	idemKey := fmt.Sprintf("%s:inv:%s", serviceName, evt.EventID)
	if err := h.idem.CheckAndMark(ctx, idemKey, evt.OrderID, evt.EventType); err != nil {
		if errors.Is(err, idempotency.ErrDuplicate) {
			return nil
		}
		return err
	}

	h.sendNotification(ctx, evt.OrderID, evt.CorrelationID, evt.EventID,
		"SMS", "Your order is confirmed and stock is reserved! 📦",
		"order_completed")

	// Final state transition: INVENTORY_RESERVED → COMPLETED
	if err := h.engine.Transition(ctx, workflow.TransitionInput{
		OrderID:   evt.OrderID,
		ToState:   workflow.StateCompleted,
		EventType: events.EventInventoryReserved,
		Service:   serviceName,
	}); err != nil {
		log.Printf("transition to COMPLETED error (non-fatal): %v", err)
	}

	observability.WorkflowState.WithLabelValues(string(workflow.StateCompleted)).Inc()
	log.Printf("🏁 ORDER COMPLETED | order=%s", evt.OrderID)
	return nil
}

func (h *NotificationHandler) sendNotification(
	ctx context.Context,
	orderID, correlationID, triggerEventID string,
	channel, message, template string,
) {
	// Simulate sending — in production this calls SendGrid / Twilio / SES.
	log.Printf("📬 [%s] order=%s | %s", channel, orderID, message)

	outEvt := events.NotificationEvent{
		BaseEvent: events.BaseEvent{
			EventID:        uuid.New().String(),
			EventType:      events.EventNotificationSent,
			OrderID:        orderID,
			CorrelationID:  correlationID,
			IdempotencyKey: fmt.Sprintf("%s:%s:%s", serviceName, triggerEventID, channel),
			AttemptCount:   0,
			Timestamp:      time.Now(),
			Version:        "v1",
		},
		Channel:  channel,
		Template: template,
		Status:   "SENT",
	}

	if err := h.producer.Publish(ctx, kafka.TopicNotificationEvents, orderID, outEvt); err != nil {
		log.Printf("publish NOTIFICATION_SENT error: %v", err)
	}

	observability.EventsProcessed.WithLabelValues(serviceName, events.EventNotificationSent, "success").Inc()
}
