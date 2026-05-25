package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/FlowOrchestrator/floworch/internal/idempotency"
	"github.com/FlowOrchestrator/floworch/internal/kafka"
	"github.com/FlowOrchestrator/floworch/internal/logger"
	"github.com/FlowOrchestrator/floworch/internal/observability"
	"github.com/FlowOrchestrator/floworch/internal/retry"
	"github.com/FlowOrchestrator/floworch/internal/workflow"
	"github.com/FlowOrchestrator/floworch/pkg/events"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	segkafka "github.com/segmentio/kafka-go"
)

const serviceName = "payment-service"

type PaymentHandler struct {
	log       zerolog.Logger
	producer  *kafka.Producer
	engine    *workflow.Engine
	idem      *idempotency.Store
	scheduler *retry.Scheduler
}

func (h *PaymentHandler) Handle(ctx context.Context, msg segkafka.Message) error {
	start := time.Now()

	var evt events.OrderCreatedEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		h.log.Error().Err(err).Msg("decode error — skipping malformed message")
		return nil
	}

	// Enrich context + logger with correlation fields for every downstream call
	ctx = logger.WithCorrelation(ctx, evt.CorrelationID, evt.OrderID)
	ctx = logger.WithAttempt(ctx, evt.AttemptCount)
	log := logger.FromContext(ctx, h.log)

	log.Info().
		Str("event_id", evt.EventID).
		Int("attempt", evt.AttemptCount).
		Float64("amount", evt.Amount).
		Msg("received ORDER_CREATED")

	// ── Idempotency ───────────────────────────────────────────────────────
	idemKey := fmt.Sprintf("%s:%s", serviceName, evt.EventID)
	if err := h.idem.CheckAndMark(ctx, idemKey, evt.OrderID, evt.EventType); err != nil {
		if errors.Is(err, idempotency.ErrDuplicate) {
			log.Warn().Str("idem_key", idemKey).Msg("duplicate event — skipping")
			observability.EventsProcessed.WithLabelValues(serviceName, evt.EventType, "duplicate").Inc()
			return nil
		}
		return fmt.Errorf("idempotency check: %w", err)
	}

	// ── Payment processing ────────────────────────────────────────────────
	payErr := processPayment(evt.Amount)
	observability.EventDuration.WithLabelValues(serviceName, evt.EventType).Observe(time.Since(start).Seconds())

	if payErr != nil {
		return h.handleFailure(ctx, log, evt, payErr)
	}
	return h.handleSuccess(ctx, log, evt)
}

func (h *PaymentHandler) handleSuccess(ctx context.Context, log zerolog.Logger, evt events.OrderCreatedEvent) error {
	txID := uuid.New().String()

	outEvt := events.PaymentEvent{
		BaseEvent: events.BaseEvent{
			EventID:        uuid.New().String(),
			EventType:      events.EventPaymentSuccess,
			OrderID:        evt.OrderID,
			CorrelationID:  evt.CorrelationID,
			IdempotencyKey: fmt.Sprintf("%s:%s:success", serviceName, evt.EventID),
			AttemptCount:   0,
			Timestamp:      time.Now(),
			Version:        "v1",
		},
		Status:        events.PaymentStatusSuccess,
		TransactionID: txID,
		Amount:        evt.Amount,
	}

	if err := h.producer.Publish(ctx, kafka.TopicPaymentEvents, evt.OrderID, outEvt); err != nil {
		return fmt.Errorf("publish PAYMENT_SUCCESS: %w", err)
	}

	if err := h.engine.Transition(ctx, workflow.TransitionInput{
		OrderID:   evt.OrderID,
		ToState:   workflow.StatePaymentSuccess,
		EventType: events.EventPaymentSuccess,
		Service:   serviceName,
	}); err != nil {
		log.Warn().Err(err).Msg("workflow transition error (non-fatal)")
	}

	observability.EventsProcessed.WithLabelValues(serviceName, events.EventPaymentSuccess, "success").Inc()
	log.Info().Str("transaction_id", txID).Msg("✅ PAYMENT_SUCCESS")
	return nil
}

func (h *PaymentHandler) handleFailure(ctx context.Context, log zerolog.Logger, evt events.OrderCreatedEvent, payErr error) error {
	log.Warn().
		Err(payErr).
		Int("attempt", evt.AttemptCount).
		Int("max_attempts", retry.DefaultPolicy.MaxAttempts).
		Msg("❌ payment failed")

	failEvt := events.PaymentEvent{
		BaseEvent: events.BaseEvent{
			EventID:        uuid.New().String(),
			EventType:      events.EventPaymentFailed,
			OrderID:        evt.OrderID,
			CorrelationID:  evt.CorrelationID,
			IdempotencyKey: fmt.Sprintf("%s:%s:failed:%d", serviceName, evt.EventID, evt.AttemptCount),
			AttemptCount:   evt.AttemptCount,
			Timestamp:      time.Now(),
			Version:        "v1",
		},
		Status:        events.PaymentStatusFailed,
		Amount:        evt.Amount,
		FailureReason: payErr.Error(),
	}
	_ = h.producer.Publish(ctx, kafka.TopicPaymentEvents, evt.OrderID, failEvt)

	if err := h.scheduler.Schedule(ctx, evt.OrderID, evt.EventType,
		kafka.TopicOrderCreated, evt, evt.AttemptCount, payErr,
	); err != nil {
		log.Error().Err(err).Msg("scheduler error")
	}

	toState := workflow.StatePaymentFailed
	if !retry.DefaultPolicy.ShouldRetry(evt.AttemptCount) {
		toState = workflow.StateDLQ
		log.Error().Msg("🪦 max retries exceeded — moving to DLQ")
		observability.DLQEvents.WithLabelValues(serviceName, evt.EventType).Inc()
	} else {
		delay := retry.DefaultPolicy.Delay(evt.AttemptCount)
		log.Info().Dur("retry_in", delay).Int("next_attempt", evt.AttemptCount+1).Msg("🔄 retry scheduled")
		observability.RetryScheduled.WithLabelValues(
			serviceName, evt.EventType, fmt.Sprintf("%d", evt.AttemptCount+1),
		).Inc()
	}

	if err := h.engine.Transition(ctx, workflow.TransitionInput{
		OrderID:   evt.OrderID,
		ToState:   toState,
		EventType: events.EventPaymentFailed,
		Service:   serviceName,
	}); err != nil {
		log.Warn().Err(err).Msg("workflow transition error (non-fatal)")
	}

	observability.EventsProcessed.WithLabelValues(serviceName, evt.EventType, "failed").Inc()
	return nil
}

// processPayment simulates a real payment gateway using the live failure rate.
// Rate is controlled at runtime via POST /config/payment-failure-rate.
func processPayment(amount float64) error {
	time.Sleep(time.Duration(50+rand.Intn(150)) * time.Millisecond)

	if rand.Float64() < getFailureRate() {
		reasons := []string{
			"payment gateway timeout",
			"insufficient funds",
			"card declined",
			"bank connection error",
		}
		return errors.New(reasons[rand.Intn(len(reasons))])
	}
	return nil
}
