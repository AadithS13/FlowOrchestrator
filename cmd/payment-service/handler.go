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

	ctx = logger.WithCorrelation(ctx, evt.CorrelationID, evt.OrderID)
	ctx = logger.WithAttempt(ctx, evt.AttemptCount)
	log := logger.FromContext(ctx, h.log)

	log.Info().
		Str("event_id", evt.EventID).
		Int("attempt", evt.AttemptCount).
		Float64("amount", evt.Amount).
		Msg("received ORDER_CREATED")

	// ── Idempotency ───────────────────────────────────────────────────────
	// attempt_count is part of the key: same event_id is reused across
	// retries, so each attempt needs its own unique idempotency slot.
	idemKey := fmt.Sprintf("%s:%s:%d", serviceName, evt.EventID, evt.AttemptCount)
	if err := h.idem.CheckAndMark(ctx, idemKey, evt.OrderID, evt.EventType); err != nil {
		if errors.Is(err, idempotency.ErrDuplicate) {
			log.Warn().Str("idem_key", idemKey).Msg("duplicate event — skipping")
			observability.EventsProcessed.WithLabelValues(serviceName, evt.EventType, "duplicate").Inc()
			return nil
		}
		return fmt.Errorf("idempotency check: %w", err)
	}

	// ── Advance state on retry: RETRYING_PAYMENT → PAYMENT_PROCESSING ────
	// On the first attempt (0) the order is already in PAYMENT_PROCESSING
	// (set by the order service). On subsequent attempts it is in
	// RETRYING_PAYMENT — move it back to PAYMENT_PROCESSING so the audit
	// log clearly shows each processing cycle.
	if evt.AttemptCount > 0 {
		if err := h.engine.Transition(ctx, workflow.TransitionInput{
			OrderID:   evt.OrderID,
			ToState:   workflow.StatePaymentProcessing,
			EventType: "RETRY_ATTEMPT",
			Service:   serviceName,
		}); err != nil {
			log.Warn().Err(err).Msg("RETRYING_PAYMENT→PAYMENT_PROCESSING transition error (non-fatal)")
		} else {
			log.Info().Int("attempt", evt.AttemptCount).Msg("🔁 retry attempt — back to PAYMENT_PROCESSING")
		}
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
	willRetry := retry.DefaultPolicy.ShouldRetry(evt.AttemptCount)

	log.Warn().
		Err(payErr).
		Int("attempt", evt.AttemptCount).
		Int("max_attempts", retry.DefaultPolicy.MaxAttempts).
		Bool("will_retry", willRetry).
		Msg("❌ payment failed")

	// Always publish PAYMENT_FAILED so notification service can react.
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

	// ── Transition: PAYMENT_PROCESSING → PAYMENT_FAILED ──────────────────
	if err := h.engine.Transition(ctx, workflow.TransitionInput{
		OrderID:   evt.OrderID,
		ToState:   workflow.StatePaymentFailed,
		EventType: events.EventPaymentFailed,
		Service:   serviceName,
	}); err != nil {
		log.Warn().Err(err).Msg("PAYMENT_FAILED transition error (non-fatal)")
	}

	if willRetry {
		// ── Transition: PAYMENT_FAILED → RETRYING_PAYMENT ────────────────
		// This state clearly communicates "failed, waiting for backoff"
		// distinct from a terminal failure.
		if err := h.engine.Transition(ctx, workflow.TransitionInput{
			OrderID:   evt.OrderID,
			ToState:   workflow.StateRetryingPayment,
			EventType: "RETRY_SCHEDULED",
			Service:   serviceName,
		}); err != nil {
			log.Warn().Err(err).Msg("RETRYING_PAYMENT transition error (non-fatal)")
		}

		delay := retry.DefaultPolicy.Delay(evt.AttemptCount)
		log.Info().
			Dur("retry_in", delay).
			Int("next_attempt", evt.AttemptCount+1).
			Msg("🔄 retry scheduled → RETRYING_PAYMENT")

		observability.RetryScheduled.WithLabelValues(
			serviceName, evt.EventType, fmt.Sprintf("%d", evt.AttemptCount+1),
		).Inc()
	} else {
		// ── Max retries exceeded → DLQ ────────────────────────────────────
		if err := h.engine.Transition(ctx, workflow.TransitionInput{
			OrderID:   evt.OrderID,
			ToState:   workflow.StateDLQ,
			EventType: "MAX_RETRIES_EXCEEDED",
			Service:   serviceName,
		}); err != nil {
			log.Warn().Err(err).Msg("DLQ transition error (non-fatal)")
		}
		log.Error().Msg("🪦 max retries exceeded — moving to DLQ")
		observability.DLQEvents.WithLabelValues(serviceName, evt.EventType).Inc()
	}

	// Persist retry or DLQ record.
	if err := h.scheduler.Schedule(ctx, evt.OrderID, evt.EventType,
		kafka.TopicOrderCreated, evt, evt.AttemptCount, payErr,
	); err != nil {
		log.Error().Err(err).Msg("scheduler error")
	}

	observability.EventsProcessed.WithLabelValues(serviceName, evt.EventType, "failed").Inc()
	return nil
}

// processPayment simulates a real payment gateway using the live failure rate.
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
