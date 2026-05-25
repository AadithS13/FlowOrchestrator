package retry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/FlowOrchestrator/floworch/internal/kafka"
	"github.com/google/uuid"
)

// Scheduler persists failed events and republishes them after the backoff window.
// One Scheduler instance runs per service process.
type Scheduler struct {
	db       *sql.DB
	producer *kafka.Producer
	policy   Policy
	service  string
}

func NewScheduler(db *sql.DB, producer *kafka.Producer, service string, policy Policy) *Scheduler {
	return &Scheduler{db: db, producer: producer, policy: policy, service: service}
}

// Schedule persists a failed event for future retry or routes it straight to the DLQ
// if max attempts are already exhausted.
func (s *Scheduler) Schedule(ctx context.Context, orderID, eventType, topic string, payload any, attemptCount int, lastErr error) error {
	if !s.policy.ShouldRetry(attemptCount) {
		errMsg := "max retries exceeded"
		if lastErr != nil {
			errMsg = lastErr.Error()
		}
		return s.moveToDLQ(ctx, orderID, eventType, topic, payload, attemptCount, errMsg)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("scheduler: marshal payload: %w", err)
	}

	nextRetry := s.policy.NextRetryAt(attemptCount)

	_, err = s.db.ExecContext(ctx,
		`INSERT INTO retry_events
		 (id, order_id, event_type, topic, payload, attempt_count, max_attempts, next_retry_at, last_error, status, created_at, updated_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'PENDING',NOW(),NOW())`,
		uuid.New().String(), orderID, eventType, topic, data,
		attemptCount, s.policy.MaxAttempts, nextRetry,
		errorString(lastErr),
	)
	return err
}

// Run starts a background polling loop that republishes due retry events.
// Call in a goroutine; returns when ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.processDue(ctx); err != nil {
				log.Printf("[retry-scheduler:%s] poll error: %v", s.service, err)
			}
		}
	}
}

func (s *Scheduler) processDue(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, order_id, event_type, topic, payload, attempt_count
		 FROM   retry_events
		 WHERE  status = 'PENDING' AND next_retry_at <= NOW()
		 FOR UPDATE SKIP LOCKED
		 LIMIT 20`,
	)
	if err != nil {
		return fmt.Errorf("query due retries: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id, orderID, eventType, topic string
		var rawPayload []byte
		var attemptCount int

		if err := rows.Scan(&id, &orderID, &eventType, &topic, &rawPayload, &attemptCount); err != nil {
			return err
		}

		var event map[string]any
		if err := json.Unmarshal(rawPayload, &event); err != nil {
			log.Printf("[retry-scheduler] unmarshal error for id=%s: %v", id, err)
			continue
		}
		event["attempt_count"] = attemptCount + 1

		if err := s.producer.Publish(ctx, topic, orderID, event); err != nil {
			log.Printf("[retry-scheduler] republish error for id=%s: %v", id, err)
			continue
		}

		if _, err := s.db.ExecContext(ctx,
			`UPDATE retry_events SET status='PROCESSED', updated_at=NOW() WHERE id=$1`, id,
		); err != nil {
			log.Printf("[retry-scheduler] mark-processed error for id=%s: %v", id, err)
		}

		log.Printf("[retry-scheduler:%s] republished %s for order %s (attempt %d)",
			s.service, eventType, orderID, attemptCount+1)
	}

	return rows.Err()
}

func (s *Scheduler) moveToDLQ(ctx context.Context, orderID, eventType, topic string, payload any, attemptCount int, errMsg string) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("dlq: marshal payload: %w", err)
	}

	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO dlq_events (id, order_id, event_type, original_topic, payload, error_message, attempt_count, created_at)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())`,
		uuid.New().String(), orderID, eventType, topic, data, errMsg, attemptCount,
	); err != nil {
		return fmt.Errorf("dlq: insert: %w", err)
	}

	// Publish to DLQ Kafka topic so alert consumers / dashboards can react.
	if err := s.producer.Publish(ctx, kafka.TopicDLQEvents, orderID, payload); err != nil {
		log.Printf("[dlq] kafka publish error: %v", err)
	}

	log.Printf("[dlq:%s] event %s for order %s moved to DLQ after %d attempts",
		s.service, eventType, orderID, attemptCount)
	return nil
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
