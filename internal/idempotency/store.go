package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// ErrDuplicate is returned when the event was already successfully processed.
// Callers must ACK the Kafka message and skip processing — not treat this as a failure.
var ErrDuplicate = errors.New("idempotency: duplicate event, already processed")

// Store checks and marks idempotency keys in PostgreSQL.
// Key format convention: "{service-name}:{event_id}"
// e.g.  "payment-service:550e8400-e29b-41d4-a716-446655440000"
type Store struct {
	db *sql.DB
}

func NewStore(db *sql.DB) *Store {
	return &Store{db: db}
}

// Check returns ErrDuplicate if the key was already marked, nil if it's fresh.
func (s *Store) Check(ctx context.Context, key string) error {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM idempotency_keys WHERE key = $1`, key,
	).Scan(&count)
	if err != nil {
		return fmt.Errorf("idempotency check: %w", err)
	}
	if count > 0 {
		return ErrDuplicate
	}
	return nil
}

// Mark records key as processed. Call this only after the business logic succeeds.
// Uses ON CONFLICT DO NOTHING so concurrent retries are safe.
func (s *Store) Mark(ctx context.Context, key, orderID, eventType string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO idempotency_keys (key, order_id, event_type, processed_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (key) DO NOTHING`,
		key, orderID, eventType, time.Now(),
	)
	if err != nil {
		return fmt.Errorf("idempotency mark: %w", err)
	}
	return nil
}

// CheckAndMark is the atomic version: check + mark in one call using
// INSERT ... ON CONFLICT to detect duplicates without a separate SELECT.
// Returns ErrDuplicate if the key was already present.
func (s *Store) CheckAndMark(ctx context.Context, key, orderID, eventType string) error {
	result, err := s.db.ExecContext(ctx,
		`INSERT INTO idempotency_keys (key, order_id, event_type, processed_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (key) DO NOTHING`,
		key, orderID, eventType, time.Now(),
	)
	if err != nil {
		return fmt.Errorf("idempotency check-and-mark: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrDuplicate
	}
	return nil
}
