package workflow

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Engine executes validated state transitions inside a DB transaction.
// It acquires a row-level lock on the order before transitioning, preventing
// concurrent services from racing on the same order.
type Engine struct {
	db *sql.DB
}

func NewEngine(db *sql.DB) *Engine {
	return &Engine{db: db}
}

type TransitionInput struct {
	OrderID   string
	ToState   State
	EventType string // e.g. "PAYMENT_SUCCESS"
	Service   string // e.g. "payment-service"
	Metadata  map[string]any
}

// Transition atomically validates, applies, and audits a state change.
func (e *Engine) Transition(ctx context.Context, in TransitionInput) error {
	tx, err := e.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("workflow engine: begin tx: %w", err)
	}
	defer tx.Rollback()

	var currentState State
	err = tx.QueryRowContext(ctx,
		`SELECT status FROM orders WHERE id = $1 FOR UPDATE`,
		in.OrderID,
	).Scan(&currentState)
	if err == sql.ErrNoRows {
		return fmt.Errorf("workflow engine: order %s not found", in.OrderID)
	}
	if err != nil {
		return fmt.Errorf("workflow engine: get state: %w", err)
	}

	if !IsValidTransition(currentState, in.ToState) {
		return ErrInvalidTransition{From: currentState, To: in.ToState, OrderID: in.OrderID}
	}

	now := time.Now()

	_, err = tx.ExecContext(ctx,
		`UPDATE orders SET status = $1, updated_at = $2 WHERE id = $3`,
		in.ToState, now, in.OrderID,
	)
	if err != nil {
		return fmt.Errorf("workflow engine: update order: %w", err)
	}

	_, err = tx.ExecContext(ctx,
		`INSERT INTO workflow_states
		 (id, order_id, from_state, to_state, event_type, service, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		uuid.New().String(), in.OrderID,
		currentState, in.ToState,
		in.EventType, in.Service, now,
	)
	if err != nil {
		return fmt.Errorf("workflow engine: audit log: %w", err)
	}

	return tx.Commit()
}

// CurrentState returns the live state of an order (no lock, read-only).
func (e *Engine) CurrentState(ctx context.Context, orderID string) (State, error) {
	var s State
	err := e.db.QueryRowContext(ctx,
		`SELECT status FROM orders WHERE id = $1`, orderID,
	).Scan(&s)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("order %s not found", orderID)
	}
	return s, err
}
