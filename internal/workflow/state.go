package workflow

import "fmt"

type State string

const (
	StatePending              State = "PENDING"
	StatePaymentProcessing    State = "PAYMENT_PROCESSING"
	StatePaymentSuccess       State = "PAYMENT_SUCCESS"
	StatePaymentFailed        State = "PAYMENT_FAILED"
	StateInventoryProcessing  State = "INVENTORY_PROCESSING"
	StateInventoryReserved    State = "INVENTORY_RESERVED"
	StateInventoryFailed      State = "INVENTORY_FAILED"
	StateCompleted            State = "COMPLETED"
	StateFailed               State = "FAILED"
	StateDLQ                  State = "DLQ"
)

// validTransitions is the state machine definition.
// Any transition not listed here is rejected by the engine, preventing
// stale retries from rolling back a completed workflow.
var validTransitions = map[State][]State{
	StatePending:             {StatePaymentProcessing},
	StatePaymentProcessing:   {StatePaymentSuccess, StatePaymentFailed, StateDLQ},
	StatePaymentSuccess:      {StateInventoryProcessing},
	StatePaymentFailed:       {StatePaymentProcessing, StateFailed}, // PAYMENT_PROCESSING allows retry
	StateInventoryProcessing: {StateInventoryReserved, StateInventoryFailed, StateDLQ},
	StateInventoryFailed:     {StateInventoryProcessing, StateFailed},
	StateInventoryReserved:   {StateCompleted},
}

func IsValidTransition(from, to State) bool {
	// DLQ is always a valid escape hatch regardless of current state.
	if to == StateDLQ {
		return true
	}
	allowed, ok := validTransitions[from]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == to {
			return true
		}
	}
	return false
}

func (s State) String() string { return string(s) }

// ErrInvalidTransition is returned when a requested transition is not in the state machine.
type ErrInvalidTransition struct {
	From State
	To   State
	OrderID string
}

func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("invalid transition %s → %s for order %s", e.From, e.To, e.OrderID)
}
