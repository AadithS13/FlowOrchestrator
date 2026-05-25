package workflow

import "fmt"

type State string

const (
	StatePending              State = "PENDING"
	StatePaymentProcessing    State = "PAYMENT_PROCESSING"
	StatePaymentFailed        State = "PAYMENT_FAILED"
	StateRetryingPayment      State = "RETRYING_PAYMENT"   // scheduled for retry, backoff in progress
	StatePaymentSuccess       State = "PAYMENT_SUCCESS"
	StateInventoryProcessing  State = "INVENTORY_PROCESSING"
	StateInventoryReserved    State = "INVENTORY_RESERVED"
	StateInventoryFailed      State = "INVENTORY_FAILED"
	StateCompleted            State = "COMPLETED"
	StateFailed               State = "FAILED"
	StateDLQ                  State = "DLQ"
)

// validTransitions is the authoritative state machine definition.
//
// New retry flow with RETRYING_PAYMENT:
//
//	PAYMENT_PROCESSING → PAYMENT_FAILED        (payment attempt failed)
//	PAYMENT_FAILED     → RETRYING_PAYMENT      (retry scheduled, backoff timer running)
//	RETRYING_PAYMENT   → PAYMENT_PROCESSING    (backoff elapsed, event republished)
//	RETRYING_PAYMENT   → DLQ                   (max retries exhausted)
//
// This gives clean, inspectable semantics. An order in RETRYING_PAYMENT
// tells you exactly: "payment failed, we're waiting to try again."
// Previously PAYMENT_FAILED was overloaded for both "failed" and "waiting to retry."
var validTransitions = map[State][]State{
	StatePending:             {StatePaymentProcessing},
	StatePaymentProcessing:   {StatePaymentSuccess, StatePaymentFailed, StateDLQ},
	StatePaymentFailed:       {StateRetryingPayment, StateFailed},
	StateRetryingPayment:     {StatePaymentProcessing, StateDLQ},
	StatePaymentSuccess:      {StateInventoryProcessing},
	StateInventoryProcessing: {StateInventoryReserved, StateInventoryFailed, StateDLQ},
	StateInventoryFailed:     {StateInventoryProcessing, StateFailed},
	StateInventoryReserved:   {StateCompleted},
}

// IsValidTransition reports whether moving from → to is permitted.
// DLQ is always valid as an emergency escape hatch.
func IsValidTransition(from, to State) bool {
	if to == StateDLQ {
		return true
	}
	for _, s := range validTransitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

func (s State) String() string { return string(s) }

type ErrInvalidTransition struct {
	From    State
	To      State
	OrderID string
}

func (e ErrInvalidTransition) Error() string {
	return fmt.Sprintf("invalid transition %s → %s for order %s", e.From, e.To, e.OrderID)
}
