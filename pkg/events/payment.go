package events

type PaymentEvent struct {
	BaseEvent
	Status        string  `json:"status"`                    // SUCCESS | FAILED
	TransactionID string  `json:"transaction_id"`
	Amount        float64 `json:"amount"`
	FailureReason string  `json:"failure_reason,omitempty"`
}

const (
	PaymentStatusSuccess = "SUCCESS"
	PaymentStatusFailed  = "FAILED"
)
