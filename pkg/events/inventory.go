package events

type InventoryEvent struct {
	BaseEvent
	Status        string      `json:"status"`          // RESERVED | FAILED
	ReservationID string      `json:"reservation_id"`
	Items         []OrderItem `json:"items"`
}

const (
	InventoryStatusReserved = "RESERVED"
	InventoryStatusFailed   = "FAILED"
)
