package events

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Name      string  `json:"name"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type OrderCreatedEvent struct {
	BaseEvent
	CustomerID string      `json:"customer_id"`
	Amount     float64     `json:"amount"`
	Items      []OrderItem `json:"items"`
}
