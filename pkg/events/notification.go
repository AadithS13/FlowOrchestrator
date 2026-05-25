package events

type NotificationEvent struct {
	BaseEvent
	Channel     string `json:"channel"`      // EMAIL | SMS
	RecipientID string `json:"recipient_id"`
	Template    string `json:"template"`
	Status      string `json:"status"`       // SENT | FAILED
}

const (
	NotificationChannelEmail = "EMAIL"
	NotificationChannelSMS   = "SMS"
)
