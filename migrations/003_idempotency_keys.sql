-- Prevents double-processing of the same event by any service.
-- Key format: "{service}:{event_id}"  e.g. "payment-service:uuid-..."
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key          VARCHAR(512) PRIMARY KEY,
    order_id     UUID         NOT NULL,
    event_type   VARCHAR(100) NOT NULL,
    processed_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- TTL cleanup: rows older than 7 days can be purged safely.
CREATE INDEX IF NOT EXISTS idx_idem_processed_at ON idempotency_keys (processed_at);
