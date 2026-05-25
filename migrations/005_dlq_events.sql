-- Permanent graveyard for events that exhausted all retries.
-- Rows here require manual human intervention.
CREATE TABLE IF NOT EXISTS dlq_events (
    id             UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id       UUID,
    event_type     VARCHAR(100) NOT NULL,
    original_topic VARCHAR(255) NOT NULL,
    payload        JSONB        NOT NULL,
    error_message  TEXT,
    attempt_count  INT          NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dlq_order_id  ON dlq_events (order_id);
CREATE INDEX IF NOT EXISTS idx_dlq_created   ON dlq_events (created_at DESC);
