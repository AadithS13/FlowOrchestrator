CREATE TABLE IF NOT EXISTS retry_events (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id      UUID        NOT NULL,
    event_type    VARCHAR(100) NOT NULL,
    topic         VARCHAR(255) NOT NULL,        -- original Kafka topic to republish to
    payload       JSONB        NOT NULL,
    attempt_count INT          NOT NULL DEFAULT 0,
    max_attempts  INT          NOT NULL DEFAULT 3,
    next_retry_at TIMESTAMPTZ  NOT NULL,
    last_error    TEXT,
    status        VARCHAR(50)  NOT NULL DEFAULT 'PENDING', -- PENDING | PROCESSED | EXHAUSTED
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_retry_due    ON retry_events (next_retry_at) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_retry_order  ON retry_events (order_id);
