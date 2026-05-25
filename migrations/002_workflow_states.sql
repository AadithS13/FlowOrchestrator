-- Append-only audit log of every state transition.
-- Never update rows; always insert a new one.
CREATE TABLE IF NOT EXISTS workflow_states (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID        NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    from_state  VARCHAR(50),                -- NULL on the first PENDING→PAYMENT_PROCESSING
    to_state    VARCHAR(50) NOT NULL,
    event_type  VARCHAR(100),               -- e.g. ORDER_CREATED, PAYMENT_SUCCESS
    service     VARCHAR(100),               -- which service wrote this row
    metadata    JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ws_order_id   ON workflow_states (order_id);
CREATE INDEX IF NOT EXISTS idx_ws_created_at ON workflow_states (created_at DESC);
