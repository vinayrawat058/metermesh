CREATE TABLE IF NOT EXISTS customer_usage (
    customer_id  TEXT        NOT NULL,
    metric       TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    quantity     BIGINT      NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, metric, window_start)
);
