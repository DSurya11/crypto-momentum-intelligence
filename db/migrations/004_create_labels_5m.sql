SET TIME ZONE 'UTC';

CREATE TABLE IF NOT EXISTS labels_5m (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(100) NOT NULL,
    bucket_timestamp TIMESTAMPTZ NOT NULL,
    future_return_2h DOUBLE PRECISION,
    target_up_5pct_2h SMALLINT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_labels_5m_token_address
        FOREIGN KEY (token_address)
        REFERENCES tokens(token_address),
    CONSTRAINT uq_labels_5m_token_bucket
        UNIQUE (token_address, bucket_timestamp),
    CONSTRAINT ck_labels_5m_target_binary
        CHECK (target_up_5pct_2h IN (0, 1))
);

CREATE INDEX IF NOT EXISTS idx_labels_5m_bucket_timestamp
    ON labels_5m (bucket_timestamp DESC);