SET TIME ZONE 'UTC';

CREATE TABLE IF NOT EXISTS token_price_5m (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(100) NOT NULL,
    bucket_timestamp TIMESTAMPTZ NOT NULL,
    open_price DOUBLE PRECISION,
    high_price DOUBLE PRECISION,
    low_price DOUBLE PRECISION,
    close_price DOUBLE PRECISION,
    sample_count INTEGER NOT NULL DEFAULT 0,
    source VARCHAR(20) NOT NULL DEFAULT 'swap_ratio',
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_token_price_5m_token_address
        FOREIGN KEY (token_address)
        REFERENCES tokens(token_address),
    CONSTRAINT uq_token_price_5m_token_bucket
        UNIQUE (token_address, bucket_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_token_price_5m_bucket_timestamp
    ON token_price_5m (bucket_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_token_price_5m_token_bucket
    ON token_price_5m (token_address, bucket_timestamp DESC);
