SET TIME ZONE 'UTC';

CREATE TABLE IF NOT EXISTS token_metrics_5m (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(100) NOT NULL,
    bucket_timestamp TIMESTAMPTZ NOT NULL,
    total_volume NUMERIC(30,10) NOT NULL,
    buy_volume NUMERIC(30,10) NOT NULL,
    sell_volume NUMERIC(30,10) NOT NULL,
    trade_count INTEGER NOT NULL,
    unique_wallets INTEGER NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_token_metrics_5m_token_address
        FOREIGN KEY (token_address)
        REFERENCES tokens(token_address),
    CONSTRAINT uq_token_metrics_5m_token_bucket
        UNIQUE (token_address, bucket_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_token_metrics_5m_bucket_timestamp
    ON token_metrics_5m (bucket_timestamp DESC);