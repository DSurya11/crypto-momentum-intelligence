SET TIME ZONE 'UTC';

CREATE TABLE IF NOT EXISTS features_5m (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(100) NOT NULL,
    bucket_timestamp TIMESTAMPTZ NOT NULL,
    volume_velocity DOUBLE PRECISION NOT NULL,
    buy_sell_ratio DOUBLE PRECISION NOT NULL,
    trade_intensity DOUBLE PRECISION NOT NULL,
    wallet_growth_delta INTEGER NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_features_5m_token_address
        FOREIGN KEY (token_address)
        REFERENCES tokens(token_address),
    CONSTRAINT uq_features_5m_token_bucket
        UNIQUE (token_address, bucket_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_features_5m_bucket_timestamp
    ON features_5m (bucket_timestamp DESC);