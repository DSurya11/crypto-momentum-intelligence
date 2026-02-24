SET TIME ZONE 'UTC';

CREATE TABLE IF NOT EXISTS tokens (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(100) NOT NULL UNIQUE,
    symbol VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ,
    chain VARCHAR(20) NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS swaps_raw (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(100) NOT NULL,
    tx_hash VARCHAR(100) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    buyer_address VARCHAR(100),
    seller_address VARCHAR(100),
    amount_token NUMERIC(30,10) NOT NULL,
    amount_usd NUMERIC(30,10),
    side VARCHAR(4) NOT NULL CHECK (side IN ('buy', 'sell')),
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_swaps_token_address
        FOREIGN KEY (token_address)
        REFERENCES tokens(token_address)
);

CREATE INDEX IF NOT EXISTS idx_swaps_raw_token_timestamp
    ON swaps_raw (token_address, timestamp);

CREATE TABLE IF NOT EXISTS liquidity_events_raw (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(100) NOT NULL,
    tx_hash VARCHAR(100) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(6) NOT NULL CHECK (event_type IN ('add', 'remove')),
    liquidity_usd NUMERIC(30,10),
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_liquidity_token_address
        FOREIGN KEY (token_address)
        REFERENCES tokens(token_address)
);

CREATE INDEX IF NOT EXISTS idx_liquidity_events_raw_token_timestamp
    ON liquidity_events_raw (token_address, timestamp);

CREATE TABLE IF NOT EXISTS price_ohlcv_raw (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    open NUMERIC(30,10) NOT NULL,
    high NUMERIC(30,10) NOT NULL,
    low NUMERIC(30,10) NOT NULL,
    close NUMERIC(30,10) NOT NULL,
    volume NUMERIC(30,10) NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_price_token_address
        FOREIGN KEY (token_address)
        REFERENCES tokens(token_address)
);

CREATE INDEX IF NOT EXISTS idx_price_ohlcv_raw_token_timestamp
    ON price_ohlcv_raw (token_address, timestamp);

CREATE TABLE IF NOT EXISTS social_raw (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(100) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    mention_count INTEGER NOT NULL,
    sentiment_score DOUBLE PRECISION,
    engagement_score DOUBLE PRECISION,
    source VARCHAR(50) NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_social_token_address
        FOREIGN KEY (token_address)
        REFERENCES tokens(token_address)
);

CREATE INDEX IF NOT EXISTS idx_social_raw_token_timestamp
    ON social_raw (token_address, timestamp);

CREATE OR REPLACE FUNCTION prevent_raw_table_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Raw tables are immutable. % is not allowed on %', TG_OP, TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_swaps_raw_immutable ON swaps_raw;
CREATE TRIGGER trg_swaps_raw_immutable
BEFORE UPDATE OR DELETE ON swaps_raw
FOR EACH ROW
EXECUTE FUNCTION prevent_raw_table_mutation();

DROP TRIGGER IF EXISTS trg_liquidity_events_raw_immutable ON liquidity_events_raw;
CREATE TRIGGER trg_liquidity_events_raw_immutable
BEFORE UPDATE OR DELETE ON liquidity_events_raw
FOR EACH ROW
EXECUTE FUNCTION prevent_raw_table_mutation();

DROP TRIGGER IF EXISTS trg_price_ohlcv_raw_immutable ON price_ohlcv_raw;
CREATE TRIGGER trg_price_ohlcv_raw_immutable
BEFORE UPDATE OR DELETE ON price_ohlcv_raw
FOR EACH ROW
EXECUTE FUNCTION prevent_raw_table_mutation();

DROP TRIGGER IF EXISTS trg_social_raw_immutable ON social_raw;
CREATE TRIGGER trg_social_raw_immutable
BEFORE UPDATE OR DELETE ON social_raw
FOR EACH ROW
EXECUTE FUNCTION prevent_raw_table_mutation();