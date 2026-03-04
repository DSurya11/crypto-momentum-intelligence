SET TIME ZONE 'UTC';

CREATE TABLE IF NOT EXISTS tracked_pools (
    pool_address VARCHAR(100) PRIMARY KEY,
    token_address VARCHAR(100) NOT NULL,
    dex VARCHAR(50),
    source VARCHAR(50) NOT NULL,
    chain VARCHAR(20) NOT NULL,
    discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_tracked_pools_token_address
        FOREIGN KEY (token_address)
        REFERENCES tokens(token_address)
);

CREATE INDEX IF NOT EXISTS idx_tracked_pools_chain
    ON tracked_pools (chain);

CREATE INDEX IF NOT EXISTS idx_tracked_pools_token_address
    ON tracked_pools (token_address);
