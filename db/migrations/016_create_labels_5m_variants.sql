-- Migration 016: Create labels_5m_variants table for alternative label targets
-- Used by ingestion/labels_variant_builder.py for experimenting with different label horizons and thresholds

SET TIME ZONE 'UTC';

CREATE TABLE IF NOT EXISTS labels_5m_variants (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(100) NOT NULL,
    bucket_timestamp TIMESTAMPTZ NOT NULL,
    chain VARCHAR(50) NOT NULL DEFAULT 'ethereum',
    target_name VARCHAR(50) NOT NULL,
    horizon_buckets INTEGER NOT NULL,
    threshold DOUBLE PRECISION NOT NULL,
    future_return DOUBLE PRECISION,
    target_binary SMALLINT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT fk_labels_5m_variants_token
        FOREIGN KEY (token_address) REFERENCES tokens(token_address) ON DELETE CASCADE,
    
    CONSTRAINT uq_labels_5m_variants_key
        UNIQUE (token_address, bucket_timestamp, target_name, horizon_buckets, threshold)
);

-- Indexes for query performance
CREATE INDEX IF NOT EXISTS idx_labels_5m_variants_bucket ON labels_5m_variants (bucket_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_labels_5m_variants_token_bucket ON labels_5m_variants (token_address, bucket_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_labels_5m_variants_target ON labels_5m_variants (target_name, bucket_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_labels_5m_variants_chain ON labels_5m_variants (chain, bucket_timestamp DESC);

-- Immutability trigger: prevent updates after 24 hours
CREATE OR REPLACE FUNCTION prevent_labels_5m_variants_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM labels_5m_variants
        WHERE id = OLD.id
        AND inserted_at < NOW() - INTERVAL '24 hours'
    ) THEN
        RAISE EXCEPTION 'Cannot update/delete immutable variant labels older than 24 hours';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_labels_5m_variants_immutability ON labels_5m_variants;
CREATE TRIGGER trg_labels_5m_variants_immutability
BEFORE UPDATE OR DELETE ON labels_5m_variants
FOR EACH ROW
EXECUTE FUNCTION prevent_labels_5m_variants_mutation();
