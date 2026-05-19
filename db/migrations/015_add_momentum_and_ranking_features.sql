-- Migration 015: Add missing momentum and ranking features to features_5m table
-- These columns are computed by ingestion/features_5m_builder.py but were not created by previous migrations

SET TIME ZONE 'UTC';

ALTER TABLE features_5m
ADD COLUMN IF NOT EXISTS rvol_5m DOUBLE PRECISION NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS momentum_15m DOUBLE PRECISION NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS momentum_30m DOUBLE PRECISION NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS momentum_accel DOUBLE PRECISION NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS buy_pressure DOUBLE PRECISION NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS rvol_rank_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS momentum_15m_rank_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS momentum_30m_rank_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS momentum_accel_rank_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS buy_pressure_rank_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS relative_momentum_rank_pct DOUBLE PRECISION NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS minutes_since_last_spike DOUBLE PRECISION NOT NULL DEFAULT 0;

-- Create indexes for commonly-accessed columns
CREATE INDEX IF NOT EXISTS idx_features_5m_rvol_5m ON features_5m (rvol_5m DESC);
CREATE INDEX IF NOT EXISTS idx_features_5m_momentum_15m ON features_5m (momentum_15m DESC);
CREATE INDEX IF NOT EXISTS idx_features_5m_buy_pressure ON features_5m (buy_pressure DESC);
