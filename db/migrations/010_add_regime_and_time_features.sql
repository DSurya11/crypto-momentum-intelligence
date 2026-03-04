SET TIME ZONE 'UTC';

-- Regime feature: rolling market-wide positive-momentum rate over last 2h (24 buckets)
ALTER TABLE features_5m
    ADD COLUMN IF NOT EXISTS market_momentum_regime DOUBLE PRECISION NOT NULL DEFAULT 0;

-- Time-of-day cyclical features (sin/cos encoding of hour)
ALTER TABLE features_5m
    ADD COLUMN IF NOT EXISTS hour_sin DOUBLE PRECISION NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hour_cos DOUBLE PRECISION NOT NULL DEFAULT 0;

-- Liquidity proxy: total volume relative to cross-sectional median
ALTER TABLE features_5m
    ADD COLUMN IF NOT EXISTS volume_relative_to_median DOUBLE PRECISION NOT NULL DEFAULT 0;

-- Order flow imbalance: size-weighted directional pressure
ALTER TABLE features_5m
    ADD COLUMN IF NOT EXISTS order_flow_imbalance DOUBLE PRECISION NOT NULL DEFAULT 0;
