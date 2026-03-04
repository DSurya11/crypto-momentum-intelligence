SET TIME ZONE 'UTC';

-- Adaptive (percentile-based) label: 1 if the token's 2h return is in the top 20% of
-- all tokens in that timestamp window, 0 otherwise.
ALTER TABLE labels_5m
    ADD COLUMN IF NOT EXISTS target_adaptive_top20 SMALLINT DEFAULT NULL;

-- Also store the percentile rank of the future return within its timestamp cohort
ALTER TABLE labels_5m
    ADD COLUMN IF NOT EXISTS future_return_pctrank DOUBLE PRECISION DEFAULT NULL;

-- Check constraint for adaptive label
ALTER TABLE labels_5m
    ADD CONSTRAINT ck_labels_5m_adaptive_binary
        CHECK (target_adaptive_top20 IS NULL OR target_adaptive_top20 IN (0, 1));
