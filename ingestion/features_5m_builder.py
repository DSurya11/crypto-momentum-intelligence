from __future__ import annotations

import os
from dataclasses import dataclass
from getpass import getpass

import psycopg
from dotenv import load_dotenv


@dataclass
class FeatureBuildStats:
    source_metric_rows: int
    upserted_feature_rows: int


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_db_password() -> str:
    password = os.getenv("PGPASSWORD")
    if password:
        return password
    return getpass("PostgreSQL password for PGUSER: ")


def build_features_5m(max_metric_rows: int) -> FeatureBuildStats:
    conn = psycopg.connect(
        host=get_env("PGHOST"),
        port=int(get_env("PGPORT", "5432")),
        dbname=get_env("PGDATABASE"),
        user=get_env("PGUSER"),
        password=get_db_password(),
        sslmode=get_env("PGSSLMODE", "disable"),
    )

    with conn:
        with conn.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT id
                    FROM token_metrics_5m
                    ORDER BY bucket_timestamp DESC
                    LIMIT %s
                ) recent_metrics
                """,
                (max_metric_rows,),
            )
            source_metric_rows = int(cursor.fetchone()[0])

            if source_metric_rows == 0:
                return FeatureBuildStats(source_metric_rows=0, upserted_feature_rows=0)

            cursor.execute(
                """
                WITH selected_metrics AS (
                    SELECT
                        m.token_address,
                        m.bucket_timestamp,
                        m.total_volume,
                        m.buy_volume,
                        m.sell_volume,
                        m.trade_count,
                        m.unique_wallets,
                        p.close_price,
                        LOWER(COALESCE(t.chain, 'base')) AS chain
                    FROM token_metrics_5m m
                    LEFT JOIN token_price_5m p
                        ON m.token_address = p.token_address
                       AND m.bucket_timestamp = p.bucket_timestamp
                    LEFT JOIN tokens t ON m.token_address = t.token_address
                    ORDER BY m.bucket_timestamp DESC
                    LIMIT %s
                ),
                ordered AS (
                    SELECT
                        token_address,
                        bucket_timestamp,
                        total_volume,
                        buy_volume,
                        sell_volume,
                        trade_count,
                        unique_wallets,
                        close_price,
                        chain,
                        AVG(total_volume) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                        ) AS total_volume_avg_1h,
                        AVG(trade_count::DOUBLE PRECISION) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                        ) AS trade_count_avg_1h,
                        LAG(unique_wallets) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                        ) AS prev_unique_wallets,
                        LAG(close_price, 12) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                        ) AS close_price_1h_ago,
                        LAG(close_price) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                        ) AS prev_close_price
                    FROM selected_metrics
                ),
                computed_base AS (
                    SELECT
                        token_address,
                        bucket_timestamp,
                        buy_volume,
                        sell_volume,
                        chain,
                        CASE
                            WHEN total_volume_avg_1h IS NULL OR total_volume_avg_1h = 0 THEN 0::DOUBLE PRECISION
                            ELSE (total_volume::DOUBLE PRECISION / total_volume_avg_1h::DOUBLE PRECISION)
                        END AS volume_velocity,
                        COALESCE(
                            buy_volume::DOUBLE PRECISION / NULLIF(sell_volume::DOUBLE PRECISION, 0),
                            0::DOUBLE PRECISION
                        ) AS buy_sell_ratio,
                        CASE
                            WHEN trade_count_avg_1h IS NULL OR trade_count_avg_1h = 0 THEN 0::DOUBLE PRECISION
                            ELSE (trade_count::DOUBLE PRECISION / trade_count_avg_1h)
                        END AS trade_intensity,
                        (unique_wallets - COALESCE(prev_unique_wallets, unique_wallets))::INTEGER AS wallet_growth_delta,
                        CASE
                            WHEN close_price IS NULL OR close_price_1h_ago IS NULL OR close_price_1h_ago = 0 THEN 0::DOUBLE PRECISION
                            ELSE (close_price::DOUBLE PRECISION / close_price_1h_ago::DOUBLE PRECISION) - 1::DOUBLE PRECISION
                        END AS return_1h,
                        -- 5-minute absolute price change for spike detection
                        CASE
                            WHEN close_price IS NULL OR prev_close_price IS NULL OR prev_close_price = 0 THEN 0::DOUBLE PRECISION
                            ELSE ABS(close_price::DOUBLE PRECISION / prev_close_price::DOUBLE PRECISION - 1.0)
                        END AS price_change_5m_abs
                    FROM ordered
                ),
                computed_ranks AS (
                    SELECT
                        token_address,
                        bucket_timestamp,
                        chain,
                        volume_velocity,
                        buy_sell_ratio,
                        trade_intensity,
                        wallet_growth_delta,
                        return_1h,
                        price_change_5m_abs,
                        (volume_velocity - COALESCE(
                            LAG(volume_velocity) OVER (
                                PARTITION BY token_address
                                ORDER BY bucket_timestamp
                            ),
                            volume_velocity
                        ))::DOUBLE PRECISION AS volume_accel,
                        COALESCE(
                            PERCENT_RANK() OVER (
                                PARTITION BY bucket_timestamp
                                ORDER BY volume_velocity
                            ),
                            0::DOUBLE PRECISION
                        )::DOUBLE PRECISION AS volume_velocity_rank_pct,
                        COALESCE(
                            PERCENT_RANK() OVER (
                                PARTITION BY bucket_timestamp
                                ORDER BY buy_sell_ratio
                            ),
                            0::DOUBLE PRECISION
                        )::DOUBLE PRECISION AS buy_sell_ratio_rank_pct,
                        COALESCE(
                            PERCENT_RANK() OVER (
                                PARTITION BY bucket_timestamp
                                ORDER BY trade_intensity
                            ),
                            0::DOUBLE PRECISION
                        )::DOUBLE PRECISION AS trade_intensity_rank_pct,
                        -- order flow imbalance: (buy - sell) / (buy + sell)
                        CASE
                            WHEN (buy_volume + sell_volume) = 0 THEN 0::DOUBLE PRECISION
                            ELSE ((buy_volume - sell_volume)::DOUBLE PRECISION / (buy_volume + sell_volume)::DOUBLE PRECISION)
                        END AS order_flow_imbalance,
                        -- last bucket timestamp where price spiked >= 5% (for cooldown feature)
                        MAX(CASE WHEN price_change_5m_abs >= 0.05 THEN bucket_timestamp ELSE NULL END)
                            OVER (
                                PARTITION BY token_address
                                ORDER BY bucket_timestamp
                                ROWS UNBOUNDED PRECEDING
                            ) AS last_spike_ts
                    FROM computed_base
                ),
                -- market-wide regime: fraction of tokens with positive return_1h in the same bucket
                regime_stats AS (
                    SELECT
                        bucket_timestamp,
                        AVG(CASE WHEN return_1h > 0 THEN 1.0 ELSE 0.0 END)::DOUBLE PRECISION AS market_momentum_regime,
                        COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY volume_velocity), 0)::DOUBLE PRECISION AS median_volume_velocity
                    FROM computed_ranks
                    GROUP BY bucket_timestamp
                ),
                computed AS (
                    SELECT
                        r.token_address,
                        r.bucket_timestamp,
                        r.volume_velocity,
                        r.buy_sell_ratio,
                        r.trade_intensity,
                        r.wallet_growth_delta,
                        r.return_1h,
                        r.volume_accel,
                        r.volume_velocity_rank_pct,
                        r.buy_sell_ratio_rank_pct,
                        r.trade_intensity_rank_pct,
                        r.order_flow_imbalance,
                        COALESCE(s.market_momentum_regime, 0)::DOUBLE PRECISION AS market_momentum_regime,
                        -- time-of-day cyclical encoding
                        SIN(2 * PI() * EXTRACT(HOUR FROM r.bucket_timestamp) / 24.0)::DOUBLE PRECISION AS hour_sin,
                        COS(2 * PI() * EXTRACT(HOUR FROM r.bucket_timestamp) / 24.0)::DOUBLE PRECISION AS hour_cos,
                        -- volume relative to cross-sectional median
                        CASE
                            WHEN COALESCE(s.median_volume_velocity, 0) = 0 THEN 0::DOUBLE PRECISION
                            ELSE (r.volume_velocity / s.median_volume_velocity)::DOUBLE PRECISION
                        END AS volume_relative_to_median,
                        -- minutes since last >=5% price spike (-1 if no spike in data window)
                        COALESCE(
                            EXTRACT(EPOCH FROM (r.bucket_timestamp - r.last_spike_ts)) / 60.0,
                            -1.0
                        )::DOUBLE PRECISION AS minutes_since_last_spike,
                        -- chain one-hot (base is implicit baseline, excluded to avoid multicollinearity)
                        CASE WHEN r.chain = 'bsc'    THEN 1.0 ELSE 0.0 END::DOUBLE PRECISION AS is_bsc,
                        CASE WHEN r.chain = 'solana' THEN 1.0 ELSE 0.0 END::DOUBLE PRECISION AS is_solana,
                        CASE WHEN r.chain = 'eth'    THEN 1.0 ELSE 0.0 END::DOUBLE PRECISION AS is_eth
                    FROM computed_ranks r
                    LEFT JOIN regime_stats s ON r.bucket_timestamp = s.bucket_timestamp
                )
                INSERT INTO features_5m (
                    token_address,
                    bucket_timestamp,
                    volume_velocity,
                    buy_sell_ratio,
                    trade_intensity,
                    wallet_growth_delta,
                    return_1h,
                    volume_accel,
                    volume_velocity_rank_pct,
                    buy_sell_ratio_rank_pct,
                    trade_intensity_rank_pct,
                    market_momentum_regime,
                    hour_sin,
                    hour_cos,
                    volume_relative_to_median,
                    order_flow_imbalance,
                    minutes_since_last_spike,
                    is_bsc,
                    is_solana,
                    is_eth
                )
                SELECT
                    token_address,
                    bucket_timestamp,
                    volume_velocity,
                    buy_sell_ratio,
                    trade_intensity,
                    wallet_growth_delta,
                    return_1h,
                    volume_accel,
                    volume_velocity_rank_pct,
                    buy_sell_ratio_rank_pct,
                    trade_intensity_rank_pct,
                    market_momentum_regime,
                    hour_sin,
                    hour_cos,
                    volume_relative_to_median,
                    order_flow_imbalance,
                    minutes_since_last_spike,
                    is_bsc,
                    is_solana,
                    is_eth
                FROM computed
                ON CONFLICT (token_address, bucket_timestamp)
                DO UPDATE SET
                    volume_velocity = EXCLUDED.volume_velocity,
                    buy_sell_ratio = EXCLUDED.buy_sell_ratio,
                    trade_intensity = EXCLUDED.trade_intensity,
                    wallet_growth_delta = EXCLUDED.wallet_growth_delta,
                    return_1h = EXCLUDED.return_1h,
                    volume_accel = EXCLUDED.volume_accel,
                    volume_velocity_rank_pct = EXCLUDED.volume_velocity_rank_pct,
                    buy_sell_ratio_rank_pct = EXCLUDED.buy_sell_ratio_rank_pct,
                    trade_intensity_rank_pct = EXCLUDED.trade_intensity_rank_pct,
                    market_momentum_regime = EXCLUDED.market_momentum_regime,
                    hour_sin = EXCLUDED.hour_sin,
                    hour_cos = EXCLUDED.hour_cos,
                    volume_relative_to_median = EXCLUDED.volume_relative_to_median,
                    order_flow_imbalance = EXCLUDED.order_flow_imbalance,
                    minutes_since_last_spike = EXCLUDED.minutes_since_last_spike,
                    is_bsc = EXCLUDED.is_bsc,
                    is_solana = EXCLUDED.is_solana,
                    is_eth = EXCLUDED.is_eth,
                    updated_at = NOW()
                """,
                (max_metric_rows,),
            )

            upserted_feature_rows = int(cursor.rowcount)

    return FeatureBuildStats(
        source_metric_rows=source_metric_rows,
        upserted_feature_rows=upserted_feature_rows,
    )


def main() -> None:
    load_dotenv()
    max_metric_rows = int(get_env("FEATURES_MAX_METRIC_ROWS", "20000"))

    stats = build_features_5m(max_metric_rows=max_metric_rows)
    print(
        "features_5m build complete. "
        f"source_metric_rows={stats.source_metric_rows} upserted_feature_rows={stats.upserted_feature_rows}"
    )


if __name__ == "__main__":
    main()