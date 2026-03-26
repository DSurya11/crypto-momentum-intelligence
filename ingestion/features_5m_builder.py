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
                            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                        ) AS total_volume_avg_30,
                        AVG(close_price::DOUBLE PRECISION) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
                        ) AS close_sma_12,
                        AVG(close_price::DOUBLE PRECISION) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                            ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
                        ) AS close_sma_26,
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
                        LAG(close_price, 3) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                        ) AS close_price_15m_ago,
                        LAG(close_price, 6) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                        ) AS close_price_30m_ago,
                        LAG(close_price) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                        ) AS prev_close_price,
                        -- Rolling average volume as a robust proxy for rolling median (unsupported as window func in PG)
                        AVG(total_volume) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                            ROWS BETWEEN 12 PRECEDING AND 1 PRECEDING
                        ) AS volume_avg_1h_rolling
                    FROM selected_metrics
                ),
                computed_base AS (
                    SELECT
                        token_address,
                        bucket_timestamp,
                        buy_volume,
                        sell_volume,
                        total_volume,
                        chain,
                        CASE
                            WHEN total_volume_avg_1h IS NULL OR total_volume_avg_1h = 0 THEN 0::DOUBLE PRECISION
                            ELSE (total_volume::DOUBLE PRECISION / total_volume_avg_1h::DOUBLE PRECISION)
                        END AS volume_velocity,
                        COALESCE(
                            buy_volume::DOUBLE PRECISION / NULLIF(sell_volume::DOUBLE PRECISION, 0),
                            0::DOUBLE PRECISION
                        ) AS buy_sell_ratio,
                        COALESCE(
                            buy_volume::DOUBLE PRECISION / NULLIF(buy_volume::DOUBLE PRECISION + sell_volume::DOUBLE PRECISION, 0),
                            0.5::DOUBLE PRECISION
                        ) AS buy_pressure,
                        CASE
                            WHEN volume_avg_1h_rolling IS NULL OR volume_avg_1h_rolling = 0 THEN 0::DOUBLE PRECISION
                            ELSE (total_volume::DOUBLE PRECISION / volume_avg_1h_rolling::DOUBLE PRECISION)
                        END AS rvol_5m,
                        CASE
                            WHEN trade_count_avg_1h IS NULL OR trade_count_avg_1h = 0 THEN 0::DOUBLE PRECISION
                            ELSE (trade_count::DOUBLE PRECISION / trade_count_avg_1h)
                        END AS trade_intensity,
                        (unique_wallets - COALESCE(prev_unique_wallets, unique_wallets))::INTEGER AS wallet_growth_delta,
                        CASE
                            WHEN close_price IS NULL OR close_price_1h_ago IS NULL OR close_price_1h_ago = 0 THEN 0::DOUBLE PRECISION
                            ELSE (close_price::DOUBLE PRECISION / close_price_1h_ago::DOUBLE PRECISION) - 1::DOUBLE PRECISION
                        END AS return_1h,
                        CASE
                            WHEN close_price IS NULL OR close_price_15m_ago IS NULL OR close_price_15m_ago = 0 THEN 0::DOUBLE PRECISION
                            ELSE (close_price::DOUBLE PRECISION / close_price_15m_ago::DOUBLE PRECISION) - 1::DOUBLE PRECISION
                        END AS momentum_15m,
                        CASE
                            WHEN close_price IS NULL OR close_price_30m_ago IS NULL OR close_price_30m_ago = 0 THEN 0::DOUBLE PRECISION
                            ELSE (close_price::DOUBLE PRECISION / close_price_30m_ago::DOUBLE PRECISION) - 1::DOUBLE PRECISION
                        END AS momentum_30m,
                        CASE
                            WHEN close_price IS NULL OR prev_close_price IS NULL OR prev_close_price = 0 THEN 0::DOUBLE PRECISION
                            ELSE (close_price::DOUBLE PRECISION / prev_close_price::DOUBLE PRECISION) - 1::DOUBLE PRECISION
                        END AS return_5m,
                        CASE
                            WHEN volume_avg_1h_rolling IS NULL OR volume_avg_1h_rolling = 0 THEN 0::DOUBLE PRECISION
                            ELSE (total_volume::DOUBLE PRECISION - volume_avg_1h_rolling::DOUBLE PRECISION) / volume_avg_1h_rolling::DOUBLE PRECISION
                        END AS volume_shock,
                        CASE
                            WHEN close_sma_12 IS NULL OR close_sma_26 IS NULL THEN 0::DOUBLE PRECISION
                            ELSE (close_sma_12 - close_sma_26)::DOUBLE PRECISION
                        END AS macd_proxy,
                        CASE
                            WHEN close_price IS NULL OR prev_close_price IS NULL THEN 0::DOUBLE PRECISION
                            ELSE GREATEST(close_price::DOUBLE PRECISION - prev_close_price::DOUBLE PRECISION, 0::DOUBLE PRECISION)
                        END AS gain_5m,
                        CASE
                            WHEN close_price IS NULL OR prev_close_price IS NULL THEN 0::DOUBLE PRECISION
                            ELSE GREATEST(prev_close_price::DOUBLE PRECISION - close_price::DOUBLE PRECISION, 0::DOUBLE PRECISION)
                        END AS loss_5m,
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
                        buy_pressure,
                        rvol_5m,
                        trade_intensity,
                        wallet_growth_delta,
                        return_1h,
                        momentum_15m,
                        momentum_30m,
                        (momentum_15m - momentum_30m)::DOUBLE PRECISION AS momentum_accel,
                        return_5m,
                        volume_shock,
                        macd_proxy,
                        gain_5m,
                        loss_5m,
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
                        COALESCE(
                            PERCENT_RANK() OVER (
                                PARTITION BY bucket_timestamp, chain
                                ORDER BY rvol_5m
                            ),
                            0::DOUBLE PRECISION
                        )::DOUBLE PRECISION AS rvol_rank_pct,
                        COALESCE(
                            PERCENT_RANK() OVER (
                                PARTITION BY bucket_timestamp, chain
                                ORDER BY momentum_15m
                            ),
                            0::DOUBLE PRECISION
                        )::DOUBLE PRECISION AS momentum_15m_rank_pct,
                        COALESCE(
                            PERCENT_RANK() OVER (
                                PARTITION BY bucket_timestamp, chain
                                ORDER BY momentum_30m
                            ),
                            0::DOUBLE PRECISION
                        )::DOUBLE PRECISION AS momentum_30m_rank_pct,
                        COALESCE(
                            PERCENT_RANK() OVER (
                                PARTITION BY bucket_timestamp, chain
                                ORDER BY (momentum_15m - momentum_30m)
                            ),
                            0::DOUBLE PRECISION
                        )::DOUBLE PRECISION AS momentum_accel_rank_pct,
                        COALESCE(
                            PERCENT_RANK() OVER (
                                PARTITION BY bucket_timestamp, chain
                                ORDER BY buy_pressure
                            ),
                            0::DOUBLE PRECISION
                        )::DOUBLE PRECISION AS buy_pressure_rank_pct,
                        COALESCE(
                            AVG(return_5m) OVER (PARTITION BY bucket_timestamp, chain),
                            0::DOUBLE PRECISION
                        )::DOUBLE PRECISION AS chain_avg_return_5m,
                        COALESCE(
                            AVG(momentum_30m) OVER (PARTITION BY chain, bucket_timestamp),
                            0::DOUBLE PRECISION
                        )::DOUBLE PRECISION AS chain_avg_momentum_30m,
                        AVG(gain_5m) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                        )::DOUBLE PRECISION AS avg_gain_14,
                        AVG(loss_5m) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                        )::DOUBLE PRECISION AS avg_loss_14,
                        -- last bucket timestamp where price spiked >= 5%% (for cooldown feature)
                        MAX(CASE WHEN price_change_5m_abs >= 0.05 THEN bucket_timestamp ELSE NULL END)
                            OVER (
                                PARTITION BY token_address
                                ORDER BY bucket_timestamp
                                ROWS UNBOUNDED PRECEDING
                            ) AS last_spike_ts
                    FROM computed_base
                ),
                -- per-chain regime: fraction of tokens with positive return_1h in the same bucket+chain
                regime_stats AS (
                    SELECT
                        bucket_timestamp,
                        chain,
                        AVG(CASE WHEN return_1h > 0 THEN 1.0 ELSE 0.0 END)::DOUBLE PRECISION AS market_momentum_regime,
                        COALESCE(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY volume_velocity), 0)::DOUBLE PRECISION AS median_volume_velocity
                    FROM computed_ranks
                    GROUP BY bucket_timestamp, chain
                ),
                computed AS (
                    SELECT
                        r.token_address,
                        r.bucket_timestamp,
                        r.volume_velocity,
                        r.buy_sell_ratio,
                        r.buy_pressure,
                        r.rvol_5m,
                        r.trade_intensity,
                        r.wallet_growth_delta,
                        r.return_1h,
                        r.momentum_15m,
                        r.momentum_30m,
                        r.momentum_accel,
                        r.volume_accel,
                        r.rvol_rank_pct,
                        r.momentum_15m_rank_pct,
                        r.momentum_30m_rank_pct,
                        r.momentum_accel_rank_pct,
                        r.buy_pressure_rank_pct,
                        (r.momentum_30m - COALESCE(r.chain_avg_momentum_30m, 0))::DOUBLE PRECISION AS relative_momentum,
                        COALESCE(
                            PERCENT_RANK() OVER (
                                PARTITION BY r.bucket_timestamp, r.chain
                                ORDER BY (r.momentum_30m - COALESCE(r.chain_avg_momentum_30m, 0))
                            ),
                            0::DOUBLE PRECISION
                        )::DOUBLE PRECISION AS relative_momentum_rank_pct,
                        (r.return_5m - (r.return_1h / 12.0))::DOUBLE PRECISION AS momentum_acceleration,
                        r.volume_shock,
                        r.macd_proxy,
                        CASE
                            WHEN COALESCE(r.avg_loss_14, 0) = 0 THEN
                                CASE WHEN COALESCE(r.avg_gain_14, 0) > 0 THEN 100::DOUBLE PRECISION ELSE 50::DOUBLE PRECISION END
                            ELSE (100.0 - (100.0 / (1.0 + (r.avg_gain_14 / NULLIF(r.avg_loss_14, 0)))))::DOUBLE PRECISION
                        END AS rsi_14,
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
                        -- minutes since last >=5%% price spike (-1 if no spike in data window)
                        COALESCE(
                            EXTRACT(EPOCH FROM (r.bucket_timestamp - r.last_spike_ts)) / 60.0,
                            -1.0
                        )::DOUBLE PRECISION AS minutes_since_last_spike,
                        LN(1.0 + GREATEST(EXTRACT(EPOCH FROM (r.bucket_timestamp - t.created_at)) / 3600.0, 0))::DOUBLE PRECISION AS time_since_launch_log
                    FROM computed_ranks r
                    LEFT JOIN regime_stats s ON r.bucket_timestamp = s.bucket_timestamp AND r.chain = s.chain
                    LEFT JOIN tokens t ON r.token_address = t.token_address
                )
                INSERT INTO features_5m (
                    token_address,
                    bucket_timestamp,
                    volume_velocity,
                    buy_sell_ratio,
                    buy_pressure,
                    rvol_5m,
                    trade_intensity,
                    wallet_growth_delta,
                    return_1h,
                    momentum_15m,
                    momentum_30m,
                    momentum_accel,
                    volume_accel,
                    relative_momentum,
                    momentum_acceleration,
                    rvol_rank_pct,
                    momentum_15m_rank_pct,
                    momentum_30m_rank_pct,
                    momentum_accel_rank_pct,
                    buy_pressure_rank_pct,
                    relative_momentum_rank_pct,
                    volume_shock,
                    macd_proxy,
                    rsi_14,
                    volume_velocity_rank_pct,
                    buy_sell_ratio_rank_pct,
                    trade_intensity_rank_pct,
                    market_momentum_regime,
                    hour_sin,
                    hour_cos,
                    volume_relative_to_median,
                    order_flow_imbalance,
                    minutes_since_last_spike,
                    time_since_launch_log
                )
                SELECT
                    token_address,
                    bucket_timestamp,
                    volume_velocity,
                    buy_sell_ratio,
                    buy_pressure,
                    rvol_5m,
                    trade_intensity,
                    wallet_growth_delta,
                    return_1h,
                    momentum_15m,
                    momentum_30m,
                    momentum_accel,
                    volume_accel,
                    relative_momentum,
                    momentum_acceleration,
                    rvol_rank_pct,
                    momentum_15m_rank_pct,
                    momentum_30m_rank_pct,
                    momentum_accel_rank_pct,
                    buy_pressure_rank_pct,
                    relative_momentum_rank_pct,
                    volume_shock,
                    macd_proxy,
                    rsi_14,
                    volume_velocity_rank_pct,
                    buy_sell_ratio_rank_pct,
                    trade_intensity_rank_pct,
                    market_momentum_regime,
                    hour_sin,
                    hour_cos,
                    volume_relative_to_median,
                    order_flow_imbalance,
                    minutes_since_last_spike,
                    time_since_launch_log
                FROM computed
                ON CONFLICT (token_address, bucket_timestamp)
                DO UPDATE SET
                    volume_velocity = EXCLUDED.volume_velocity,
                    buy_sell_ratio = EXCLUDED.buy_sell_ratio,
                    buy_pressure = EXCLUDED.buy_pressure,
                    rvol_5m = EXCLUDED.rvol_5m,
                    trade_intensity = EXCLUDED.trade_intensity,
                    wallet_growth_delta = EXCLUDED.wallet_growth_delta,
                    return_1h = EXCLUDED.return_1h,
                    momentum_15m = EXCLUDED.momentum_15m,
                    momentum_30m = EXCLUDED.momentum_30m,
                    momentum_accel = EXCLUDED.momentum_accel,
                    volume_accel = EXCLUDED.volume_accel,
                    relative_momentum = EXCLUDED.relative_momentum,
                    momentum_acceleration = EXCLUDED.momentum_acceleration,
                    rvol_rank_pct = EXCLUDED.rvol_rank_pct,
                    momentum_15m_rank_pct = EXCLUDED.momentum_15m_rank_pct,
                    momentum_30m_rank_pct = EXCLUDED.momentum_30m_rank_pct,
                    momentum_accel_rank_pct = EXCLUDED.momentum_accel_rank_pct,
                    buy_pressure_rank_pct = EXCLUDED.buy_pressure_rank_pct,
                    relative_momentum_rank_pct = EXCLUDED.relative_momentum_rank_pct,
                    volume_shock = EXCLUDED.volume_shock,
                    macd_proxy = EXCLUDED.macd_proxy,
                    rsi_14 = EXCLUDED.rsi_14,
                    volume_velocity_rank_pct = EXCLUDED.volume_velocity_rank_pct,
                    buy_sell_ratio_rank_pct = EXCLUDED.buy_sell_ratio_rank_pct,
                    trade_intensity_rank_pct = EXCLUDED.trade_intensity_rank_pct,
                    market_momentum_regime = EXCLUDED.market_momentum_regime,
                    hour_sin = EXCLUDED.hour_sin,
                    hour_cos = EXCLUDED.hour_cos,
                    volume_relative_to_median = EXCLUDED.volume_relative_to_median,
                    order_flow_imbalance = EXCLUDED.order_flow_imbalance,
                    minutes_since_last_spike = EXCLUDED.minutes_since_last_spike,
                    time_since_launch_log = EXCLUDED.time_since_launch_log,
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