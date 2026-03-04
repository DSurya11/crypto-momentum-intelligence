from __future__ import annotations

import os
from dataclasses import dataclass
from getpass import getpass

import psycopg
from dotenv import load_dotenv


@dataclass
class AggregationStats:
    source_swaps: int
    upserted_buckets: int


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


def build_token_metrics_5m(max_swaps: int) -> AggregationStats:
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
                    FROM swaps_raw
                    ORDER BY timestamp DESC
                    LIMIT %s
                ) recent
                """,
                (max_swaps,),
            )
            source_swaps = int(cursor.fetchone()[0])

            if source_swaps == 0:
                return AggregationStats(source_swaps=0, upserted_buckets=0)

            cursor.execute(
                """
                WITH selected_swaps AS (
                    SELECT
                        token_address,
                        timestamp,
                        side,
                        COALESCE(amount_token, 0)::NUMERIC(30,10) AS volume_token,
                        buyer_address,
                        seller_address
                    FROM swaps_raw
                    ORDER BY timestamp DESC
                    LIMIT %s
                ),
                bucketed AS (
                    SELECT
                        token_address,
                        to_timestamp(FLOOR(EXTRACT(EPOCH FROM timestamp) / 300) * 300)::TIMESTAMPTZ AS bucket_timestamp,
                        side,
                        volume_token,
                        buyer_address,
                        seller_address
                    FROM selected_swaps
                ),
                token_agg AS (
                    SELECT
                        token_address,
                        bucket_timestamp,
                        SUM(volume_token)::NUMERIC(30,10) AS total_volume_token,
                        SUM(CASE WHEN side = 'buy' THEN volume_token ELSE 0 END)::NUMERIC(30,10) AS buy_volume_token,
                        SUM(CASE WHEN side = 'sell' THEN volume_token ELSE 0 END)::NUMERIC(30,10) AS sell_volume_token,
                        COUNT(*)::INTEGER AS trade_count
                    FROM bucketed
                    GROUP BY token_address, bucket_timestamp
                ),
                wallet_agg AS (
                    SELECT
                        token_address,
                        bucket_timestamp,
                        COUNT(DISTINCT wallet)::INTEGER AS unique_wallets
                    FROM (
                        SELECT token_address, bucket_timestamp, buyer_address AS wallet
                        FROM bucketed
                        WHERE buyer_address IS NOT NULL

                        UNION ALL

                        SELECT token_address, bucket_timestamp, seller_address AS wallet
                        FROM bucketed
                        WHERE seller_address IS NOT NULL
                    ) wallets
                    GROUP BY token_address, bucket_timestamp
                ),
                priced AS (
                    SELECT
                        t.token_address,
                        t.bucket_timestamp,
                        -- USD volume = token_volume * close_price from price table
                        -- Falls back to 0 when no price available (e.g. Alchemy-only tokens)
                        (t.total_volume_token * COALESCE(p.close_price, 0))::NUMERIC(30,10) AS total_volume,
                        (t.buy_volume_token  * COALESCE(p.close_price, 0))::NUMERIC(30,10) AS buy_volume,
                        (t.sell_volume_token * COALESCE(p.close_price, 0))::NUMERIC(30,10) AS sell_volume,
                        t.trade_count,
                        COALESCE(w.unique_wallets, 0)::INTEGER AS unique_wallets
                    FROM token_agg t
                    LEFT JOIN token_price_5m p
                        ON t.token_address = p.token_address
                       AND t.bucket_timestamp = p.bucket_timestamp
                    LEFT JOIN wallet_agg w
                        ON t.token_address = w.token_address
                       AND t.bucket_timestamp = w.bucket_timestamp
                )
                INSERT INTO token_metrics_5m (
                    token_address,
                    bucket_timestamp,
                    total_volume,
                    buy_volume,
                    sell_volume,
                    trade_count,
                    unique_wallets
                )
                SELECT
                    token_address,
                    bucket_timestamp,
                    total_volume,
                    buy_volume,
                    sell_volume,
                    trade_count,
                    unique_wallets
                FROM priced
                ON CONFLICT (token_address, bucket_timestamp)
                DO UPDATE SET
                    total_volume = EXCLUDED.total_volume,
                    buy_volume = EXCLUDED.buy_volume,
                    sell_volume = EXCLUDED.sell_volume,
                    trade_count = EXCLUDED.trade_count,
                    unique_wallets = EXCLUDED.unique_wallets,
                    updated_at = NOW()
                """,
                (max_swaps,),
            )

            upserted_buckets = int(cursor.rowcount)

    return AggregationStats(source_swaps=source_swaps, upserted_buckets=upserted_buckets)


def main() -> None:
    load_dotenv()
    max_swaps = int(get_env("METRICS_MAX_SWAPS", "5000"))

    stats = build_token_metrics_5m(max_swaps=max_swaps)
    print(
        "token_metrics_5m aggregation complete. "
        f"source_swaps={stats.source_swaps} upserted_buckets={stats.upserted_buckets}"
    )


if __name__ == "__main__":
    main()