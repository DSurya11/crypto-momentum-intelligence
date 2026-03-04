"""
Layer 2: Derive 5-minute OHLC price from swap data.

Price sources (in priority order):
  1. Swaps with amount_usd AND amount_token → price = usd / token  (Gecko)
  2. Future: pool-ratio inference for exotic pairs

Produces one row per (token_address, bucket_timestamp) in token_price_5m.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from getpass import getpass

import psycopg
from dotenv import load_dotenv


@dataclass
class PriceBuildStats:
    source_swaps: int
    upserted_price_rows: int


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


def build_token_price_5m(max_swaps: int) -> PriceBuildStats:
    """Derive 5-minute OHLC from recent swaps that carry USD info."""
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

            # Count eligible swaps (ones with USD pricing data).
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM (
                    SELECT id
                    FROM swaps_raw
                    WHERE amount_usd IS NOT NULL
                      AND amount_token IS NOT NULL
                      AND amount_token <> 0
                      AND amount_usd > 0
                    ORDER BY timestamp DESC
                    LIMIT %s
                ) priced
                """,
                (max_swaps,),
            )
            source_swaps = int(cursor.fetchone()[0])

            if source_swaps == 0:
                return PriceBuildStats(source_swaps=0, upserted_price_rows=0)

            # Derive OHLC per 5-minute bucket.
            # Open  = price of the earliest swap in the bucket
            # Close = price of the latest swap in the bucket
            # High  = max price in the bucket
            # Low   = min price in the bucket
            cursor.execute(
                """
                WITH priced_swaps AS (
                    SELECT
                        token_address,
                        timestamp,
                        id,
                        (amount_usd / amount_token)::DOUBLE PRECISION AS trade_price
                    FROM swaps_raw
                    WHERE amount_usd IS NOT NULL
                      AND amount_token IS NOT NULL
                      AND amount_token <> 0
                      AND amount_usd > 0
                    ORDER BY timestamp DESC
                    LIMIT %s
                ),
                bucketed AS (
                    SELECT
                        token_address,
                        to_timestamp(
                            FLOOR(EXTRACT(EPOCH FROM timestamp) / 300) * 300
                        )::TIMESTAMPTZ AS bucket_timestamp,
                        id,
                        trade_price
                    FROM priced_swaps
                ),
                ohlc AS (
                    SELECT
                        token_address,
                        bucket_timestamp,
                        -- OPEN: price of the first swap (lowest id in bucket)
                        (ARRAY_AGG(trade_price ORDER BY id ASC))[1]  AS open_price,
                        MAX(trade_price)                              AS high_price,
                        MIN(trade_price)                              AS low_price,
                        -- CLOSE: price of the last swap (highest id in bucket)
                        (ARRAY_AGG(trade_price ORDER BY id DESC))[1] AS close_price,
                        COUNT(*)::INTEGER                             AS sample_count
                    FROM bucketed
                    GROUP BY token_address, bucket_timestamp
                )
                INSERT INTO token_price_5m (
                    token_address,
                    bucket_timestamp,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    sample_count,
                    source
                )
                SELECT
                    token_address,
                    bucket_timestamp,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    sample_count,
                    'swap_ratio'
                FROM ohlc
                ON CONFLICT (token_address, bucket_timestamp)
                DO UPDATE SET
                    open_price   = EXCLUDED.open_price,
                    high_price   = EXCLUDED.high_price,
                    low_price    = EXCLUDED.low_price,
                    close_price  = EXCLUDED.close_price,
                    sample_count = EXCLUDED.sample_count,
                    source       = EXCLUDED.source,
                    updated_at   = NOW()
                """,
                (max_swaps,),
            )

            upserted_price_rows = int(cursor.rowcount)

    return PriceBuildStats(
        source_swaps=source_swaps,
        upserted_price_rows=upserted_price_rows,
    )


def main() -> None:
    load_dotenv()
    max_swaps = int(get_env("PRICE_MAX_SWAPS", "50000"))

    stats = build_token_price_5m(max_swaps=max_swaps)
    print(
        "token_price_5m build complete. "
        f"source_swaps={stats.source_swaps} upserted_price_rows={stats.upserted_price_rows}"
    )


if __name__ == "__main__":
    main()
