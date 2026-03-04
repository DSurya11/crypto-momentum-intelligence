from __future__ import annotations

import os
from dataclasses import dataclass
from getpass import getpass

import psycopg
from dotenv import load_dotenv


@dataclass
class LabelBuildStats:
    source_buckets: int
    upserted_labels: int


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


def build_labels_5m(horizon_buckets: int, target_threshold: float) -> LabelBuildStats:
    """Derive labels from token_price_5m (Layer 2) — no swaps_raw dependency."""
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

            # Count source price buckets
            cursor.execute("SELECT COUNT(*) FROM token_price_5m")
            source_buckets = int(cursor.fetchone()[0])

            if source_buckets == 0:
                return LabelBuildStats(source_buckets=0, upserted_labels=0)

            # Clean stale labels with no return yet (will be recomputed)
            cursor.execute(
                """
                DELETE FROM labels_5m
                WHERE future_return_2h IS NULL
                """
            )

            # Compute labels from token_price_5m.close_price
            cursor.execute(
                """
                WITH labeled AS (
                    SELECT
                        token_address,
                        bucket_timestamp,
                        close_price,
                        LEAD(close_price, %s) OVER (
                            PARTITION BY token_address
                            ORDER BY bucket_timestamp
                        ) AS future_close_price
                    FROM token_price_5m
                    WHERE close_price IS NOT NULL
                      AND close_price > 0
                ),
                finalized AS (
                    SELECT
                        token_address,
                        bucket_timestamp,
                        ((future_close_price - close_price) / close_price) AS future_return_2h,
                        CASE
                            WHEN ((future_close_price - close_price) / close_price) >= %s THEN 1
                            ELSE 0
                        END::SMALLINT AS target_up_5pct_2h,
                        -- Percentile rank of return within each timestamp cohort
                        PERCENT_RANK() OVER (
                            PARTITION BY bucket_timestamp
                            ORDER BY ((future_close_price - close_price) / close_price)
                        )::DOUBLE PRECISION AS future_return_pctrank
                    FROM labeled
                    WHERE future_close_price IS NOT NULL
                      AND close_price <> 0
                ),
                with_adaptive AS (
                    SELECT
                        token_address,
                        bucket_timestamp,
                        future_return_2h,
                        target_up_5pct_2h,
                        future_return_pctrank,
                        CASE WHEN future_return_pctrank >= 0.80 THEN 1 ELSE 0 END::SMALLINT AS target_adaptive_top20
                    FROM finalized
                )
                INSERT INTO labels_5m (
                    token_address,
                    bucket_timestamp,
                    future_return_2h,
                    target_up_5pct_2h,
                    future_return_pctrank,
                    target_adaptive_top20
                )
                SELECT
                    token_address,
                    bucket_timestamp,
                    future_return_2h,
                    target_up_5pct_2h,
                    future_return_pctrank,
                    target_adaptive_top20
                FROM with_adaptive
                ON CONFLICT (token_address, bucket_timestamp)
                DO UPDATE SET
                    future_return_2h = EXCLUDED.future_return_2h,
                    target_up_5pct_2h = EXCLUDED.target_up_5pct_2h,
                    future_return_pctrank = EXCLUDED.future_return_pctrank,
                    target_adaptive_top20 = EXCLUDED.target_adaptive_top20,
                    updated_at = NOW()
                """,
                (horizon_buckets, target_threshold),
            )

            upserted_labels = int(cursor.rowcount)

    return LabelBuildStats(source_buckets=source_buckets, upserted_labels=upserted_labels)


def main() -> None:
    load_dotenv()
    horizon_buckets = int(get_env("LABELS_HORIZON_BUCKETS", "24"))
    target_threshold = float(get_env("LABELS_TARGET_THRESHOLD", "0.05"))

    stats = build_labels_5m(
        horizon_buckets=horizon_buckets,
        target_threshold=target_threshold,
    )
    print(
        "labels_5m build complete. "
        f"source_buckets={stats.source_buckets} upserted_labels={stats.upserted_labels} "
        f"horizon_buckets={horizon_buckets} target_threshold={target_threshold}"
    )


if __name__ == "__main__":
    main()