from __future__ import annotations

import os
from dataclasses import dataclass
from getpass import getpass

import psycopg
from dotenv import load_dotenv


@dataclass
class VariantLabelBuildStats:
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


def build_variant_labels_5m(
    target_name: str,
    horizon_buckets: int,
    target_threshold: float,
) -> VariantLabelBuildStats:
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

            cursor.execute("SELECT COUNT(*) FROM token_price_5m")
            source_buckets = int(cursor.fetchone()[0])

            if source_buckets == 0:
                return VariantLabelBuildStats(source_buckets=0, upserted_labels=0)

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
                        ((future_close_price - close_price) / close_price) AS future_return,
                        CASE
                            WHEN ((future_close_price - close_price) / close_price) >= %s THEN 1
                            ELSE 0
                        END::SMALLINT AS target_binary
                    FROM labeled
                    WHERE future_close_price IS NOT NULL
                      AND close_price <> 0
                )
                INSERT INTO labels_5m_variants (
                    token_address,
                    bucket_timestamp,
                    target_name,
                    horizon_buckets,
                    threshold,
                    future_return,
                    target_binary
                )
                SELECT
                    token_address,
                    bucket_timestamp,
                    %s,
                    %s,
                    %s,
                    future_return,
                    target_binary
                FROM finalized
                ON CONFLICT (token_address, bucket_timestamp, target_name)
                DO UPDATE SET
                    horizon_buckets = EXCLUDED.horizon_buckets,
                    threshold = EXCLUDED.threshold,
                    future_return = EXCLUDED.future_return,
                    target_binary = EXCLUDED.target_binary,
                    updated_at = NOW()
                """,
                (
                    horizon_buckets,
                    target_threshold,
                    target_name,
                    horizon_buckets,
                    target_threshold,
                ),
            )

            upserted_labels = int(cursor.rowcount)

    return VariantLabelBuildStats(source_buckets=source_buckets, upserted_labels=upserted_labels)


def main() -> None:
    load_dotenv()

    target_name = get_env("VARIANT_TARGET_NAME", "up_3pct_2h")
    horizon_buckets = int(get_env("VARIANT_LABELS_HORIZON_BUCKETS", "24"))
    target_threshold = float(get_env("VARIANT_LABELS_TARGET_THRESHOLD", "0.03"))

    stats = build_variant_labels_5m(
        target_name=target_name,
        horizon_buckets=horizon_buckets,
        target_threshold=target_threshold,
    )

    print(
        "labels_5m_variants build complete. "
        f"target_name={target_name} source_buckets={stats.source_buckets} "
        f"upserted_labels={stats.upserted_labels} horizon_buckets={horizon_buckets} "
        f"target_threshold={target_threshold}"
    )


if __name__ == "__main__":
    main()
