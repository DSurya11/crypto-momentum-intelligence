from __future__ import annotations

import argparse
import os
import time
from getpass import getpass

import psycopg
from dotenv import load_dotenv


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


def fetch_label_metrics() -> tuple[int, int, float | None]:
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
            cursor.execute(
                """
                SELECT
                    COUNT(*)::INTEGER AS label_count,
                    COALESCE(SUM(target_up_5pct_2h), 0)::INTEGER AS positive_count,
                    CASE
                        WHEN COUNT(*) = 0 THEN NULL
                        ELSE ROUND(SUM(target_up_5pct_2h)::NUMERIC / COUNT(*), 4)
                    END AS ratio
                FROM labels_5m
                """
            )
            row = cursor.fetchone()

    if row is None:
        return 0, 0, None

    label_count = int(row[0])
    positive_count = int(row[1])
    ratio = float(row[2]) if row[2] is not None else None
    return label_count, positive_count, ratio


def print_metrics() -> None:
    label_count, positive_count, ratio = fetch_label_metrics()
    ratio_text = "NULL" if ratio is None else f"{ratio:.4f}"
    print(f"label_count={label_count} positive_count={positive_count} ratio={ratio_text}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Minimal labels watcher")
    parser.add_argument("--once", action="store_true", help="Print once and exit")
    args = parser.parse_args()

    load_dotenv()

    interval_seconds = int(get_env("LABEL_WATCH_INTERVAL_SECONDS", "300"))

    if args.once:
        print_metrics()
        return

    while True:
        print_metrics()
        time.sleep(interval_seconds)


if __name__ == "__main__":
    main()
