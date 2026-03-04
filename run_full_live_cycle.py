from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

from dotenv import load_dotenv

import pipeline_runner


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def run_pipeline_ticks(tick_count: int, tick_sleep_seconds: int) -> None:
    for tick_idx in range(1, tick_count + 1):
        print(f"\n[PIPELINE] Starting tick {tick_idx}/{tick_count} at {utc_now()}")
        pipeline_runner.run_tick(tick_idx)
        if tick_idx < tick_count and tick_sleep_seconds > 0:
            print(f"[PIPELINE] Sleeping {tick_sleep_seconds}s before next tick")
            time.sleep(tick_sleep_seconds)


def run_live_pick(
    model: str,
    feature_set: str,
    label_target: str,
    preprocessing: str,
    top_n: int,
    market_api: str,
    snapshot_path: str,
) -> None:
    cmd = [
        sys.executable,
        os.path.join("research", "live_top_coins.py"),
        "--mode",
        "pick",
        "--model",
        model,
        "--feature-set",
        feature_set,
        "--label-target",
        label_target,
        "--preprocessing",
        preprocessing,
        "--top-n",
        str(top_n),
        "--market-api",
        market_api,
        "--snapshot-path",
        snapshot_path,
    ]

    print(f"\n[PICKS] Running: {' '.join(cmd)}")
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"
    subprocess.run(cmd, check=True, env=env)


def run_live_verify(snapshot_path: str, verify_minutes: int) -> None:
    cmd = [
        sys.executable,
        os.path.join("research", "live_top_coins.py"),
        "--mode",
        "verify",
        "--snapshot-path",
        snapshot_path,
        "--verify-minutes",
        str(verify_minutes),
    ]
    print(f"\n[VERIFY] Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-command full cycle: pipeline tick(s) + live picks (+ optional verify)"
    )
    parser.add_argument("--skip-tick", action="store_true", help="Skip pipeline tick stage")
    parser.add_argument("--tick-count", type=int, default=1, help="How many immediate ticks to run")
    parser.add_argument(
        "--tick-sleep-seconds",
        type=int,
        default=0,
        help="Sleep between ticks (only when tick-count > 1)",
    )
    parser.add_argument(
        "--fail-on-tick-error",
        action="store_true",
        help="Stop immediately if a pipeline tick fails",
    )

    parser.add_argument("--ingest-max-pools", type=int, default=15)
    parser.add_argument("--ingest-max-pages-per-pool", type=int, default=2)
    parser.add_argument("--ingest-max-trades-per-pool", type=int, default=30)
    parser.add_argument("--ingest-lookback-hours", type=int, default=24)

    parser.add_argument("--model", choices=["logistic", "xgboost_tuned", "ensemble", "stacking"], default="stacking")
    parser.add_argument("--feature-set", choices=["v2", "cross_rank", "base"], default="v2")
    parser.add_argument("--label-target", choices=["adaptive", "fixed"], default="adaptive")
    parser.add_argument("--preprocessing", choices=["robust", "none"], default="robust")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--market-api", choices=["none", "coinstats"], default="coinstats")
    parser.add_argument("--snapshot-path", default="research/live_picks_snapshot.csv")

    parser.add_argument(
        "--verify-after-minutes",
        type=int,
        default=0,
        help="If >0, waits this many minutes then runs verify mode",
    )
    parser.add_argument(
        "--verify-minutes",
        type=int,
        default=5,
        help="Minimum minutes required by verify mode",
    )

    parser.add_argument(
        "--loop",
        action="store_true",
        help="Run continuously in a loop (Ctrl+C to stop)",
    )
    parser.add_argument(
        "--loop-interval-minutes",
        type=int,
        default=5,
        help="Minutes to sleep between cycles when --loop is set",
    )

    args = parser.parse_args()

    load_dotenv()

    os.environ["INGEST_MAX_POOLS"] = str(args.ingest_max_pools)
    os.environ["INGEST_MAX_PAGES_PER_POOL"] = str(args.ingest_max_pages_per_pool)
    os.environ["INGEST_MAX_TRADES_PER_POOL"] = str(args.ingest_max_trades_per_pool)
    os.environ["INGEST_LOOKBACK_HOURS"] = str(args.ingest_lookback_hours)

    cycle_num = 0
    while True:
        cycle_num += 1
        print("\n" + "=" * 88)
        print(f"FULL LIVE CYCLE #{cycle_num} START")
        print("=" * 88)
        print(f"Started at: {utc_now()}")
        print(
            "[CONFIG] "
            f"INGEST_MAX_POOLS={os.environ['INGEST_MAX_POOLS']} "
            f"INGEST_MAX_PAGES_PER_POOL={os.environ['INGEST_MAX_PAGES_PER_POOL']} "
            f"INGEST_MAX_TRADES_PER_POOL={os.environ['INGEST_MAX_TRADES_PER_POOL']} "
            f"INGEST_LOOKBACK_HOURS={os.environ['INGEST_LOOKBACK_HOURS']}"
        )

        try:
            if not args.skip_tick:
                try:
                    run_pipeline_ticks(args.tick_count, args.tick_sleep_seconds)
                except Exception as err:
                    print(f"[PIPELINE] Tick error: {err}")
                    if args.fail_on_tick_error:
                        raise
                    print("[PIPELINE] Continuing to live picks despite tick error")
            else:
                print("[PIPELINE] Skipped (--skip-tick)")

            run_live_pick(
                model=args.model,
                feature_set=args.feature_set,
                label_target=args.label_target,
                preprocessing=args.preprocessing,
                top_n=args.top_n,
                market_api=args.market_api,
                snapshot_path=args.snapshot_path,
            )

            if args.verify_after_minutes > 0:
                sleep_seconds = int(args.verify_after_minutes * 60)
                print(f"\n[VERIFY] Waiting {sleep_seconds}s before verify")
                time.sleep(sleep_seconds)
                run_live_verify(snapshot_path=args.snapshot_path, verify_minutes=args.verify_minutes)

        except Exception as err:
            print(f"[CYCLE] Error in cycle #{cycle_num}: {err}")

        print(f"\n[CYCLE] Cycle #{cycle_num} finished at {utc_now()}")

        if not args.loop:
            break

        interval = args.loop_interval_minutes * 60
        print(f"[LOOP] Sleeping {args.loop_interval_minutes} min before next cycle... (Ctrl+C to stop)")
        try:
            time.sleep(interval)
        except KeyboardInterrupt:
            print("\n[LOOP] Interrupted by user. Exiting.")
            break

    print("\n" + "=" * 88)
    print("FULL LIVE CYCLE COMPLETE")
    print("=" * 88)


if __name__ == "__main__":
    main()
