from __future__ import annotations

import argparse
import atexit
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


SUMMARY_ROC_RE = re.compile(r"SUMMARY\s+ROC_MEAN=([0-9.\-]+)")
SUMMARY_PTOP_RE = re.compile(r"SUMMARY\s+P@TOP\d+_MEAN=([0-9.\-]+)")
RECOMMEND_RE = re.compile(r"RECOMMENDATION=(\w+)\s*(.*)")


def utc_now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def acquire_lock(lock_path: Path) -> int:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = lock_path.open("x", encoding="utf-8")
    except FileExistsError:
        print(f"[monitor] lock exists, another instance appears active: {lock_path}", flush=True)
        raise SystemExit(0)

    fd.write(str(Path(".").resolve()))
    fd.write("\n")
    fd.write(str(sys.executable))
    fd.write("\n")
    fd.write(str(Path(__file__).resolve()))
    fd.write("\n")
    fd.write(str(utc_now_text()))
    fd.write("\n")
    fd.close()

    def _cleanup() -> None:
        try:
            if lock_path.exists():
                lock_path.unlink()
        except Exception:
            pass

    atexit.register(_cleanup)
    return 0


def parse_summary(stdout: str) -> tuple[float, float, str, str]:
    roc_match = SUMMARY_ROC_RE.search(stdout)
    ptop_match = SUMMARY_PTOP_RE.search(stdout)
    rec_match = RECOMMEND_RE.search(stdout)

    if not roc_match or not ptop_match:
        raise ValueError("Could not parse SUMMARY metrics from evaluator output")

    roc_mean = float(roc_match.group(1))
    ptop_mean = float(ptop_match.group(1))
    recommendation = rec_match.group(1) if rec_match else "N/A"
    rec_reason = rec_match.group(2).strip() if rec_match else ""
    return roc_mean, ptop_mean, recommendation, rec_reason


def run_walkforward(
    feature_set: str,
    label_table: str,
    target_name: str,
    top_fraction: float,
    train_fraction: float,
    test_fraction: float,
    step_fraction: float,
    csv_path: str,
    baseline_roc_mean: float | None,
    baseline_ptop_mean: float | None,
    max_roc_drop: float,
    min_ptop_delta: float,
) -> tuple[str, float, float, str, str]:
    command = [
        sys.executable,
        "research/logreg_walkforward_timesplit.py",
        "--label-table",
        label_table,
        "--top-fraction",
        str(top_fraction),
        "--train-fraction",
        str(train_fraction),
        "--test-fraction",
        str(test_fraction),
        "--step-fraction",
        str(step_fraction),
        "--feature-set",
        feature_set,
        "--csv-path",
        csv_path,
        "--max-roc-drop",
        str(max_roc_drop),
        "--min-ptop-delta",
        str(min_ptop_delta),
    ]

    if target_name:
        command.extend(["--target-name", target_name])
    if baseline_roc_mean is not None:
        command.extend(["--baseline-roc-mean", str(baseline_roc_mean)])
    if baseline_ptop_mean is not None:
        command.extend(["--baseline-ptop-mean", str(baseline_ptop_mean)])

    result = subprocess.run(command, capture_output=True, text=True, check=False)
    output = (result.stdout or "") + ("\n[stderr]\n" + result.stderr if result.stderr else "")

    if result.returncode != 0:
        raise RuntimeError(f"walkforward run failed for {feature_set}:\n{output}")

    roc_mean, ptop_mean, recommendation, rec_reason = parse_summary(result.stdout or "")
    return output, roc_mean, ptop_mean, recommendation, rec_reason


def main() -> None:
    parser = argparse.ArgumentParser(description="Continuous walk-forward drift monitor (base vs cross_rank)")
    parser.add_argument("--label-table", type=str, default="labels_5m")
    parser.add_argument("--target-name", type=str, default="")
    parser.add_argument("--top-fraction", type=float, default=0.10)
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--test-fraction", type=float, default=0.20)
    parser.add_argument("--step-fraction", type=float, default=0.10)
    parser.add_argument("--max-roc-drop", type=float, default=0.01)
    parser.add_argument("--min-ptop-delta", type=float, default=0.0)
    parser.add_argument("--interval-seconds", type=int, default=3600)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--csv-path", type=str, default="research/walkforward_runs.csv")
    parser.add_argument("--log-dir", type=str, default="research")
    parser.add_argument("--lock-path", type=str, default="research/walkforward_drift_monitor.lock")
    args = parser.parse_args()

    if args.interval_seconds <= 0:
        raise ValueError("--interval-seconds must be > 0")

    project_root = Path(__file__).resolve().parent.parent

    lock_path = Path(args.lock_path)
    if not lock_path.is_absolute():
        lock_path = (project_root / lock_path).resolve()

    acquire_lock(lock_path)

    log_dir = Path(args.log_dir)
    if not log_dir.is_absolute():
        log_dir = (project_root / log_dir).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)

    cycle = 0
    while True:
        cycle += 1
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        monitor_log = log_dir / f"walkforward_monitor_{stamp}.txt"

        print(f"\n[monitor] cycle={cycle} started at {utc_now_text()}", flush=True)

        base_output, base_roc, base_ptop, base_rec, base_reason = run_walkforward(
            feature_set="base",
            label_table=args.label_table,
            target_name=args.target_name,
            top_fraction=args.top_fraction,
            train_fraction=args.train_fraction,
            test_fraction=args.test_fraction,
            step_fraction=args.step_fraction,
            csv_path=args.csv_path,
            baseline_roc_mean=None,
            baseline_ptop_mean=None,
            max_roc_drop=args.max_roc_drop,
            min_ptop_delta=args.min_ptop_delta,
        )

        rank_output, rank_roc, rank_ptop, rank_rec, rank_reason = run_walkforward(
            feature_set="cross_rank",
            label_table=args.label_table,
            target_name=args.target_name,
            top_fraction=args.top_fraction,
            train_fraction=args.train_fraction,
            test_fraction=args.test_fraction,
            step_fraction=args.step_fraction,
            csv_path=args.csv_path,
            baseline_roc_mean=base_roc,
            baseline_ptop_mean=base_ptop,
            max_roc_drop=args.max_roc_drop,
            min_ptop_delta=args.min_ptop_delta,
        )

        with open(monitor_log, "w", encoding="utf-8") as handle:
            handle.write(f"[monitor] cycle={cycle} utc={utc_now_text()}\n")
            handle.write(
                f"[summary] base_roc_mean={base_roc:.4f} base_p@top_mean={base_ptop:.4f} "
                f"rank_roc_mean={rank_roc:.4f} rank_p@top_mean={rank_ptop:.4f}\n"
            )
            handle.write(
                f"[decision] rank_vs_base={rank_rec} reason={rank_reason}\n\n"
            )
            handle.write("===== BASE OUTPUT =====\n")
            handle.write(base_output)
            handle.write("\n===== CROSS_RANK OUTPUT =====\n")
            handle.write(rank_output)

        print(
            "[monitor] "
            f"base(roc={base_roc:.4f},p@top={base_ptop:.4f}) "
            f"cross_rank(roc={rank_roc:.4f},p@top={rank_ptop:.4f}) "
            f"decision={rank_rec}",
            flush=True,
        )
        print(f"[monitor] saved={monitor_log}", flush=True)

        if args.once:
            break

        print(f"[monitor] sleeping {args.interval_seconds}s", flush=True)
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
