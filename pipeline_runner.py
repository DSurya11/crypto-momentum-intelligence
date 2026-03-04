"""pipeline_runner.py

Thin orchestrator that runs each ingestion step in order for one tick.
Called by run_full_live_cycle.py:

    import pipeline_runner
    pipeline_runner.run_tick(tick_idx)
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

# All ingestion scripts live under ingestion/
_INGESTION_DIR = Path(__file__).parent / "ingestion"

_STEPS: list[tuple[str, str]] = [
    ("SWAPS",    "base_swaps_ingestor.py"),
    ("PRICES",   "token_price_5m_builder.py"),
    ("METRICS",  "token_metrics_5m_aggregator.py"),
    ("FEATURES", "features_5m_builder.py"),
    ("LABELS",   "labels_5m_builder.py"),
]


def _run_step(label: str, script: str, tick_idx: int) -> None:
    script_path = _INGESTION_DIR / script
    cmd = [sys.executable, str(script_path)]
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"
    print(f"[TICK {tick_idx}] [{label}] Running: {script}")
    result = subprocess.run(cmd, env=env, cwd=str(_INGESTION_DIR))
    if result.returncode != 0:
        raise RuntimeError(
            f"[TICK {tick_idx}] [{label}] {script} exited with code {result.returncode}"
        )
    print(f"[TICK {tick_idx}] [{label}] Done.")


def run_tick(tick_idx: int) -> None:
    """Run all ingestion steps in order for a single pipeline tick."""
    print(f"[TICK {tick_idx}] Pipeline tick starting — {len(_STEPS)} steps")
    for label, script in _STEPS:
        _run_step(label, script, tick_idx)
    print(f"[TICK {tick_idx}] Pipeline tick complete.")
