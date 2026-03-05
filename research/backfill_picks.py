"""
Backfill historical picks into pick_outcomes.

Trains the stacking model ONCE on all available data, then retroactively
scores every historical 5-minute bucket in features_5m that:
  1. Is old enough that a 2h price lookup is possible (>= 2.5h ago)
  2. Has not already been inserted into pick_outcomes

For each historical bucket, the top-N tokens by model score are chosen as
picks and their 2h performance is verified directly from token_price_5m.

Usage:
    python research/backfill_picks.py                   # top-50, all chains
    python research/backfill_picks.py --top-n 100       # wider pick set
    python research/backfill_picks.py --dry-run         # show stats only
    python research/backfill_picks.py --max-buckets 48  # last 48 buckets (~4h)
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import psycopg
from dotenv import load_dotenv

# Force UTF-8 on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Make sure the research/ directory is on sys.path so local imports work
sys.path.insert(0, os.path.dirname(__file__))

from live_top_coins import get_conn, load_training_data, latest_bucket
from walkforward_evaluator_v2 import (
    FEATURE_SETS,
    LABEL_TARGETS,
    make_extratrees,
    make_logistic,
    make_random_forest,
    make_xgboost,
    robust_preprocess,
    stacking_oof_predictions,
    tune_xgboost,
)

# Re-import from feedback_loop so we don't duplicate
sys.path.insert(0, os.path.dirname(__file__))
from feedback_loop import ensure_table, _score_to_recommendation

FEATURE_SET = "v2"
LABEL_TARGET = "adaptive"


# ---------------------------------------------------------------------------
# Train stacking model, return fitted estimators
# ---------------------------------------------------------------------------

def train_stacking(x_train: np.ndarray, y_train: np.ndarray):
    """Train stacking ensemble, return (lr, xgb, rf, et, meta)."""
    print(f"[BACKFILL] Training stacking model on {len(y_train):,} rows …")
    n_folds = 5

    oof_preds, has_all = stacking_oof_predictions(x_train, y_train, n_folds=n_folds)
    oof_cov = int(has_all.sum())
    print(f"[BACKFILL] OOF coverage: {oof_cov}/{len(y_train)}")

    meta_x = oof_preds[has_all]
    meta_y = y_train[has_all]

    from sklearn.linear_model import LogisticRegression as MetaLR
    meta = MetaLR(max_iter=1000, solver="lbfgs", random_state=42)

    if len(meta_x) < 20 or len(np.unique(meta_y)) < 2:
        raise RuntimeError("Not enough OOF data to train stacking meta-learner")

    meta.fit(meta_x, meta_y)

    tuned_params = tune_xgboost(x_train, y_train, top_frac=0.10)
    lr_f  = make_logistic()
    xgb_f = make_xgboost(y_train, **tuned_params)
    rf_f  = make_random_forest(y_train)
    et_f  = make_extratrees(y_train)

    lr_f.fit(x_train, y_train)
    xgb_f.fit(x_train, y_train)
    rf_f.fit(x_train, y_train)
    et_f.fit(x_train, y_train)

    print("[BACKFILL] Base learners trained.")
    return lr_f, xgb_f, rf_f, et_f, meta


def score_with_model(
    lr_f, xgb_f, rf_f, et_f, meta,
    x_score_pp: np.ndarray,
) -> np.ndarray:
    base_score = np.column_stack([
        lr_f.predict_proba(x_score_pp)[:, 1],
        xgb_f.predict_proba(x_score_pp)[:, 1],
        rf_f.predict_proba(x_score_pp)[:, 1],
        et_f.predict_proba(x_score_pp)[:, 1],
    ])
    return meta.predict_proba(base_score)[:, 1]


# ---------------------------------------------------------------------------
# Historical buckets
# ---------------------------------------------------------------------------

def get_historical_buckets(conn: psycopg.Connection, min_age_hours: float = 2.5) -> list[datetime]:
    """Return distinct bucket_timestamps that are old enough to have 2h prices."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=min_age_hours)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT bucket_timestamp
            FROM features_5m
            WHERE bucket_timestamp <= %s
            ORDER BY bucket_timestamp ASC
            """,
            (cutoff,),
        )
        rows = cur.fetchall()
    return [r[0] for r in rows]


def get_already_done_buckets(conn: psycopg.Connection) -> set[datetime]:
    """Return bucket_timestamps that already have rows in pick_outcomes."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT bucket_timestamp FROM pick_outcomes")
        rows = cur.fetchall()
    return {r[0] for r in rows}


# ---------------------------------------------------------------------------
# Load features at a specific bucket (for historical replay)
# ---------------------------------------------------------------------------

def load_features_at_bucket(
    conn: psycopg.Connection,
    feature_names: list[str],
    bucket_ts: datetime,
) -> tuple[list[dict], np.ndarray]:
    """Load all feature rows at an exact bucket_timestamp."""
    feature_sql = ", ".join([f"f.{name}::DOUBLE PRECISION" for name in feature_names])
    sql = f"""
        SELECT
            f.token_address,
            t.symbol,
            t.name,
            t.chain,
            f.bucket_timestamp,
            tp.close_price::DOUBLE PRECISION AS entry_price,
            {feature_sql}
        FROM features_5m f
        INNER JOIN tokens t
            ON t.token_address = f.token_address
        LEFT JOIN token_price_5m tp
            ON tp.token_address = f.token_address
           AND tp.bucket_timestamp = f.bucket_timestamp
        WHERE f.bucket_timestamp = %s
        ORDER BY f.token_address ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (bucket_ts,))
        rows = cur.fetchall()

    meta = []
    feats = []
    for r in rows:
        token_address, symbol, name, chain, bts, entry_price, *fvals = r
        meta.append({
            "token_address": token_address,
            "symbol": symbol,
            "name": name,
            "chain": chain or "base",
            "bucket_timestamp": bts,
            "entry_price": float(entry_price) if entry_price is not None else float("nan"),
        })
        feats.append(fvals)

    x = np.asarray(feats, dtype=np.float64) if feats else np.empty((0, len(feature_names)))
    return meta, x


# ---------------------------------------------------------------------------
# Look up 2h price
# ---------------------------------------------------------------------------

def lookup_price_2h(conn: psycopg.Connection, token_address: str, bucket_ts: datetime) -> float | None:
    """Return the close_price of the first bar at or after bucket_ts + 2h."""
    target_ts = bucket_ts + timedelta(hours=2)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT close_price::DOUBLE PRECISION
            FROM token_price_5m
            WHERE token_address = %s
              AND bucket_timestamp >= %s
            ORDER BY bucket_timestamp ASC
            LIMIT 1
            """,
            (token_address, target_ts),
        )
        row = cur.fetchone()
    if row and row[0] is not None:
        return float(row[0])
    return None


# ---------------------------------------------------------------------------
# Insert one pick into pick_outcomes
# ---------------------------------------------------------------------------

def insert_pick(
    conn: psycopg.Connection,
    token_address: str,
    chain: str,
    bucket_ts: datetime,
    picked_at_utc: datetime,
    model_score: float,
    recommendation: str,
    entry_price: float,
    price_2h: float | None,
) -> bool:
    """Insert a pick outcome. Returns True if inserted, False if skipped."""
    if price_2h is None or np.isnan(entry_price) or entry_price == 0:
        return False

    return_2h = (price_2h - entry_price) / entry_price * 100.0
    is_win = return_2h > 0

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pick_outcomes (
                token_address, chain, bucket_timestamp, picked_at_utc,
                model_score, recommendation, entry_price, price_2h,
                return_2h, effective_return, is_win, is_backfill, verified_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE, NOW())
            ON CONFLICT (token_address, bucket_timestamp) DO NOTHING
            """,
            (
                token_address, chain, bucket_ts, picked_at_utc,
                model_score, recommendation, entry_price, price_2h,
                return_2h, return_2h, is_win,
            ),
        )
    return True


# ---------------------------------------------------------------------------
# Main backfill
# ---------------------------------------------------------------------------

def backfill(
    top_n: int = 50,
    dry_run: bool = False,
    max_buckets: int | None = None,
    min_age_hours: float = 2.5,
) -> None:
    load_dotenv()
    conn = get_conn()

    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")

    ensure_table(conn)

    feature_names = FEATURE_SETS[FEATURE_SET]

    # ── 1. Get historical bucket list ─────────────────────────────────────
    print("[BACKFILL] Querying historical buckets …")
    all_buckets = get_historical_buckets(conn, min_age_hours=min_age_hours)
    already_done = get_already_done_buckets(conn)

    pending = [b for b in all_buckets if b not in already_done]
    if max_buckets is not None:
        pending = pending[-max_buckets:]  # most recent N buckets

    print(f"[BACKFILL] Total historical buckets: {len(all_buckets)}")
    print(f"[BACKFILL] Already done: {len(already_done)}")
    print(f"[BACKFILL] To process: {len(pending)}")

    if not pending:
        print("[BACKFILL] Nothing to do — all historical buckets already backfilled.")
        conn.close()
        return

    if dry_run:
        print("[BACKFILL] DRY RUN — no DB writes. Exiting.")
        conn.close()
        return

    # ── 2. Load training data & train model ───────────────────────────────
    print("[BACKFILL] Loading training data …")
    cur_bucket = latest_bucket(conn)
    label_sql = LABEL_TARGETS[LABEL_TARGET]
    x_train_raw, y_train, _, _, _ = load_training_data(
        conn, FEATURE_SET, LABEL_TARGET, before_bucket=cur_bucket
    )
    print(f"[BACKFILL] Training rows: {len(y_train):,}")

    # Preprocess x_train (robust) using a dummy x_test (we only need train stats here)
    x_train_pp, _, _ = robust_preprocess(x_train_raw, x_train_raw, feature_names)

    lr_f, xgb_f, rf_f, et_f, meta = train_stacking(x_train_pp, y_train)

    # ── 3. Iterate over pending buckets ───────────────────────────────────
    total_inserted = 0
    total_skipped_no_price = 0

    for idx, bucket_ts in enumerate(pending, 1):
        token_rows, x_hist_raw = load_features_at_bucket(conn, feature_names, bucket_ts)

        if len(token_rows) == 0:
            continue

        # Preprocess historical features using train distribution
        _, x_hist_pp, _ = robust_preprocess(x_train_raw, x_hist_raw, feature_names)

        probs = score_with_model(lr_f, xgb_f, rf_f, et_f, meta, x_hist_pp)

        # Rank and take top_n
        ranked_indices = np.argsort(probs)[::-1][:top_n]

        bucket_inserted = 0
        for rank_pos, i in enumerate(ranked_indices, 1):
            row = token_rows[i]
            score = float(probs[i])
            entry_price = row["entry_price"]

            if np.isnan(entry_price) or entry_price == 0:
                total_skipped_no_price += 1
                continue

            price_2h = lookup_price_2h(conn, row["token_address"], bucket_ts)
            if price_2h is None:
                total_skipped_no_price += 1
                continue

            rec = _score_to_recommendation(score)
            inserted = insert_pick(
                conn,
                token_address=row["token_address"],
                chain=row["chain"],
                bucket_ts=bucket_ts,
                picked_at_utc=bucket_ts,   # use bucket time as the pick time
                model_score=score,
                recommendation=rec,
                entry_price=entry_price,
                price_2h=price_2h,
            )
            if inserted:
                bucket_inserted += 1
                total_inserted += 1

        conn.commit()

        chains_in_bucket = {r["chain"] for r in [token_rows[i] for i in ranked_indices]}
        print(
            f"  [{idx:4d}/{len(pending)}] {bucket_ts.strftime('%Y-%m-%d %H:%M')} UTC "
            f"  tokens_scored={len(token_rows):4d}  inserted={bucket_inserted:3d}  "
            f"chains={sorted(chains_in_bucket)}"
        )

    print(
        f"\n[BACKFILL] Done. Inserted {total_inserted:,} pick_outcomes rows. "
        f"Skipped {total_skipped_no_price:,} (no entry/2h price)."
    )
    conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill historical pick outcomes from features_5m")
    parser.add_argument("--top-n", type=int, default=50, help="Picks per bucket (default 50)")
    parser.add_argument("--dry-run", action="store_true", help="Show bucket stats without writing")
    parser.add_argument("--max-buckets", type=int, default=None,
                        help="Only process the most recent N pending buckets")
    parser.add_argument("--min-age-hours", type=float, default=2.5,
                        help="Minimum bucket age in hours to attempt 2h price lookup (default 2.5)")
    args = parser.parse_args()

    backfill(
        top_n=args.top_n,
        dry_run=args.dry_run,
        max_buckets=args.max_buckets,
        min_age_hours=args.min_age_hours,
    )
