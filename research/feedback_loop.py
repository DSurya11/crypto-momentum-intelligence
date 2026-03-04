"""
Feedback loop: verify past picks, store outcomes, compute sample weights.

This module:
  1. Reads the live picks snapshot CSV
  2. Looks up actual 2h prices from the database
  3. Stores verified outcomes in the pick_outcomes table
  4. Computes sample weights: tokens the model previously picked get higher
     weight in training (failures get even more weight so the model learns
     harder from its mistakes)

Usage:
  # Verify & store outcomes:
    python research/feedback_loop.py --verify

  # Show feedback statistics:
    python research/feedback_loop.py --stats
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta, timezone

import numpy as np
import psycopg
from dotenv import load_dotenv

# Force UTF-8 output on Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _env(name: str, default: str | None = None) -> str:
    val = os.getenv(name, default)
    if val is None or val == "":
        raise ValueError(f"Missing env var: {name}")
    return val


def _conn() -> psycopg.Connection:
    return psycopg.connect(
        host=_env("PGHOST", "localhost"),
        port=int(_env("PGPORT", "5432")),
        dbname=_env("PGDATABASE"),
        user=_env("PGUSER"),
        password=_env("PGPASSWORD"),
        sslmode=_env("PGSSLMODE", "disable"),
    )


def _score_to_recommendation(score: float) -> str:
    """Same thresholds as backend/api.py (calibrated for adaptive top-20%)."""
    pct = score * 100.0 if score <= 1.0 else score
    if pct >= 55:
        return "strong_buy"
    if pct >= 45:
        return "buy"
    if pct >= 35:
        return "neutral"
    return "sell"


# ---------------------------------------------------------------------------
# Ensure table exists
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pick_outcomes (
    id              BIGSERIAL PRIMARY KEY,
    token_address   VARCHAR(100)   NOT NULL,
    chain           VARCHAR(20)    NOT NULL DEFAULT 'base',
    bucket_timestamp TIMESTAMPTZ   NOT NULL,
    picked_at_utc   TIMESTAMPTZ    NOT NULL,
    model_score     DOUBLE PRECISION NOT NULL,
    recommendation  VARCHAR(20)    NOT NULL,
    entry_price     DOUBLE PRECISION,
    price_2h        DOUBLE PRECISION,
    return_2h       DOUBLE PRECISION,
    effective_return DOUBLE PRECISION,
    is_win          BOOLEAN,
    verified_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    inserted_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_pick_outcomes
        UNIQUE (token_address, bucket_timestamp)
);
"""


def ensure_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


# ---------------------------------------------------------------------------
# Verify picks & store outcomes
# ---------------------------------------------------------------------------

def verify_and_store(
    conn: psycopg.Connection,
    snapshot_path: str,
    min_age_minutes: int = 130,
) -> dict:
    """Read snapshot, look up 2h prices, store outcomes.

    Args:
        conn: Database connection
        snapshot_path: Path to live_picks_snapshot.csv
        min_age_minutes: Minimum age of a pick before we try to verify (default
            130 = 2h + 10min buffer)

    Returns:
        dict with verify stats
    """
    if not os.path.exists(snapshot_path):
        print(f"[FEEDBACK] Snapshot not found: {snapshot_path}")
        return {"verified": 0, "skipped": 0, "already": 0}

    with open(snapshot_path, "r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=min_age_minutes)

    verified = 0
    skipped = 0
    already = 0
    wins = 0
    losses = 0

    for r in rows:
        token = r.get("token_address", "").strip()
        if not token:
            continue

        picked_at_str = r.get("picked_at_utc", "")
        try:
            picked_at = datetime.fromisoformat(picked_at_str)
            if picked_at.tzinfo is None:
                picked_at = picked_at.replace(tzinfo=timezone.utc)
        except Exception:
            skipped += 1
            continue

        # Only verify picks old enough
        if picked_at > cutoff:
            skipped += 1
            continue

        bucket_str = r.get("bucket_timestamp", "")
        try:
            bucket_ts = datetime.fromisoformat(bucket_str)
            if bucket_ts.tzinfo is None:
                bucket_ts = bucket_ts.replace(tzinfo=timezone.utc)
        except Exception:
            skipped += 1
            continue

        # Check if already recorded
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pick_outcomes WHERE token_address = %s AND bucket_timestamp = %s",
                (token, bucket_ts),
            )
            if cur.fetchone():
                already += 1
                continue

        # Get entry price
        try:
            entry_price = float(r.get("entry_close_price", "nan"))
        except Exception:
            entry_price = float("nan")

        if not entry_price or np.isnan(entry_price) or entry_price == 0:
            skipped += 1
            continue

        score_raw = float(r.get("score", "0"))
        recommendation = _score_to_recommendation(score_raw)
        chain = r.get("chain", "base") or "base"

        # Look up 2h price
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT close_price::DOUBLE PRECISION
                FROM token_price_5m
                WHERE token_address = %s
                  AND bucket_timestamp >= (%s::timestamptz + INTERVAL '2 hours')
                ORDER BY bucket_timestamp ASC
                LIMIT 1
                """,
                (token, bucket_ts),
            )
            rec = cur.fetchone()

        if not rec or rec[0] is None:
            skipped += 1
            continue

        price_2h = float(rec[0])
        return_2h = (price_2h - entry_price) / entry_price * 100.0

        # All picks are long positions — no inversion
        effective_return = return_2h
        is_win = return_2h > 0

        # Store outcome
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pick_outcomes (
                    token_address, chain, bucket_timestamp, picked_at_utc,
                    model_score, recommendation, entry_price, price_2h,
                    return_2h, effective_return, is_win, verified_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (token_address, bucket_timestamp) DO NOTHING
                """,
                (
                    token, chain, bucket_ts, picked_at,
                    score_raw, recommendation, entry_price, price_2h,
                    return_2h, effective_return, is_win,
                ),
            )
        conn.commit()

        verified += 1
        if is_win:
            wins += 1
        else:
            losses += 1

        sym = r.get("symbol", token[:8])
        print(
            f"  [{'WIN' if is_win else 'LOSS'}] {sym:12s} {chain:6s} "
            f"rec={recommendation:10s} score={score_raw:.4f} "
            f"ret={return_2h:+.2f}%"
        )

    total_verified = verified
    win_rate = (wins / total_verified * 100) if total_verified > 0 else 0

    print(f"\n[FEEDBACK] Verified: {verified}  Skipped: {skipped}  Already: {already}")
    if total_verified > 0:
        print(f"[FEEDBACK] Wins: {wins}  Losses: {losses}  Win Rate: {win_rate:.1f}%")

    return {
        "verified": verified,
        "skipped": skipped,
        "already": already,
        "wins": wins,
        "losses": losses,
        "winRate": win_rate,
    }


# ---------------------------------------------------------------------------
# Load sample weights from feedback
# ---------------------------------------------------------------------------

def load_feedback_weights(
    conn: psycopg.Connection,
    token_addresses: list[str],
    bucket_timestamps: list,
    base_weight: float = 1.0,
    win_boost: float = 1.5,
    loss_boost: float = 3.0,
) -> np.ndarray:
    """Compute per-sample training weights using feedback outcomes.

    For tokens the model previously picked:
      - Losses get `loss_boost`× weight (learn harder from mistakes)
      - Wins get `win_boost`× weight (reinforce good patterns)
      - Unpicked tokens get `base_weight` (1.0)

    This ensures the model pays extra attention to tokens it has actually
    encountered in live trading, especially to patterns that led to losses.

    Args:
        conn: DB connection
        token_addresses: List of token addresses in training data
        bucket_timestamps: List of bucket_timestamps in training data
        base_weight: Default weight for samples without feedback
        win_boost: Weight multiplier for previously-won picks
        loss_boost: Weight multiplier for previously-lost picks

    Returns:
        numpy array of sample weights, same length as token_addresses
    """
    weights = np.full(len(token_addresses), base_weight, dtype=np.float64)

    # Load all outcomes into a lookup set — include recommendation to judge model accuracy
    with conn.cursor() as cur:
        cur.execute(
            "SELECT token_address, bucket_timestamp, is_win, return_2h, recommendation FROM pick_outcomes"
        )
        outcomes = cur.fetchall()

    if not outcomes:
        print("[FEEDBACK] No feedback outcomes yet — using uniform weights")
        return weights

    # Build lookup: (token_address, bucket_timestamp_iso) → (model_error, return_2h)
    # model_error = True when the model's directional call was WRONG:
    #   - Predicted bullish (buy/strong_buy) but price fell  → model error
    #   - Predicted bearish (sell) but price rose            → model error (false negative)
    # Neutral picks are unpredictional → skip weight boosting
    outcome_map: dict[tuple[str, str], tuple[bool, float]] = {}
    for token, bucket_ts, is_win, ret, rec in outcomes:
        key = (token, bucket_ts.isoformat() if hasattr(bucket_ts, "isoformat") else str(bucket_ts))
        bullish_pick = rec in ("buy", "strong_buy")
        bearish_pick = rec == "sell"
        if bullish_pick:
            model_error = not bool(is_win)          # predicted up, got down
        elif bearish_pick:
            model_error = bool(is_win)              # predicted no-rise, but price rose
        else:
            model_error = None                      # neutral — no directional claim
        outcome_map[key] = (model_error, float(ret) if ret is not None else 0.0)

    boosted = 0
    for i, (addr, bts) in enumerate(zip(token_addresses, bucket_timestamps)):
        bts_str = bts.isoformat() if hasattr(bts, "isoformat") else str(bts)
        key = (addr, bts_str)
        if key in outcome_map:
            model_error, ret = outcome_map[key]
            if model_error is None:
                # Neutral prediction — no directional claim, leave at base weight
                pass
            elif model_error:
                # Model was directionally wrong — learn harder from this
                magnitude = min(abs(ret) / 10.0, 3.0)  # cap at 3× additional
                weights[i] = loss_boost + magnitude
                boosted += 1
            else:
                # Model was directionally correct — mild reinforcement
                weights[i] = win_boost
                boosted += 1

    # Also boost ANY occurrence of tokens that had outcomes (even at different buckets)
    # to help the model learn the general patterns of picked tokens
    outcome_tokens = {token for token, *_ in outcomes}
    token_boosted = 0
    for i, addr in enumerate(token_addresses):
        if addr in outcome_tokens and weights[i] == base_weight:
            # Mild boost for same token at different time
            weights[i] = base_weight * 1.2
            token_boosted += 1

    print(
        f"[FEEDBACK] Sample weights: {len(weights)} total, "
        f"{boosted} exact-match boosted, {token_boosted} token-match boosted, "
        f"{len(outcome_map)} outcomes loaded"
    )

    return weights


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def print_stats(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM pick_outcomes")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM pick_outcomes WHERE is_win = TRUE")
        wins = cur.fetchone()[0]

        cur.execute(
            "SELECT recommendation, COUNT(*), "
            "SUM(CASE WHEN is_win THEN 1 ELSE 0 END), "
            "AVG(effective_return), "
            "MIN(effective_return), MAX(effective_return) "
            "FROM pick_outcomes GROUP BY recommendation ORDER BY recommendation"
        )
        rec_rows = cur.fetchall()

        cur.execute(
            "SELECT chain, COUNT(*), "
            "SUM(CASE WHEN is_win THEN 1 ELSE 0 END), "
            "AVG(effective_return) "
            "FROM pick_outcomes GROUP BY chain ORDER BY chain"
        )
        chain_rows = cur.fetchall()

        # Score bucket analysis
        cur.execute(
            """
            SELECT
                CASE
                    WHEN model_score >= 0.55 THEN 'strong_buy (>=55%)'
                    WHEN model_score >= 0.45 THEN 'buy (45-55%)'
                    WHEN model_score >= 0.35 THEN 'neutral (35-45%)'
                    ELSE 'sell (<35%)'
                END AS bucket,
                COUNT(*),
                SUM(CASE WHEN is_win THEN 1 ELSE 0 END),
                AVG(return_2h),
                AVG(effective_return)
            FROM pick_outcomes
            GROUP BY bucket
            ORDER BY bucket
            """
        )
        bucket_rows = cur.fetchall()

    win_rate = (wins / total * 100) if total > 0 else 0
    print(f"\n{'='*60}")
    print(f"FEEDBACK LOOP STATISTICS")
    print(f"{'='*60}")
    print(f"Total verified picks: {total}")
    print(f"Overall win rate:     {win_rate:.1f}%")

    print(f"\n--- By Recommendation ---")
    print(f"{'Recommendation':<15} {'Total':>6} {'Wins':>6} {'WinRate':>8} {'AvgRet':>8} {'Best':>8} {'Worst':>8}")
    for rec, cnt, w, avg_ret, worst, best in rec_rows:
        wr = (w / cnt * 100) if cnt > 0 else 0
        print(f"{rec:<15} {cnt:>6} {w:>6} {wr:>7.1f}% {avg_ret:>+7.2f}% {best:>+7.2f}% {worst:>+7.2f}%")

    print(f"\n--- By Chain ---")
    print(f"{'Chain':<10} {'Total':>6} {'Wins':>6} {'WinRate':>8} {'AvgRet':>8}")
    for chain, cnt, w, avg_ret in chain_rows:
        wr = (w / cnt * 100) if cnt > 0 else 0
        print(f"{chain:<10} {cnt:>6} {w:>6} {wr:>7.1f}% {avg_ret:>+7.2f}%")

    print(f"\n--- By Score Bucket ---")
    print(f"{'Bucket':<25} {'Total':>6} {'Wins':>6} {'WinRate':>8} {'RawRet':>8} {'EffRet':>8}")
    for bucket, cnt, w, raw_ret, eff_ret in bucket_rows:
        wr = (w / cnt * 100) if cnt > 0 else 0
        print(f"{bucket:<25} {cnt:>6} {w:>6} {wr:>7.1f}% {raw_ret:>+7.2f}% {eff_ret:>+7.2f}%")

    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Model feedback loop: verify picks & compute weights")
    parser.add_argument("--verify", action="store_true", help="Verify past picks and store outcomes")
    parser.add_argument("--stats", action="store_true", help="Print feedback statistics")
    parser.add_argument("--snapshot-path", default="research/live_picks_snapshot.csv")
    parser.add_argument("--min-age-minutes", type=int, default=130,
                        help="Min age (minutes) before attempting verification")
    args = parser.parse_args()

    load_dotenv()

    with _conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")

        ensure_table(conn)

        if args.verify:
            print(f"[FEEDBACK] Verifying picks from {args.snapshot_path}...")
            verify_and_store(conn, args.snapshot_path, args.min_age_minutes)

        if args.stats:
            print_stats(conn)

        if not args.verify and not args.stats:
            parser.print_help()


if __name__ == "__main__":
    main()
