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
import json
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


# ---------------------------------------------------------------------------
# Adaptive threshold calibration
# ---------------------------------------------------------------------------

# Stored next to the snapshot in research/
_THRESHOLDS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "score_thresholds.json")

_DEFAULT_THRESHOLDS: dict = {
    "strong_buy": 0.35,
    "buy":        0.27,
    "neutral":    0.20,
    "calibrated": False,
    "sample_size": 0,
    "calibrated_at": None,
}


def load_thresholds() -> dict:
    """Load calibrated thresholds from JSON; return defaults if file missing."""
    try:
        with open(_THRESHOLDS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Validate required keys
        if all(k in data for k in ("strong_buy", "buy", "neutral")):
            return data
    except Exception:
        pass
    return dict(_DEFAULT_THRESHOLDS)


def compute_adaptive_thresholds(conn: psycopg.Connection) -> dict:
    """Calibrate score thresholds by analysing pick_outcomes win rates per bucket.

    Algorithm:
      1. Group all real (non-backfill) pick_outcomes by 0.05-wide score buckets.
      2. Compute cumulative win rate from the top score bucket downward.
         Win = price went up (effective_return > 0) — direction-neutral,
         so we measure pure raw predictive power of high scores.
      3. The threshold for each label is the LOWEST score at which the cumulative
         group (all picks scoring >= threshold) still meets the target win rate
         with enough samples.
      4. Write to research/score_thresholds.json for use by all processes.

    Targets (based on market base-rate ~40% tokens going up):
      strong_buy : cumulative win rate >= 58%  (need >= 30 picks)
      buy        : cumulative win rate >= 52%  (need >= 20 picks)
      neutral    : cumulative win rate >= 44%  (need >= 15 picks)
      sell       : anything below neutral threshold

    Falls back to hardcoded defaults when < 150 verified picks exist.
    """
    MIN_TOTAL      = 150    # minimum picks before calibrating
    MIN_SB         = 30    # min picks in group for strong_buy threshold
    MIN_BUY        = 20
    MIN_NEUTRAL    = 15
    TARGET_SB      = 0.58
    TARGET_BUY     = 0.52
    TARGET_NEUTRAL = 0.44

    fallback = dict(_DEFAULT_THRESHOLDS)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH norm AS (
                    SELECT
                        CASE
                            WHEN model_score <= 1.0 THEN model_score
                            ELSE model_score / 100.0
                        END AS score_norm,
                        recommendation,
                        effective_return
                    FROM pick_outcomes
                    WHERE model_score IS NOT NULL
                      AND effective_return IS NOT NULL
                )
                SELECT
                    FLOOR(score_norm / 0.05) * 0.05 AS bucket_low,
                    COUNT(*)                         AS n,
                    SUM(
                        CASE
                            WHEN recommendation = 'sell'  AND effective_return <= 0 THEN 1
                            WHEN recommendation != 'sell' AND effective_return >  0 THEN 1
                            ELSE 0
                        END
                    ) AS wins
                FROM norm
                GROUP BY 1
                ORDER BY 1 DESC
                """
            )
            rows = cur.fetchall()
    except Exception as e:
        print(f"[THRESHOLDS] DB error: {e}")
        return fallback

    total = sum(int(r[1]) for r in rows)
    if total < MIN_TOTAL:
        print(f"[THRESHOLDS] Only {total} verified picks — need {MIN_TOTAL} to calibrate, using defaults")
        return fallback

    cum_rows: list[tuple[float, int, float]] = []
    cum_n = 0
    cum_wins = 0
    for bucket_low, n, wins in rows:
        n_i = int(n)
        wins_i = int(wins or 0)
        cum_n += n_i
        cum_wins += wins_i
        cum_wr = (cum_wins / cum_n) if cum_n else 0.0
        cum_rows.append((float(bucket_low), cum_n, cum_wr))

    def _pick_threshold(target: float, min_n: int, default_value: float) -> float:
        for score_low, n_i, wr_i in cum_rows:
            if n_i >= min_n and wr_i >= target:
                # cum_rows are ordered by score descending, so first match is
                # the most selective threshold that still satisfies the target.
                return score_low

        # Guardrail: do not relax thresholds to very low values when target
        # quality is not achieved. Keep stable defaults instead.
        return default_value

    sb_thresh = _pick_threshold(TARGET_SB, MIN_SB, fallback["strong_buy"])
    buy_thresh = _pick_threshold(TARGET_BUY, MIN_BUY, fallback["buy"])
    neu_thresh = _pick_threshold(TARGET_NEUTRAL, MIN_NEUTRAL, fallback["neutral"])

    # Sanity: enforce ordering
    sb_thresh  = max(sb_thresh,  buy_thresh)
    buy_thresh = max(buy_thresh, neu_thresh)
    neu_thresh = min(max(neu_thresh, 0.0), 1.0)

    sb_thresh = min(max(sb_thresh, buy_thresh), 1.0)
    buy_thresh = min(max(buy_thresh, neu_thresh), 1.0)

    result = {
        "strong_buy":    round(sb_thresh,  4),
        "buy":           round(buy_thresh, 4),
        "neutral":       round(neu_thresh, 4),
        "calibrated":    True,
        "sample_size":   total,
        "calibrated_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        with open(_THRESHOLDS_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
        print(
            f"[THRESHOLDS] Calibrated from {total} picks → "
            f"strong_buy>={sb_thresh:.2f}  buy>={buy_thresh:.2f}  neutral>={neu_thresh:.2f}"
        )
    except Exception as e:
        print(f"[THRESHOLDS] Could not write {_THRESHOLDS_PATH}: {e}")

    return result


def _rank_recommendations(scores: list[float]) -> list[str]:
    """Assign recommendations from absolute model score using adaptive thresholds.

    Thresholds are loaded from research/score_thresholds.json (written by
    compute_adaptive_thresholds after each calibration cycle).  Falls back to
    hardcoded defaults when the file doesn't exist yet.

    In a weak session where all scores are low there may be zero strong_buy
    picks — that is the correct honest behaviour.
    """
    t = load_thresholds()
    STRONG_BUY = t["strong_buy"]
    BUY        = t["buy"]
    NEUTRAL    = t["neutral"]

    result: list[str] = []
    for score in scores:
        if score >= STRONG_BUY:
            result.append("strong_buy")
        elif score >= BUY:
            result.append("buy")
        elif score >= NEUTRAL:
            result.append("neutral")
        else:
            result.append("sell")
    return result


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
    is_backfill     BOOLEAN        NOT NULL DEFAULT FALSE,
    verified_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    inserted_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_pick_outcomes
        UNIQUE (token_address, bucket_timestamp)
);
"""


def ensure_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        # Migration: add is_backfill column if the table already existed without it
        cur.execute("""
            ALTER TABLE pick_outcomes
            ADD COLUMN IF NOT EXISTS is_backfill BOOLEAN NOT NULL DEFAULT FALSE
        """)
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

    # -----------------------------------------------------------------------
    # Pass 1: Validate rows and group by pick cycle (picked_at_utc)
    # Only rows that are old enough, have a valid entry price, and haven't
    # been stored yet are kept.  We group them so we can apply rank-based
    # labeling within each cycle batch.
    # -----------------------------------------------------------------------
    from collections import defaultdict
    # cycle_candidates[picked_at_utc] = list of candidate dicts
    cycle_candidates: dict[datetime, list[dict]] = defaultdict(list)

    # ------------------------------------------------------------------
    # Batch dedup: one query to find ALL already-stored (token, bucket)
    # pairs from the snapshot, avoiding N+1 queries inside the loop.
    # ------------------------------------------------------------------
    _pre_tokens: list[str] = []
    _pre_buckets: list[datetime] = []
    for _r in rows:
        _tok = _r.get("token_address", "").strip()
        _bkt = _r.get("bucket_timestamp", "")
        if not _tok or not _bkt:
            continue
        try:
            _bts = datetime.fromisoformat(_bkt)
            if _bts.tzinfo is None:
                _bts = _bts.replace(tzinfo=timezone.utc)
            _pre_tokens.append(_tok)
            _pre_buckets.append(_bts)
        except Exception:
            pass

    stored_pairs: set[tuple] = set()
    if _pre_tokens:
        with conn.cursor() as _cur:
            _cur.execute(
                "SELECT token_address, bucket_timestamp FROM pick_outcomes"
                " WHERE token_address = ANY(%s)"
                " AND bucket_timestamp = ANY(%s::timestamptz[])",
                (_pre_tokens, _pre_buckets),
            )
            stored_pairs = {(row[0], row[1]) for row in _cur.fetchall()}

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

        # Check if already recorded (batch-preloaded set, no per-row query)
        if (token, bucket_ts) in stored_pairs:
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

        try:
            score_raw = float(r.get("score", "0"))
        except Exception:
            score_raw = 0.0

        cycle_candidates[picked_at].append({
            "token": token,
            "picked_at": picked_at,
            "bucket_ts": bucket_ts,
            "chain": r.get("chain", "base") or "base",
            "score_raw": score_raw,
            "entry_price": entry_price,
            "symbol": r.get("symbol", token[:8]),
            "row": r,
        })

    # -----------------------------------------------------------------------
    # Pass 2: Rank within each cycle, look up 2h prices, store outcomes
    # -----------------------------------------------------------------------
    for picked_at, candidates in cycle_candidates.items():
        scores = [c["score_raw"] for c in candidates]
        recommendations = _rank_recommendations(scores)

        for cand, recommendation in zip(candidates, recommendations):
            token = cand["token"]
            bucket_ts = cand["bucket_ts"]
            entry_price = cand["entry_price"]
            score_raw = cand["score_raw"]
            chain = cand["chain"]

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

            # Sanity-check: returns beyond ±500% are almost always caused by
            # corrupted swap data (e.g. native-currency token mis-pricing,
            # decimal-place errors).  Skip storing so they never pollute the
            # accuracy metrics.  Log them so they can be investigated.
            if abs(return_2h) > 500.0:
                sym = cand["symbol"]
                print(
                    f"  [OUTLIER-SKIP] {sym:12s} {chain:6s} "
                    f"entry={entry_price:.6g} price_2h={price_2h:.6g} "
                    f"ret={return_2h:+.1f}% — NOT stored (price data corrupt)"
                )
                skipped += 1
                continue

            # All picks are long positions — no inversion
            effective_return = return_2h
            is_win = return_2h > 0

            # Store outcome (is_backfill=FALSE → real live pick, used for sample weights)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pick_outcomes (
                        token_address, chain, bucket_timestamp, picked_at_utc,
                        model_score, recommendation, entry_price, price_2h,
                        return_2h, effective_return, is_win, is_backfill, verified_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, NOW())
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

            sym = cand["symbol"]
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
    loss_boost: float = 4.5,
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

    # Load all outcomes into a lookup set — EXCLUDE backfilled rows (retroactive scoring
    # by today's model; those picks don't reflect real decisions and must not bias weights)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT token_address, bucket_timestamp, is_win, return_2h, recommendation FROM pick_outcomes WHERE is_backfill = FALSE"
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
                magnitude = min(abs(ret) / 8.0, 4.0)  # cap at 4x additional
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
