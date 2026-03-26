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
    try:
        with open(_THRESHOLDS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)

        if "global" in data:
            return data

        if all(k in data for k in ("strong_buy", "buy", "neutral")):
            return {
                "global": data,
                "perChain": {}
            }
    except Exception:
        pass

    return {
        "global": dict(_DEFAULT_THRESHOLDS),
        "perChain": {}
    }


def compute_adaptive_thresholds(conn: psycopg.Connection) -> dict:
    """Calibrate score thresholds by analysing pick_outcomes win rates.

    Algorithm:
      1. Fetch all recent pick_outcomes, ordered by model score DESC.
      2. Compute cumulative win rate from the highest score downward.
      3. The threshold for each label is the LOWEST score at which the cumulative
         group (all picks scoring >= threshold) still meets the target win rate
         with enough samples.
      4. Write to research/score_thresholds.json for use by all processes.

    Targets (based on market base-rate ~40% tokens going up):
      strong_buy : cumulative win rate >= 58%  (need >= 30 picks)
      buy        : cumulative win rate >= 52%  (need >= 20 picks)
      neutral    : cumulative win rate >= 44%  (need >= 15 picks)
      sell       : anything below neutral threshold

    Calibrates on the last 14 days of data to stay adaptive to recent model drift.
    """
    MIN_TOTAL      = 100    # minimum picks before calibrating per chain / globally
    MIN_SB         = 30     # min picks in group for strong_buy threshold
    MIN_BUY        = 45
    MIN_NEUTRAL    = 60
    TARGET_SB      = 0.58
    TARGET_BUY     = 0.52
    TARGET_NEUTRAL = 0.44
    SCORE_FLOOR    = 0.15   # Don't let thresholds drift below 15% model score

    fallback = dict(_DEFAULT_THRESHOLDS)
    result = {"global": fallback.copy(), "perChain": {}}

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(LOWER(chain), 'base') as chain,
                    CASE
                        WHEN model_score <= 1.0 THEN model_score
                        ELSE model_score / 100.0
                    END AS score_norm,
                    recommendation,
                    effective_return
                FROM pick_outcomes
                WHERE model_score IS NOT NULL
                  AND effective_return IS NOT NULL
                  AND picked_at_utc >= NOW() - INTERVAL '14 days'
                ORDER BY score_norm DESC
                """
            )
            rows = cur.fetchall()
    except Exception as e:
        print(f"[THRESHOLDS] DB error: {e}")
        return result

    if not rows:
        print("[THRESHOLDS] No verified picks found in the last 14 days to calibrate.")
        return result

    # Organize data for global and per-chain calibration
    global_picks = []
    chain_picks = {}

    for chain, score, rec, ret in rows:
        # Define 'win' consistent with the prediction goal: 
        # Price must go up for long-bias recommendations.
        is_win = (ret > 0)
        
        pick = (float(score), bool(is_win))
        global_picks.append(pick)
        if chain not in chain_picks:
            chain_picks[chain] = []
        chain_picks[chain].append(pick)

    def _calc_thresholds(picks: list[tuple[float, bool]]):
        # picks is already sorted by score DESC from SQL
        cum_rows = []
        cum_n = 0
        cum_wins = 0
        for score, is_win in picks:
            cum_n += 1
            if is_win:
                cum_wins += 1
            cum_wr = cum_wins / cum_n
            cum_rows.append((score, cum_n, cum_wr))

        def _pick_threshold(target: float, min_n: int, default_value: float) -> float:
            # Scanning from highest score to lowest.
            # We want the LOWEST score that still satisfies the target win rate
            # with at least min_n samples.
            last_valid = None
            for score, n, wr in cum_rows:
                if n >= min_n and wr >= target:
                    last_valid = score
                elif n >= min_n:
                    # Once WR falls below target, we stop expanding
                    break
            
            if last_valid is not None:
                return max(last_valid, SCORE_FLOOR)
            return default_value

        sb_thresh  = _pick_threshold(TARGET_SB, MIN_SB, fallback["strong_buy"])
        buy_thresh = _pick_threshold(TARGET_BUY, MIN_BUY, fallback["buy"])
        neu_thresh = _pick_threshold(TARGET_NEUTRAL, MIN_NEUTRAL, fallback["neutral"])

        # Sanity: enforce ordering strong_buy >= buy >= neutral
        buy_thresh = max(buy_thresh, neu_thresh)
        sb_thresh  = max(sb_thresh,  buy_thresh)
        
        # Ensure within 0-1 and above floor
        sb_thresh  = min(max(sb_thresh,  SCORE_FLOOR), 1.0)
        buy_thresh = min(max(buy_thresh, SCORE_FLOOR), 1.0)
        neu_thresh = min(max(neu_thresh, SCORE_FLOOR), 1.0)

        return {
            "strong_buy":    round(sb_thresh,  4),
            "buy":           round(buy_thresh, 4),
            "neutral":       round(neu_thresh, 4),
            "calibrated":    True,
            "sample_size":   len(picks),
            "calibrated_at": datetime.now(timezone.utc).isoformat(),
        }

    # Global
    global_total = len(global_picks)
    if global_total < MIN_TOTAL:
        print(f"[THRESHOLDS] Only {global_total} verified picks in 14d — need {MIN_TOTAL} to calibrate global, using defaults")
    else:
        result["global"] = _calc_thresholds(global_picks)
        print(f"[THRESHOLDS] Calibrated GLOBAL from {global_total} picks → "
              f"strong_buy>={result['global']['strong_buy']:.3f}  buy>={result['global']['buy']:.3f}  neutral>={result['global']['neutral']:.3f}")

    # Per-Chain
    for chain, picks in chain_picks.items():
        chain_total = len(picks)
        if chain_total >= MIN_TOTAL:
            result["perChain"][chain] = _calc_thresholds(picks)
            print(f"[THRESHOLDS] Calibrated {chain.upper()} from {chain_total} picks → "
                  f"strong_buy>={result['perChain'][chain]['strong_buy']:.3f}  buy>={result['perChain'][chain]['buy']:.3f}  neutral>={result['perChain'][chain]['neutral']:.3f}")
        else:
            print(f"[THRESHOLDS] {chain.upper()} has {chain_total} picks in 14d — need {MIN_TOTAL} to calibrate, using global")

    try:
        with open(_THRESHOLDS_PATH, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)
    except Exception as e:
        print(f"[THRESHOLDS] Could not write {_THRESHOLDS_PATH}: {e}")

    return result



def _rank_recommendations(scores: list[float], chains: list[str]) -> list[str]:
    """Assign recommendations from absolute model score using adaptive thresholds per chain."""
    t = load_thresholds()

    result: list[str] = []
    for score, chain in zip(scores, chains):
        ch = str(chain).lower()
        limits = t.get("perChain", {}).get(ch, t.get("global", _DEFAULT_THRESHOLDS))
        
        STRONG_BUY = limits.get("strong_buy", _DEFAULT_THRESHOLDS["strong_buy"])
        BUY        = limits.get("buy", _DEFAULT_THRESHOLDS["buy"])
        NEUTRAL    = limits.get("neutral", _DEFAULT_THRESHOLDS["neutral"])

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
        chains = [c["chain"] for c in candidates]
        recommendations = _rank_recommendations(scores, chains)

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
    win_boost: float = 1.2,
    loss_boost: float = 1.5,
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
    """Print detailed feedback statistics for the last 14 days."""
    with conn.cursor() as cur:
        # Time filter for consistency
        SINCE_14D = "picked_at_utc >= NOW() - INTERVAL '14 days'"

        cur.execute(f"SELECT COUNT(*), SUM(CASE WHEN is_win THEN 1 ELSE 0 END) FROM pick_outcomes WHERE {SINCE_14D}")
        total, wins = cur.fetchone()
        if not total:
            print("[STATS] No verified picks found in the last 14 days.")
            return

        cur.execute(
            f"SELECT recommendation, COUNT(*), "
            f"SUM(CASE WHEN is_win THEN 1 ELSE 0 END), "
            f"AVG(effective_return), "
            f"MIN(effective_return), MAX(effective_return) "
            f"FROM pick_outcomes WHERE {SINCE_14D} "
            f"GROUP BY recommendation ORDER BY recommendation"
        )
        rec_rows = cur.fetchall()

        cur.execute(
            f"SELECT chain, COUNT(*), "
            f"SUM(CASE WHEN is_win THEN 1 ELSE 0 END), "
            f"AVG(effective_return), "
            f"MIN(effective_return), MAX(effective_return) "
            f"FROM pick_outcomes WHERE {SINCE_14D} "
            f"GROUP BY chain ORDER BY chain"
        )
        chain_rows = cur.fetchall()

        cur.execute(
            f"""
            SELECT
                FLOOR(
                    (CASE WHEN model_score <= 1.0 THEN model_score ELSE model_score / 100.0 END) / 0.05
                ) * 0.05 AS bucket_low,
                COUNT(*),
                SUM(CASE WHEN is_win THEN 1 ELSE 0 END),
                AVG(return_2h),
                AVG(effective_return)
            FROM pick_outcomes
            WHERE model_score IS NOT NULL
              AND effective_return IS NOT NULL
              AND {SINCE_14D}
            GROUP BY 1
            ORDER BY 1 DESC
            """
        )
        bucket_rows = cur.fetchall()

    win_rate = (wins / total * 100) if total > 0 else 0
    print(f"\n{'='*75}")
    print(f"FEEDBACK LOOP STATISTICS (Last 14 Days)")
    print(f"{'='*75}")
    print(f"Total verified picks: {total}")
    print(f"Overall win rate:     {win_rate:.1f}%")

    print(f"\n--- By Recommendation ---")
    print(f"{'Recommendation':<15} {'Total':>6} {'Wins':>6} {'WinRate':>8} {'AvgRet':>8} {'Worst':>8} {'Best':>8}")
    for rec, cnt, w, avg_ret, worst, best in rec_rows:
        wr = (w / cnt * 100) if cnt > 0 else 0
        print(f"{rec:<15} {cnt:>6} {w:>6} {wr:>7.1f}% {avg_ret:>+7.2f}% {worst:>+7.2f}% {best:>+7.2f}%")

    print(f"\n--- By Chain ---")
    print(f"{'Chain':<10} {'Total':>6} {'Wins':>6} {'WinRate':>8} {'AvgRet':>8} {'Worst':>8} {'Best':>8}")
    for chain, cnt, w, avg_ret, worst, best in chain_rows:
        wr = (w / cnt * 100) if cnt > 0 else 0
        print(f"{chain:<10} {cnt:>6} {w:>6} {wr:>7.1f}% {avg_ret:>+7.2f}% {worst:>+7.2f}% {best:>+7.2f}%")

    print(f"\n--- By Score Bucket ---")
    print(f"{'Bucket':<10} {'Total':>6} {'Wins':>6} {'WinRate':>8} {'RawRet':>8} {'EffRet':>8}")
    for bkt_low, cnt, w, raw_ret, eff_ret in bucket_rows:
        wr = (w / cnt * 100) if cnt > 0 else 0
        label = f"{bkt_low:.2f}+"
        print(f"{label:<10} {cnt:>6} {w:>6} {wr:>7.1f}% {raw_ret:>+7.2f}% {eff_ret:>+7.2f}%")

    print(f"{'='*75}")




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
            # Auto-recalibrate thresholds after every verify run
            print(f"\n[FEEDBACK] Recalibrating thresholds...")
            compute_adaptive_thresholds(conn)

        if args.stats:
            print_stats(conn)

        if not args.verify and not args.stats:
            parser.print_help()


if __name__ == "__main__":
    main()