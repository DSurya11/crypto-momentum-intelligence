from __future__ import annotations

import csv
import json
import os
import subprocess
import threading
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psycopg
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT_PATH = PROJECT_ROOT / "research" / "live_picks_snapshot.csv"
THRESHOLDS_PATH = PROJECT_ROOT / "research" / "score_thresholds.json"

app = FastAPI(title="Crypto Momentum Backend")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RunCycleRequest(BaseModel):
    tickCount: int = 1
    topN: int = 10
    marketApi: str = "coinstats"
    ingestMaxPools: int = 15


# ---- Background cycle runner state ----------------------------------------
_cycle_lock = threading.Lock()
_cycle_status: dict[str, Any] = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "ok": None,
    "code": None,
    "output_lines": [],
    "error": None,
}


def _run_cycle_background(req: RunCycleRequest) -> None:
    """Execute the full live cycle in a background thread."""
    global _cycle_status
    script = PROJECT_ROOT / "runlive.ps1"
    cmd = [
        "powershell",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        str(script),
        "-TickCount",
        str(req.tickCount),
        "-TopN",
        str(req.topN),
        "-MarketApi",
        req.marketApi,
        "-IngestMaxPools",
        str(req.ingestMaxPools),
    ]

    with _cycle_lock:
        _cycle_status["output_lines"].append(
            f"[BACKEND] Cycle process started at {datetime.now(timezone.utc).isoformat()}"
        )

    try:
        cycle_env = os.environ.copy()
        cycle_env["PYTHONIOENCODING"] = "utf-8"
        cycle_env["PYTHONUNBUFFERED"] = "1"
        proc = subprocess.Popen(
            cmd,
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            env=cycle_env,
        )
        for line in proc.stdout:  # type: ignore[union-attr]
            stripped = line.rstrip("\n")
            if stripped:
                with _cycle_lock:
                    _cycle_status["output_lines"].append(stripped)
                    # Cap at 500 lines to avoid memory bloat
                    if len(_cycle_status["output_lines"]) > 500:
                        _cycle_status["output_lines"] = _cycle_status["output_lines"][-300:]
        proc.wait(timeout=900)
        with _cycle_lock:
            _cycle_status["ok"] = proc.returncode == 0
            _cycle_status["code"] = proc.returncode
    except subprocess.TimeoutExpired:
        proc.kill()  # type: ignore[possibly-undefined]
        with _cycle_lock:
            _cycle_status["ok"] = False
            _cycle_status["error"] = "Timed out after 900s"
    except Exception as err:
        with _cycle_lock:
            _cycle_status["ok"] = False
            _cycle_status["error"] = str(err)
    finally:
        with _cycle_lock:
            _cycle_status["running"] = False
            _cycle_status["finished_at"] = datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


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


_DEFAULT_THRESHOLDS = {"strong_buy": 35.0, "buy": 27.0, "neutral": 20.0}


def _load_thresholds() -> dict:
    """Load calibrated score thresholds from research/score_thresholds.json.

    Values in the JSON are stored as 0-1 floats (feedback_loop scale).
    This function returns them multiplied by 100 to match score_pcts scale
    used throughout api.py.  Falls back to hardcoded defaults.
    """
    try:
        with open(THRESHOLDS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if all(k in data for k in ("strong_buy", "buy", "neutral")):
            return {
                "strong_buy": float(data["strong_buy"]) * 100.0,
                "buy":        float(data["buy"])        * 100.0,
                "neutral":    float(data["neutral"])    * 100.0,
            }
    except Exception:
        pass
    return dict(_DEFAULT_THRESHOLDS)


def _recommendations_from_scores(score_pcts: list[float]) -> list[str]:
    """Assign recommendations purely from absolute model score (score_pcts 0–100).

    Thresholds are loaded from research/score_thresholds.json, auto-calibrated
    each cycle by compute_adaptive_thresholds() in feedback_loop.py based on
    real win rates per score bucket.  Falls back to hardcoded defaults.

    In a weak session where all scores are low there may be zero strong_buy
    picks — that is the correct honest behaviour.
    """
    t = _load_thresholds()
    STRONG_BUY = t["strong_buy"]
    BUY        = t["buy"]
    NEUTRAL    = t["neutral"]

    result: list[str] = []
    for score in score_pcts:
        if score >= STRONG_BUY:
            result.append("strong_buy")
        elif score >= BUY:
            result.append("buy")
        elif score >= NEUTRAL:
            result.append("neutral")
        else:
            result.append("sell")
    return result


def _score_to_recommendation(score_pct: float) -> str:
    """Single-score recommendation using same adaptive thresholds."""
    t = _load_thresholds()
    if score_pct >= t["strong_buy"]:
        return "strong_buy"
    if score_pct >= t["buy"]:
        return "buy"
    if score_pct >= t["neutral"]:
        return "neutral"
    return "sell"


def _normalize_snapshot_row(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)

    chain_val = out.get("chain")
    chain = str(chain_val).strip().lower() if chain_val is not None else ""
    score_val = out.get("score")
    entry_val = out.get("entry_close_price")

    # Legacy header without `chain` shifts new-row columns:
    # score <- chain, entry_close_price <- score, extra(None[0]) <- entry_close_price.
    if not chain and _safe_float(score_val) is None:
        shifted_chain = str(score_val).strip().lower() if score_val is not None else ""
        if shifted_chain:
            out["chain"] = shifted_chain
            out["score"] = entry_val
            extras = out.get(None)
            if isinstance(extras, list) and extras:
                out["entry_close_price"] = extras[0]

    if not out.get("chain"):
        out["chain"] = "base"

    return out


def _read_latest_snapshot(top_n: int | None = None) -> tuple[str | None, list[dict[str, str]]]:
    if not SNAPSHOT_PATH.exists():
        return None, []

    with SNAPSHOT_PATH.open("r", encoding="utf-8", newline="") as f:
        rows = [_normalize_snapshot_row(r) for r in csv.DictReader(f)]

    if not rows:
        return None, []

    # Latest timestamp *per chain* so every chain stays represented even when
    # each chain is processed at a slightly different moment during a cycle.
    latest_per_chain: dict[str, str] = {}
    for r in rows:
        chain = str(r.get("chain", "base")).strip().lower()
        ts = r["picked_at_utc"]
        if chain not in latest_per_chain or ts > latest_per_chain[chain]:
            latest_per_chain[chain] = ts

    picks = sorted(
        [r for r in rows
         if r["picked_at_utc"] == latest_per_chain.get(
             str(r.get("chain", "base")).strip().lower())],
        key=lambda x: int(x["rank"]),
    )

    latest = max(latest_per_chain.values())
    if top_n is not None:
        picks = picks[: max(1, top_n)]
    return latest, picks


def _fetch_coinstats_market(addresses: list[str]) -> dict[str, dict[str, Any]]:
    api_key = os.getenv("COINSTATS_API_KEY", "").strip()
    if not api_key or not addresses:
        return {}

    lower_to_original: dict[str, str] = {}
    for addr in addresses:
        if addr:
            lower_to_original[addr.lower()] = addr
    request_addresses = list(lower_to_original.values())
    if not request_addresses:
        return {}

    blockchains = os.getenv("COINSTATS_BLOCKCHAINS", "").strip()
    query_params: dict[str, str] = {
        "contractAddresses": ",".join(request_addresses),
        "limit": str(max(20, len(request_addresses))),
    }
    if blockchains:
        query_params["blockchains"] = blockchains

    query = urllib.parse.urlencode(query_params)

    req = urllib.request.Request(
        f"https://openapiv1.coinstats.app/coins?{query}",
        headers={
            "X-API-KEY": api_key,
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )

    try:
        payload = json.loads(urllib.request.urlopen(req, timeout=30).read().decode("utf-8"))
    except Exception:
        return {}

    requested = set(lower_to_original.keys())
    out: dict[str, dict[str, Any]] = {}
    for coin in payload.get("result") or []:
        addrs: list[str] = []

        single_addr = coin.get("contractAddress")
        if isinstance(single_addr, str) and single_addr:
            addrs.append(single_addr.lower())

        for item in coin.get("contractAddresses") or []:
            if isinstance(item, str):
                addrs.append(item.lower())
                continue

            if isinstance(item, dict):
                addr_val = item.get("contractAddress") or item.get("address")
                if isinstance(addr_val, str) and addr_val:
                    addrs.append(addr_val.lower())

        for addr in addrs:
            if requested and addr not in requested:
                continue
            data = {
                "symbol": coin.get("symbol"),
                "name": coin.get("name"),
                "price": _safe_float(coin.get("price")),
                "price_change_24h": _safe_float(coin.get("priceChange1d")),
                "market_cap": _safe_float(coin.get("marketCap")),
                "volume_24h": _safe_float(coin.get("volume")),
            }
            out[addr] = data
            original_addr = lower_to_original.get(addr)
            if original_addr and original_addr != addr:
                out[original_addr] = data
    return out


@app.get("/api/health")
def api_health():
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM swaps_raw")
                swaps = int(cur.fetchone()[0])

                cur.execute("SELECT COUNT(*) FROM labels_5m")
                labels = int(cur.fetchone()[0])

                cur.execute("SELECT MAX(bucket_timestamp) FROM features_5m")
                max_feature_bucket = cur.fetchone()[0]

                # Win rate from pick_outcomes: correct direction = win
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE
                            WHEN recommendation = 'sell'     AND effective_return <= 0 THEN 1
                            WHEN recommendation != 'sell'    AND effective_return >  0 THEN 1
                            ELSE 0
                        END) AS wins
                    FROM pick_outcomes
                    """
                )
                _wr = cur.fetchone()
                win_rate: float | None = (
                    round(float(_wr[1]) / float(_wr[0]) * 100.0, 1)
                    if _wr and _wr[0] else None
                )

        # lastRun = most recent of: snapshot pick time or latest pipeline bucket
        try:
            picked_at, picks = _read_latest_snapshot(top_n=1)
            last_run = picked_at
            total_picks = len(_read_latest_snapshot(top_n=None)[1])
        except Exception:
            last_run = None
            total_picks = 0

        # Prefer the latest pipeline bucket if it is more recent than the snapshot.
        # Compare as datetime objects to avoid ASCII ordering bugs (T vs space in isoformat).
        if max_feature_bucket:
            bucket_iso = max_feature_bucket.isoformat()
            if last_run is None:
                last_run = bucket_iso
            else:
                try:
                    from datetime import timezone as _tz
                    def _parse_dt(s: str):
                        from datetime import datetime
                        s = s.replace(" ", "T")
                        try:
                            return datetime.fromisoformat(s)
                        except Exception:
                            return datetime.fromisoformat(s[:19]).replace(tzinfo=_tz.utc)
                    bucket_dt = _parse_dt(bucket_iso)
                    snap_dt = _parse_dt(last_run)
                    # Normalise to UTC for fair comparison
                    if bucket_dt.tzinfo is None:
                        bucket_dt = bucket_dt.replace(tzinfo=_tz.utc)
                    if snap_dt.tzinfo is None:
                        snap_dt = snap_dt.replace(tzinfo=_tz.utc)
                    if bucket_dt > snap_dt:
                        last_run = bucket_iso
                except Exception:
                    pass  # keep existing last_run on parse failure

        # Normalise lastRun to ISO-8601 with T separator so browsers parse it correctly
        if last_run and isinstance(last_run, str):
            last_run = last_run.replace(" ", "T")

        return {
            "database": "online",
            "marketApi": "online" if os.getenv("COINSTATS_API_KEY") else "degraded",
            "pipeline": "online",
            "lastRun": last_run,
            "totalPicks": total_picks,
            "winRate": win_rate,
            "swaps": swaps,
            "labels": labels,
            "latestBucket": max_feature_bucket.isoformat() if max_feature_bucket else None,
        }
    except Exception as err:
        return {
            "database": "offline",
            "marketApi": "degraded",
            "pipeline": "degraded",
            "error": str(err),
            "lastRun": None,
            "totalPicks": 0,
            "winRate": None,
            "swaps": 0,
            "labels": 0,
            "latestBucket": None,
        }


@app.get("/api/latest-picks")
def api_latest_picks(top_n: int = 10, chain: str = "all"):
    picked_at, all_picks = _read_latest_snapshot(top_n=None)
    if picked_at is None:
        return {"pickedAt": None, "elapsedMinutes": None, "rows": []}
    chain_raw = (chain or "all").strip().lower()
    if chain_raw != "all":
        allowed = {c.strip() for c in chain_raw.split(",") if c.strip()}
        picks = [p for p in all_picks if str(p.get("chain", "base")).strip().lower() in allowed]
    else:
        picks = all_picks

    if top_n is not None and top_n > 0:
        picks = picks[:top_n]

    picked_dt = datetime.fromisoformat(picked_at)
    elapsed_min = (datetime.now(timezone.utc) - picked_dt).total_seconds() / 60.0

    addresses = [p["token_address"] for p in picks if p.get("token_address")]
    market = _fetch_coinstats_market(addresses)

    # Pre-compute rank-based recommendations across the full batch of picks
    score_pcts_batch = [
        (_safe_float(p.get("score")) or 0.0) * 100.0
        if (_safe_float(p.get("score")) or 0.0) <= 1.0
        else (_safe_float(p.get("score")) or 0.0)
        for p in picks
    ]
    batch_recommendations = _recommendations_from_scores(score_pcts_batch)

    rows: list[dict[str, Any]] = []
    for p, recommendation in zip(picks, batch_recommendations):
        addr = p["token_address"]
        mk = market.get(addr) or market.get(addr.lower(), {})
        score_raw = _safe_float(p.get("score")) or 0.0
        score_pct = score_raw * 100.0 if score_raw <= 1.0 else score_raw
        entry_price = _safe_float(p.get("entry_close_price"))
        now_price = mk.get("price") if mk.get("price") is not None else entry_price
        change_pct = None
        if entry_price and now_price and entry_price != 0:
            change_pct = ((now_price - entry_price) / entry_price) * 100.0

        raw_change = mk.get("price_change_24h") if mk.get("price_change_24h") is not None else change_pct
        # All picks tracked as long positions — no sell inversion
        effective_change = raw_change

        rows.append(
            {
                "rank": int(p["rank"]),
                "symbol": mk.get("symbol") or p.get("symbol"),
                "name": mk.get("name") or p.get("name"),
                "tokenAddress": p.get("token_address"),
                "chain": p.get("chain", "base"),
                "modelScore": score_pct,
                "currentPrice": now_price,
                "priceChange24h": raw_change,
                "effectiveChange": effective_change,
                "pickedAt": p.get("picked_at_utc"),
                "verifyAfterMinutes": 120,
                "label": recommendation,
                "marketCap": mk.get("market_cap"),
                "volume24h": mk.get("volume_24h"),
            }
        )

    return {
        "pickedAt": picked_at,
        "elapsedMinutes": elapsed_min,
        "rows": rows,
    }


@app.get("/api/verify-latest")
def api_verify_latest():
    picked_at, picks = _read_latest_snapshot(top_n=None)
    if picked_at is None:
        return {"message": "No picks yet", "verified": []}
    now = datetime.now(timezone.utc)
    elapsed_min = (now - datetime.fromisoformat(picked_at)).total_seconds() / 60.0

    addresses = [p["token_address"] for p in picks if p.get("token_address")]
    market = _fetch_coinstats_market(addresses)

    rows = []
    for p in picks:
        addr = p["token_address"]
        mk = market.get(addr) or market.get(addr.lower(), {})
        entry = _safe_float(p.get("entry_close_price"))
        now_price = mk.get("price")
        change = None
        if entry and now_price and entry != 0:
            change = ((now_price - entry) / entry) * 100.0

        rows.append(
            {
                "rank": int(p["rank"]),
                "symbol": mk.get("symbol") or p.get("symbol"),
                "name": mk.get("name") or p.get("name"),
                "tokenAddress": p.get("token_address"),
                "entryPrice": entry,
                "nowPrice": now_price,
                "changePct": change,
            }
        )

    return {
        "pickedAt": picked_at,
        "now": now.isoformat(),
        "elapsedMinutes": elapsed_min,
        "rows": rows,
    }


@app.get("/api/performance")
def api_performance(limit: int = 100, labels: str = "strong_buy,buy,neutral,sell", verified_only: bool = True):
    """
    Performance endpoint backed by pick_outcomes table (all verified history)
    plus the live snapshot CSV for picks still too recent to be verified.
    """
    _OUTLIER_CAP = 500.0
    LONG_LABELS = {"buy", "strong_buy"}
    allowed_labels = {x.strip() for x in labels.split(",") if x.strip()}
    t = _load_thresholds()  # current score thresholds (0-100 scale)
    _sb, _buy, _neu = t["strong_buy"], t["buy"], t["neutral"]
    # SQL expression that normalises model_score to 0-100 regardless of storage scale
    _score_pct_sql = "(CASE WHEN model_score <= 1.0 THEN model_score * 100.0 ELSE model_score END)"
    # SQL CASE that converts normalised score to label using current thresholds
    _rec_sql = (
        f"CASE WHEN {_score_pct_sql} >= {_sb}  THEN 'strong_buy'"
        f"     WHEN {_score_pct_sql} >= {_buy} THEN 'buy'"
        f"     WHEN {_score_pct_sql} >= {_neu} THEN 'neutral'"
        f"     ELSE 'sell' END"
    )

    def _is_model_win(rec: str, eff_ret: float) -> bool:
        """Sell = win if price fell; all others = win if price rose."""
        return eff_ret <= 0 if rec == "sell" else eff_ret > 0

    def _capped(v: float) -> float:
        return min(max(v, -_OUTLIER_CAP), _OUTLIER_CAP)

    # ── 1. Pull verified picks from pick_outcomes ──────────────────────────
    verified_rows: list[dict] = []
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                # Most-recent pick per token from pick_outcomes, labelled correctly
                cur.execute(
                    """
                    SELECT
                        po.token_address,
                        COALESCE(t.symbol, po.token_address) AS symbol,
                        COALESCE(t.name, po.token_address)   AS name,
                        po.chain,
                        po.picked_at_utc,
                        po.bucket_timestamp,
                        po.model_score,
                        po.recommendation,
                        po.entry_price,
                        po.price_2h,
                        po.effective_return,
                        po.is_win,
                        (
                            SELECT close_price::DOUBLE PRECISION
                            FROM token_price_5m
                            WHERE token_address = po.token_address
                              AND bucket_timestamp >= po.bucket_timestamp + INTERVAL '5 minutes'
                            ORDER BY bucket_timestamp ASC
                            LIMIT 1
                        ) AS price_5m,
                        (
                            SELECT close_price::DOUBLE PRECISION
                            FROM token_price_5m
                            WHERE token_address = po.token_address
                              AND bucket_timestamp >= po.bucket_timestamp + INTERVAL '10 minutes'
                            ORDER BY bucket_timestamp ASC
                            LIMIT 1
                        ) AS price_10m
                    FROM (
                        SELECT DISTINCT ON (token_address)
                            token_address, chain, picked_at_utc, bucket_timestamp,
                            model_score, recommendation, entry_price, price_2h,
                            effective_return, is_win
                        FROM pick_outcomes
                        ORDER BY token_address, picked_at_utc DESC
                    ) po
                    LEFT JOIN tokens t ON t.token_address = po.token_address
                    ORDER BY po.picked_at_utc DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                db_rows = cur.fetchall()

            for r in db_rows:
                (
                    token_address, symbol, name, chain,
                    picked_at_utc, bucket_ts,
                    model_score, recommendation,
                    entry_price, price_2h, eff_ret, is_win_db,
                    price_5m, price_10m,
                ) = r

                score_pct = (float(model_score) * 100.0) if (model_score is not None and float(model_score) <= 1.0) else (float(model_score or 0))
                # Always re-derive label from current thresholds (stored value may be from old rank-based system)
                # Use pre-loaded _sb/_buy/_neu to avoid re-reading JSON per row
                recommendation = ("strong_buy" if score_pct >= _sb else "buy" if score_pct >= _buy else "neutral" if score_pct >= _neu else "sell")

                if allowed_labels and recommendation not in allowed_labels:
                    continue

                ep = float(entry_price) if entry_price is not None else None
                p2h = float(price_2h) if price_2h is not None else None
                er2h_raw = float(eff_ret) if eff_ret is not None else None
                p5 = float(price_5m) if price_5m is not None else None
                p10 = float(price_10m) if price_10m is not None else None
                r5_raw = ((p5 - ep) / ep * 100.0) if (p5 and ep) else None
                r10_raw = ((p10 - ep) / ep * 100.0) if (p10 and ep) else None

                # Cap displayed per-row returns at ±500%; flag real outliers
                is_outlier = (er2h_raw is not None and abs(er2h_raw) > _OUTLIER_CAP)
                er2h = _capped(er2h_raw) if er2h_raw is not None else None
                r5   = _capped(r5_raw)   if r5_raw   is not None else None
                r10  = _capped(r10_raw)  if r10_raw  is not None else None

                picked_at_str = picked_at_utc.isoformat() if hasattr(picked_at_utc, "isoformat") else str(picked_at_utc)

                verified_rows.append({
                    "symbol": symbol,
                    "name": name,
                    "chain": chain or "base",
                    "pickedAt": picked_at_str,
                    "recommendation": recommendation,
                    "scorePct": score_pct,
                    "pickedPrice": ep,
                    "price5m": p5,
                    "price10m": p10,
                    "price2h": p2h,
                    "futurePoints": 24,  # historical; actual bar count not critical
                    "return5m": r5,
                    "return10m": r10,
                    "return2h": er2h,
                    "effectiveReturn5m": r5,
                    "effectiveReturn10m": r10,
                    "effectiveReturn2h": er2h,
                    "isOutlier": is_outlier,
                    "verified": True,
                })

    except Exception as exc:
        # DB unavailable — fall through to CSV-only mode
        import traceback; traceback.print_exc()

    # ── 2. Recent unverified picks from CSV (< 2.5h old) ─────────────────
    recent_csv_rows: list[dict] = []
    if SNAPSHOT_PATH.exists():
        cutoff_ts = datetime.now(timezone.utc) - timedelta(hours=2.5)
        with SNAPSHOT_PATH.open("r", encoding="utf-8", newline="") as f:
            csv_picks = list(csv.DictReader(f))

        from collections import defaultdict as _dd
        _cycle_rows: dict[str, list[dict]] = _dd(list)
        for r in csv_picks:
            _cycle_rows[r.get("picked_at_utc", "")].append(r)
        _rec_cache: dict[tuple[str, str], str] = {}
        for cycle_ts, cycle_picks in _cycle_rows.items():
            _spcts = [
                (_safe_float(p.get("score")) or 0.0) * 100.0
                if (_safe_float(p.get("score")) or 0.0) <= 1.0
                else (_safe_float(p.get("score")) or 0.0)
                for p in cycle_picks
            ]
            _recs = _recommendations_from_scores(_spcts)
            for p, label in zip(cycle_picks, _recs):
                _rec_cache[(p["token_address"], cycle_ts)] = label

        seen_csv: set[str] = set()
        for r in sorted(csv_picks, key=lambda x: x.get("picked_at_utc", ""), reverse=True):
            token = r.get("token_address", "")
            if not token or token in seen_csv:
                continue
            try:
                picked_at = datetime.fromisoformat(r["picked_at_utc"])
                if picked_at.tzinfo is None:
                    picked_at = picked_at.replace(tzinfo=timezone.utc)
            except Exception:
                continue
            # Only include picks too recent to be in pick_outcomes
            if picked_at <= cutoff_ts:
                continue

            score_raw = _safe_float(r.get("score"))
            score_pct = (score_raw * 100.0) if (score_raw is not None and score_raw <= 1.0) else (score_raw or 0.0)
            recommendation = _rec_cache.get((token, r.get("picked_at_utc", "")), _score_to_recommendation(score_pct))

            if allowed_labels and recommendation not in allowed_labels:
                continue

            ep = _safe_float(r.get("entry_close_price"))
            if not ep or ep == 0:
                continue

            picked_bucket = datetime.fromisoformat(r["bucket_timestamp"])
            p5 = p10 = p2h = None
            r5 = r10 = r2h = None
            try:
                with _conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            SELECT
                                (SELECT close_price::DOUBLE PRECISION FROM token_price_5m
                                  WHERE token_address = %s AND bucket_timestamp >= (%s::timestamptz + INTERVAL '5 minutes')
                                  ORDER BY bucket_timestamp ASC LIMIT 1),
                                (SELECT close_price::DOUBLE PRECISION FROM token_price_5m
                                  WHERE token_address = %s AND bucket_timestamp >= (%s::timestamptz + INTERVAL '10 minutes')
                                  ORDER BY bucket_timestamp ASC LIMIT 1),
                                (SELECT close_price::DOUBLE PRECISION FROM token_price_5m
                                  WHERE token_address = %s AND bucket_timestamp >= (%s::timestamptz + INTERVAL '2 hours')
                                  ORDER BY bucket_timestamp ASC LIMIT 1)
                            """,
                            (token, picked_bucket, token, picked_bucket, token, picked_bucket),
                        )
                        rec = cur.fetchone()
                if rec:
                    p5 = float(rec[0]) if rec[0] is not None else None
                    p10 = float(rec[1]) if rec[1] is not None else None
                    p2h = float(rec[2]) if rec[2] is not None else None
                    if p5: r5 = (p5 - ep) / ep * 100.0
                    if p10: r10 = (p10 - ep) / ep * 100.0
                    if p2h: r2h = (p2h - ep) / ep * 100.0
            except Exception:
                pass

            if verified_only and p2h is None:
                continue

            seen_csv.add(token)
            recent_csv_rows.append({
                "symbol": r.get("symbol"),
                "name": r.get("name"),
                "chain": r.get("chain", "base"),
                "pickedAt": r.get("picked_at_utc"),
                "recommendation": recommendation,
                "scorePct": score_pct,
                "pickedPrice": ep,
                "price5m": p5, "price10m": p10, "price2h": p2h,
                "futurePoints": 0,
                "return5m": _capped(r5) if r5 is not None else None,
                "return10m": _capped(r10) if r10 is not None else None,
                "return2h": _capped(r2h) if r2h is not None else None,
                "effectiveReturn5m": _capped(r5) if r5 is not None else None,
                "effectiveReturn10m": _capped(r10) if r10 is not None else None,
                "effectiveReturn2h": _capped(r2h) if r2h is not None else None,
                "isOutlier": (r2h is not None and abs(r2h) > _OUTLIER_CAP),
                "verified": r2h is not None,
            })

    # ── 3. Merge: CSV recent on top, then verified DB rows ─────────────────
    # Deduplicate: CSV takes precedence for tokens appearing in both
    csv_tokens = {r["symbol"] for r in recent_csv_rows}
    perf_rows = recent_csv_rows + [r for r in verified_rows if r["symbol"] not in csv_tokens]
    perf_rows = perf_rows[:limit]

    # ── 4. Aggregate stats from the FULL pick_outcomes table (not just limit) ──
    total = 0
    win_rate = 0.0
    avg_ret = 0.0
    chain_breakdown: dict = {}
    rec_breakdown: dict = {}
    cumulative: list = []

    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                # Overall (re-derive recommendation from model_score using current thresholds)
                cur.execute(
                    f"""
                    SELECT
                        COUNT(*),
                        SUM(CASE WHEN ({_rec_sql} = 'sell' AND effective_return <= 0)
                                   OR ({_rec_sql} != 'sell' AND effective_return > 0)
                                 THEN 1 ELSE 0 END),
                        AVG(LEAST(GREATEST(effective_return, %s), %s))
                            FILTER (WHERE {_rec_sql} IN ('buy','strong_buy'))
                    FROM pick_outcomes
                    """,
                    (-_OUTLIER_CAP, _OUTLIER_CAP),
                )
                row = cur.fetchone()
                total = int(row[0] or 0)
                wins_all = int(row[1] or 0)
                avg_ret = float(row[2] or 0.0) if row[2] is not None else 0.0
                win_rate = (wins_all / total * 100.0) if total else 0.0

                # Chain breakdown (re-derive recommendation from model_score using current thresholds)
                cur.execute(
                    f"""
                    SELECT
                        chain,
                        COUNT(*) AS n,
                        SUM(CASE WHEN ({_rec_sql} = 'sell' AND effective_return<=0)
                                   OR ({_rec_sql} != 'sell' AND effective_return>0)
                                 THEN 1 ELSE 0 END) AS wins,
                        AVG(LEAST(GREATEST(effective_return, %s), %s))
                            FILTER (WHERE {_rec_sql} IN ('buy','strong_buy')) AS avg_ret,
                        MAX(effective_return) FILTER (WHERE {_rec_sql} IN ('buy','strong_buy')) AS best,
                        MIN(effective_return) FILTER (WHERE {_rec_sql} IN ('buy','strong_buy')) AS worst
                    FROM pick_outcomes
                    GROUP BY chain ORDER BY n DESC
                    """,
                    (-_OUTLIER_CAP, _OUTLIER_CAP),
                )
                for r in cur.fetchall():
                    n = int(r[1] or 0)
                    w = int(r[2] or 0)
                    chain_breakdown[r[0]] = {
                        "total": n,
                        "wins": w,
                        "winRate": (w / n * 100.0) if n else 0.0,
                        "avgReturn": float(r[3] or 0.0),
                        "bestReturn": float(r[4] or 0.0),
                        "worstReturn": float(r[5] or 0.0),
                        "outliers": 0,
                    }

                # Recommendation breakdown (group by score-derived label, not stored label)
                cur.execute(
                    f"""
                    SELECT
                        {_rec_sql} AS rec,
                        COUNT(*) AS n,
                        SUM(CASE WHEN ({_rec_sql} = 'sell' AND effective_return<=0)
                                   OR ({_rec_sql} != 'sell' AND effective_return>0)
                                 THEN 1 ELSE 0 END) AS wins,
                        AVG(LEAST(GREATEST(effective_return, %s), %s)) AS avg_ret,
                        MAX(effective_return) AS best,
                        MIN(effective_return) AS worst
                    FROM pick_outcomes
                    GROUP BY {_rec_sql}
                    """,
                    (-_OUTLIER_CAP, _OUTLIER_CAP),
                )
                for r in cur.fetchall():
                    n = int(r[1] or 0)
                    w = int(r[2] or 0)
                    rec_breakdown[r[0]] = {
                        "total": n,
                        "wins": w,
                        "winRate": (w / n * 100.0) if n else 0.0,
                        "avgReturn": float(r[3] or 0.0),
                        "bestReturn": float(r[4] or 0.0),
                        "worstReturn": float(r[5] or 0.0),
                        "outliers": 0,
                    }

                # Cumulative return (score-derived buy+strong_buy, chronological)
                cur.execute(
                    f"""
                    SELECT picked_at_utc, LEAST(GREATEST(effective_return, %s), %s)
                    FROM pick_outcomes
                    WHERE {_rec_sql} IN ('buy','strong_buy')
                    ORDER BY picked_at_utc ASC
                    """,
                    (-_OUTLIER_CAP, _OUTLIER_CAP),
                )
                equity = 100.0
                for r in cur.fetchall():
                    ret_pct = float(r[1] or 0.0)
                    # Guard against invalid <-100% rows from dirty price data.
                    ret_pct = max(ret_pct, -95.0)
                    equity *= (1.0 + ret_pct / 100.0)
                    date_label = r[0].strftime("%b %d") if r[0] else ""
                    cumulative.append({"date": date_label, "cumReturn": equity - 100.0})

    except Exception:
        import traceback; traceback.print_exc()

    return {
        "rows": perf_rows,
        "cumulative": cumulative,
        "summary": {
            "winRate": win_rate,
            "avgReturn2h": avg_ret,
            "total": total,
        },
        "chainBreakdown": chain_breakdown,
        "recBreakdown": rec_breakdown,
    }


IMPORTANCE_PATH = PROJECT_ROOT / "research" / "feature_importance.json"


@app.get("/api/feature-importance")
def api_feature_importance():
    if not IMPORTANCE_PATH.exists():
        return {"features": {}, "timestamp": None, "model": None, "featureSet": None, "trainRows": 0, "scoringRows": 0}

    with IMPORTANCE_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/api/thresholds")
def api_thresholds():
    """Return the active score thresholds (calibrated or hardcoded defaults)."""
    defaults = _DEFAULT_THRESHOLDS   # in 0-100 scale
    if not THRESHOLDS_PATH.exists():
        return {
            "strongBuy":    defaults["strong_buy"],
            "buy":          defaults["buy"],
            "neutral":      defaults["neutral"],
            "calibrated":   False,
            "sampleSize":   0,
            "calibratedAt": None,
        }
    try:
        with open(THRESHOLDS_PATH, "r", encoding="utf-8") as f:
            d = json.load(f)
        return {
            "strongBuy":    round(float(d.get("strong_buy", defaults["strong_buy"] / 100)) * 100, 1),
            "buy":          round(float(d.get("buy",        defaults["buy"]        / 100)) * 100, 1),
            "neutral":      round(float(d.get("neutral",    defaults["neutral"]    / 100)) * 100, 1),
            "calibrated":   bool(d.get("calibrated", False)),
            "sampleSize":   int(d.get("sample_size", 0)),
            "calibratedAt": d.get("calibrated_at"),
        }
    except Exception:
        return {
            "strongBuy":    defaults["strong_buy"],
            "buy":          defaults["buy"],
            "neutral":      defaults["neutral"],
            "calibrated":   False,
            "sampleSize":   0,
            "calibratedAt": None,
        }


def api_settings():
    def mask(v: str | None) -> str:
        if not v:
            return ""
        if len(v) <= 8:
            return "*" * len(v)
        return v[:3] + "*" * (len(v) - 6) + v[-3:]

    return {
        "env": [
            {"key": "PGHOST", "value": os.getenv("PGHOST", ""), "masked": False},
            {"key": "PGDATABASE", "value": os.getenv("PGDATABASE", ""), "masked": False},
            {"key": "PGUSER", "value": os.getenv("PGUSER", ""), "masked": False},
            {"key": "PGPASSWORD", "value": mask(os.getenv("PGPASSWORD", "")), "masked": True},
            {"key": "COINSTATS_API_KEY", "value": mask(os.getenv("COINSTATS_API_KEY", "")), "masked": True},
            {"key": "COINSTATS_BLOCKCHAINS", "value": os.getenv("COINSTATS_BLOCKCHAINS", ""), "masked": False},
            {"key": "PRIMARY_DATA_SOURCE", "value": os.getenv("PRIMARY_DATA_SOURCE", "gecko"), "masked": False},
        ]
    }


@app.post("/api/run-cycle")
def api_run_cycle(req: RunCycleRequest):
    script = PROJECT_ROOT / "runlive.ps1"
    if not script.exists():
        raise HTTPException(status_code=404, detail="runlive.ps1 not found")

    global _cycle_status
    with _cycle_lock:
        if _cycle_status["running"]:
            return {"ok": False, "error": "A cycle is already running", "alreadyRunning": True}
        _cycle_status = {
            "running": True,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "ok": None,
            "code": None,
            "output_lines": [],
            "error": None,
        }

    thread = threading.Thread(target=_run_cycle_background, args=(req,), daemon=True)
    thread.start()
    return {"ok": True, "started": True}


@app.get("/api/cycle-status")
def api_cycle_status(since_line: int = 0):
    """Poll for cycle progress. Returns new log lines since `since_line`."""
    with _cycle_lock:
        all_lines = _cycle_status["output_lines"]
        new_lines = all_lines[since_line:]
        return {
            "running": _cycle_status["running"],
            "startedAt": _cycle_status["started_at"],
            "finishedAt": _cycle_status["finished_at"],
            "ok": _cycle_status["ok"],
            "code": _cycle_status["code"],
            "error": _cycle_status["error"],
            "totalLines": len(all_lines),
            "newLines": new_lines,
        }


# ---------------------------------------------------------------------------
# Meme Radar
# ---------------------------------------------------------------------------
_meme_cache: dict[str, Any] = {"data": None, "ts": 0.0, "refreshing": False}
_MEME_CACHE_TTL = 900  # 15 minutes


def _refresh_meme_cache_bg() -> None:
    """Run meme radar in background thread and update cache."""
    import time as _time
    if _meme_cache["refreshing"]:
        return  # already running
    _meme_cache["refreshing"] = True
    try:
        try:
            from backend.meme_radar import run_meme_radar
        except ImportError:
            import sys
            sys.path.insert(0, str(Path(__file__).resolve().parent))
            import meme_radar as _mr
            run_meme_radar = _mr.run_meme_radar
        result = run_meme_radar(
            limit_per_sub=10,
            min_virality=10.0,
            max_results=20,
            search_coins=True,
        )
        _meme_cache["data"] = result
        _meme_cache["ts"] = _time.time()
    except Exception as exc:
        print(f"[MEME-RADAR] Background refresh failed: {exc}")
    finally:
        _meme_cache["refreshing"] = False


@app.get("/api/meme-radar")
def api_meme_radar(refresh: bool = False):
    """Return trending memes + related crypto tokens.

    Results are cached for 15 minutes. The endpoint always returns instantly:
    - If cache is fresh → return it immediately.
    - If cache is stale / empty → trigger background refresh and return
      stale cache (or a loading placeholder if no cache yet).
    Pass ?refresh=true to force a new background fetch.
    """
    import time as _time
    import threading
    now = _time.time()
    cache_fresh = _meme_cache["data"] and (now - _meme_cache["ts"]) < _MEME_CACHE_TTL

    if not cache_fresh or refresh:
        # Kick off background refresh (no-op if already running)
        t = threading.Thread(target=_refresh_meme_cache_bg, daemon=True)
        t.start()

    if _meme_cache["data"]:
        # Return cached data immediately (may be slightly stale during refresh)
        data = dict(_meme_cache["data"])
        data["refreshing"] = _meme_cache["refreshing"]
        data["cacheAge"] = int(now - _meme_cache["ts"]) if _meme_cache["ts"] else None
        return data

    # No cache yet — return loading placeholder so frontend can show spinner
    return {
        "timestamp": None,
        "totalScanned": 0,
        "totalViable": 0,
        "memesSearched": 0,
        "results": [],
        "refreshing": True,
        "cacheAge": None,
        "loading": True,
    }
