from __future__ import annotations

import csv
import json
import os
import subprocess
import threading
import urllib.parse
import urllib.request
from datetime import datetime, timezone
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


def _recommendations_from_scores(score_pcts: list[float]) -> list[str]:
    """Assign recommendations by percentile rank within the batch.

    Model-agnostic: works regardless of score scale (ensemble probabilities
    cluster 35-60%, stacking meta-learner probabilities cluster 15-28%, etc.).

    Within the batch:
      Top  20% of scores → "strong_buy"
      Next 20%           → "buy"
      Next 20%           → "neutral"
      Bottom 40%         → "sell"

    This guarantees a sensible label distribution regardless of which model
    produced the scores or what probability scale it uses.
    """
    if not score_pcts:
        return []
    n = len(score_pcts)
    if n == 1:
        return ["neutral"]

    # Rank: 0 = highest score, n-1 = lowest
    import numpy as _np
    arr = _np.array(score_pcts)
    # fractional rank relative to batch (0.0 = top, 1.0 = bottom)
    order = arr.argsort()[::-1]  # indices sorted best→worst
    rank_pct = _np.empty(n)
    rank_pct[order] = _np.linspace(0.0, 1.0, n)

    result: list[str] = []
    for rp in rank_pct:
        if rp < 0.20:
            result.append("strong_buy")
        elif rp < 0.40:
            result.append("buy")
        elif rp < 0.60:
            result.append("neutral")
        else:
            result.append("sell")
    return result


def _score_to_recommendation(score_pct: float) -> str:
    """Legacy single-score thresholds — ONLY used as fallback for single-item queries.

    For any batch context use _recommendations_from_scores() instead, which is
    rank-based and model-agnostic.
    """
    if score_pct >= 55:
        return "strong_buy"
    if score_pct >= 45:
        return "buy"
    if score_pct >= 35:
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


def _read_latest_snapshot(top_n: int | None = None) -> tuple[str, list[dict[str, str]]]:
    if not SNAPSHOT_PATH.exists():
        raise FileNotFoundError("live_picks_snapshot.csv not found")

    with SNAPSHOT_PATH.open("r", encoding="utf-8", newline="") as f:
        rows = [_normalize_snapshot_row(r) for r in csv.DictReader(f)]

    if not rows:
        raise ValueError("live_picks_snapshot.csv is empty")

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
            "winRate": None,
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
    if not SNAPSHOT_PATH.exists():
        return {"rows": [], "cumulative": [], "summary": {"winRate": 0, "avgReturn2h": 0, "total": 0}}

    with SNAPSHOT_PATH.open("r", encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    rows = sorted(rows, key=lambda r: r["picked_at_utc"], reverse=True)

    # Pre-compute rank-based recommendations grouped by cycle (picked_at_utc).
    # This is model-agnostic: top 20% → strong_buy, next 20% → buy, etc.,
    # regardless of whether scores cluster at 15-30% (stacking) or 35-60% (ensemble).
    from collections import defaultdict as _dd
    _cycle_rows: dict[str, list[dict]] = _dd(list)
    for r in rows:
        _cycle_rows[r["picked_at_utc"]].append(r)

    _rec_cache: dict[tuple[str, str], str] = {}  # (token_address, picked_at_utc) → label
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

    allowed_labels = {x.strip() for x in labels.split(",") if x.strip()}
    perf_rows = []
    seen_selected_tokens: set[str] = set()
    for r in rows:
        token = r["token_address"]
        if token in seen_selected_tokens:
            continue

        picked_bucket = datetime.fromisoformat(r["bucket_timestamp"])
        picked_price = _safe_float(r.get("entry_close_price"))
        score_raw = _safe_float(r.get("score"))
        score_pct = (score_raw * 100.0) if (score_raw is not None and score_raw <= 1.0) else (score_raw or 0.0)
        recommendation = _rec_cache.get((token, r["picked_at_utc"]), _score_to_recommendation(score_pct))

        if allowed_labels and recommendation not in allowed_labels:
            continue

        if not picked_price or picked_price == 0:
            continue

        p5 = p10 = p2h = None
        future_points = 0
        r5 = r10 = r2h = None
        try:
            with _conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            (
                                SELECT close_price::DOUBLE PRECISION
                                FROM token_price_5m
                                WHERE token_address = %s
                                  AND bucket_timestamp >= (%s::timestamptz + INTERVAL '5 minutes')
                                ORDER BY bucket_timestamp ASC
                                LIMIT 1
                            ) AS price_5m,
                            (
                                SELECT close_price::DOUBLE PRECISION
                                FROM token_price_5m
                                WHERE token_address = %s
                                  AND bucket_timestamp >= (%s::timestamptz + INTERVAL '10 minutes')
                                ORDER BY bucket_timestamp ASC
                                LIMIT 1
                            ) AS price_10m,
                            (
                                SELECT close_price::DOUBLE PRECISION
                                FROM token_price_5m
                                WHERE token_address = %s
                                  AND bucket_timestamp >= (%s::timestamptz + INTERVAL '2 hours')
                                ORDER BY bucket_timestamp ASC
                                LIMIT 1
                            ) AS price_2h,
                            (
                                SELECT COUNT(*)::INT
                                FROM token_price_5m
                                WHERE token_address = %s
                                  AND bucket_timestamp > %s::timestamptz
                                  AND bucket_timestamp <= (%s::timestamptz + INTERVAL '2 hours')
                            ) AS points_until_2h
                        """,
                        (
                            token,
                            picked_bucket,
                            token,
                            picked_bucket,
                            token,
                            picked_bucket,
                            token,
                            picked_bucket,
                            picked_bucket,
                        ),
                    )
                    rec = cur.fetchone()

            if rec:
                p5 = float(rec[0]) if rec[0] is not None else None
                p10 = float(rec[1]) if rec[1] is not None else None
                p2h = float(rec[2]) if rec[2] is not None else None
                future_points = int(rec[3] or 0)

            if p5 is not None:
                r5 = ((p5 - picked_price) / picked_price) * 100.0
            if p10 is not None:
                r10 = ((p10 - picked_price) / picked_price) * 100.0
            if p2h is not None:
                r2h = ((p2h - picked_price) / picked_price) * 100.0
        except Exception:
            pass

        # All picks tracked as long positions — no sell inversion
        er5, er10, er2h = r5, r10, r2h

        if verified_only and p2h is None:
            continue

        seen_selected_tokens.add(token)

        perf_rows.append(
            {
                "symbol": r.get("symbol"),
                "name": r.get("name"),
                "chain": r.get("chain", "base"),
                "pickedAt": r.get("picked_at_utc"),
                "recommendation": recommendation,
                "scorePct": score_pct,
                "pickedPrice": picked_price,
                "price5m": p5,
                "price10m": p10,
                "price2h": p2h,
                "futurePoints": future_points,
                "return5m": r5,
                "return10m": r10,
                "return2h": r2h,
                "effectiveReturn5m": er5,
                "effectiveReturn10m": er10,
                "effectiveReturn2h": er2h,
                "verified": r2h is not None,
            }
        )

        if len(perf_rows) >= max(1, limit):
            break

    valid_2h = [p for p in perf_rows if p["effectiveReturn2h"] is not None]
    total = len(valid_2h)

    def _is_model_win(p: dict) -> bool:
        """Model win = directional call was correct.
        Sell = model predicted no gain → win if price fell (ret ≤ 0).
        All other labels = model predicted gain → win if price rose (ret > 0)."""
        ret = p["effectiveReturn2h"]
        return ret <= 0 if p["recommendation"] == "sell" else ret > 0

    win_rate = (sum(1 for p in valid_2h if _is_model_win(p)) / total * 100.0) if total else 0.0

    # Cumulative return = only picks you'd actually enter (buy / strong_buy).
    # Sell picks are avoidance signals — no position is opened, so they don't
    # contribute to portfolio return.  Neutral is also excluded (no clear entry).
    LONG_LABELS = {"buy", "strong_buy"}
    traded = [p for p in valid_2h if p["recommendation"] in LONG_LABELS]
    # Cap extreme returns at ±500% for avg to prevent data anomalies (e.g. 10x pumps) from dominating
    _OUTLIER_CAP = 500.0
    avg_ret = (sum(min(max(p["effectiveReturn2h"], -_OUTLIER_CAP), _OUTLIER_CAP) for p in traded) / len(traded)) if traded else 0.0

    cumulative = []
    running = 0.0
    for p in sorted(traded, key=lambda x: x["pickedAt"]):
        running += p["effectiveReturn2h"]
        date_label = datetime.fromisoformat(p["pickedAt"]).strftime("%b %d")
        cumulative.append({"date": date_label, "cumReturn": running})

    # ── Per-chain & per-recommendation breakdown ──
    chain_map: dict[str, list[dict]] = {}
    rec_map: dict[str, list[dict]] = {}
    for p in valid_2h:
        c = p.get("chain", "base")
        chain_map.setdefault(c, []).append(p)
        rec_map.setdefault(p["recommendation"], []).append(p)

    _OUTLIER_CAP = 500.0

    def _stats(items: list[dict], long_only_avg: bool = False) -> dict:
        n = len(items)
        wins = sum(1 for x in items if _is_model_win(x))
        # For chain breakdown: avg/best/worst only counts buy/strong_buy positions (sell = avoidance, no position taken).
        # For rec breakdown: avg/best/worst is all picks in that group.
        perf_items = [x for x in items if x["recommendation"] in LONG_LABELS] if long_only_avg else items
        avg = (
            sum(min(max(x["effectiveReturn2h"], -_OUTLIER_CAP), _OUTLIER_CAP) for x in perf_items) / len(perf_items)
            if perf_items else 0.0
        )
        best = max((x["effectiveReturn2h"] for x in perf_items), default=0.0)
        worst = min((x["effectiveReturn2h"] for x in perf_items), default=0.0)
        outliers = sum(1 for x in perf_items if abs(x["effectiveReturn2h"]) > _OUTLIER_CAP)
        return {"total": n, "wins": wins, "winRate": (wins / n * 100.0) if n else 0.0, "avgReturn": avg, "bestReturn": best, "worstReturn": worst, "outliers": outliers}

    # Chain avg = only positions taken (buy/strong_buy); sell picks are avoidance signals, no position opened
    chain_breakdown = {c: _stats(items, long_only_avg=True) for c, items in sorted(chain_map.items())}
    rec_breakdown = {r: _stats(items) for r, items in sorted(rec_map.items())}

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


@app.get("/api/settings")
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
