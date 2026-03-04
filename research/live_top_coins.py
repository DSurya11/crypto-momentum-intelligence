from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from getpass import getpass

# Force UTF-8 output on Windows (avoids cp1252 UnicodeEncodeError for non-ASCII coin names)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import numpy as np
import psycopg
from dotenv import load_dotenv

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


def get_conn() -> psycopg.Connection:
    return psycopg.connect(
        host=get_env("PGHOST"),
        port=int(get_env("PGPORT", "5432")),
        dbname=get_env("PGDATABASE"),
        user=get_env("PGUSER"),
        password=get_db_password(),
        sslmode=get_env("PGSSLMODE", "disable"),
    )


def latest_bucket(conn: psycopg.Connection) -> datetime:
    with conn.cursor() as cursor:
        cursor.execute("SELECT MAX(bucket_timestamp) FROM features_5m")
        row = cursor.fetchone()
    if not row or row[0] is None:
        raise ValueError("No features_5m rows found")
    return row[0]


def load_training_data(conn: psycopg.Connection, feature_set: str, label_target: str, before_bucket: datetime):
    feature_names = FEATURE_SETS[feature_set]
    label_sql = LABEL_TARGETS[label_target]
    feature_sql = ", ".join([f"f.{name}::DOUBLE PRECISION" for name in feature_names])

    sql = f"""
        SELECT {feature_sql}, {label_sql},
               f.token_address, f.bucket_timestamp
        FROM features_5m f
        INNER JOIN labels_5m l
          ON f.token_address = l.token_address
         AND f.bucket_timestamp = l.bucket_timestamp
        WHERE {label_sql} IN (0,1)
          AND f.bucket_timestamp < %s
        ORDER BY f.bucket_timestamp ASC, f.token_address ASC
    """

    with conn.cursor() as cursor:
        cursor.execute(sql, (before_bucket,))
        rows = cursor.fetchall()

    if len(rows) < 500:
        raise ValueError(f"Not enough training rows before latest bucket: {len(rows)}")

    n = len(feature_names)
    x = np.asarray([r[:n] for r in rows], dtype=np.float64)
    y = np.asarray([r[n] for r in rows], dtype=np.int32)
    train_addrs = [r[n + 1] for r in rows]
    train_buckets = [r[n + 2] for r in rows]
    return x, y, feature_names, train_addrs, train_buckets


def load_scoring_rows(conn: psycopg.Connection, feature_set: str, bucket: datetime):
    feature_names = FEATURE_SETS[feature_set]
    feature_sql = ", ".join([f"f.{name}::DOUBLE PRECISION" for name in feature_names])

    sql = f"""
        SELECT
            f.token_address,
            t.symbol,
            t.name,
            t.chain,
            f.bucket_timestamp,
            tp.close_price::DOUBLE PRECISION AS close_price,
            {feature_sql}
        FROM features_5m f
        INNER JOIN tokens t
            ON t.token_address = f.token_address
        LEFT JOIN token_price_5m tp
            ON tp.token_address = f.token_address
           AND tp.bucket_timestamp = f.bucket_timestamp
        WHERE f.bucket_timestamp = %s
        ORDER BY t.symbol ASC
    """

    with conn.cursor() as cursor:
        cursor.execute(sql, (bucket,))
        rows = cursor.fetchall()

    if not rows:
        raise ValueError(f"No scoring rows for bucket {bucket}")

    meta = []
    feats = []
    for r in rows:
        token_address, symbol, name, chain, bucket_ts, close_price, *feature_vals = r
        meta.append(
            {
                "token_address": token_address,
                "symbol": symbol,
                "name": name,
                "chain": chain or "base",
                "bucket_timestamp": bucket_ts,
                "close_price": float(close_price) if close_price is not None else float("nan"),
            }
        )
        feats.append(feature_vals)

    x_score = np.asarray(feats, dtype=np.float64)
    return meta, x_score, feature_names


def score_live(
    model_type: str,
    x_train: np.ndarray,
    y_train: np.ndarray,
    x_score: np.ndarray,
    robust: bool,
    feature_names: list[str],
    sample_weights: np.ndarray | None = None,
):
    if robust:
        x_train_pp, x_score_pp, _ = robust_preprocess(x_train, x_score, feature_names=feature_names)
    else:
        x_train_pp, x_score_pp = x_train, x_score

    tuned_params = {}
    importances: dict[str, float] = {}

    # XGBoost natively supports sample_weight in .fit()
    # LogisticRegression in a Pipeline: pass as clf__sample_weight
    lr_fit_kw: dict = {}
    xgb_fit_kw: dict = {}
    if sample_weights is not None:
        lr_fit_kw["clf__sample_weight"] = sample_weights
        xgb_fit_kw["sample_weight"] = sample_weights

    if model_type == "logistic":
        lr = make_logistic()
        lr.fit(x_train_pp, y_train, **lr_fit_kw)
        prob = lr.predict_proba(x_score_pp)[:, 1]
        # Logistic coefficients as importance (absolute value normalised)
        coefs = np.abs(lr.named_steps["clf"].coef_[0]) if hasattr(lr, "named_steps") else np.abs(lr.coef_[0])
        total = coefs.sum() or 1.0
        importances = {fn: round(float(c / total * 100), 2) for fn, c in zip(feature_names, coefs)}

    elif model_type == "xgboost_tuned":
        tuned_params = tune_xgboost(x_train_pp, y_train, top_frac=0.10)
        xgb = make_xgboost(y_train, **tuned_params)
        xgb.fit(x_train_pp, y_train, **xgb_fit_kw)
        prob = xgb.predict_proba(x_score_pp)[:, 1]
        raw = xgb.feature_importances_
        total = raw.sum() or 1.0
        importances = {fn: round(float(v / total * 100), 2) for fn, v in zip(feature_names, raw)}

    elif model_type == "ensemble":
        lr = make_logistic()
        lr.fit(x_train_pp, y_train, **lr_fit_kw)
        lr_prob = lr.predict_proba(x_score_pp)[:, 1]
        lr_coefs = np.abs(lr.named_steps["clf"].coef_[0]) if hasattr(lr, "named_steps") else np.abs(lr.coef_[0])

        tuned_params = tune_xgboost(x_train_pp, y_train, top_frac=0.10)
        xgb = make_xgboost(y_train, **tuned_params)
        xgb.fit(x_train_pp, y_train, **xgb_fit_kw)
        xgb_prob = xgb.predict_proba(x_score_pp)[:, 1]
        xgb_raw = xgb.feature_importances_

        prob = 0.4 * lr_prob + 0.6 * xgb_prob

        # Blend importances: 40% logistic + 60% xgboost (normalised)
        lr_total = lr_coefs.sum() or 1.0
        xgb_total = xgb_raw.sum() or 1.0
        blended = 0.4 * (lr_coefs / lr_total) + 0.6 * (xgb_raw / xgb_total)
        importances = {fn: round(float(v * 100), 2) for fn, v in zip(feature_names, blended)}

    elif model_type == "stacking":
        # ── Level-0: time-aware OOF predictions from 4 diverse base learners ──
        # Learners: Logistic Regression, XGBoost, Random Forest, Extra Trees
        # Uses expanding-window TimeSeriesSplit to prevent look-ahead bias.
        from sklearn.linear_model import LogisticRegression as MetaLR

        print("[STACKING] Generating out-of-fold meta-features (5-fold time-split)...")
        oof_preds, has_all = stacking_oof_predictions(
            x_train_pp, y_train, n_folds=5, sample_weights=sample_weights
        )
        oof_coverage = int(has_all.sum())
        print(f"[STACKING] OOF coverage: {oof_coverage}/{len(y_train)} rows")

        # ── Level-1: meta-learner trained on OOF rows ──
        meta_x = oof_preds[has_all]
        meta_y = y_train[has_all]
        meta_sw = sample_weights[has_all] if sample_weights is not None else None

        meta = MetaLR(max_iter=1000, solver="lbfgs", random_state=42)
        if len(meta_x) < 20 or len(np.unique(meta_y)) < 2:
            # Fallback to weighted average if not enough OOF rows
            print("[STACKING] Insufficient OOF data — falling back to ensemble average")
            lr_fb = make_logistic()
            lr_fb.fit(x_train_pp, y_train, **lr_fit_kw)
            xgb_fb = make_xgboost(y_train)
            xgb_fb.fit(x_train_pp, y_train, **xgb_fit_kw)
            prob = 0.4 * lr_fb.predict_proba(x_score_pp)[:, 1] + 0.6 * xgb_fb.predict_proba(x_score_pp)[:, 1]
            lr_c = np.abs(lr_fb.named_steps["clf"].coef_[0])
            xgb_r = xgb_fb.feature_importances_
            blended = 0.4 * (lr_c / (lr_c.sum() or 1)) + 0.6 * (xgb_r / (xgb_r.sum() or 1))
            importances = {fn: round(float(v * 100), 2) for fn, v in zip(feature_names, blended)}
        else:
            if meta_sw is not None:
                meta.fit(meta_x, meta_y, sample_weight=meta_sw)
            else:
                meta.fit(meta_x, meta_y)

            # ── Retrain all 4 base learners on full training data ──
            print("[STACKING] Training final base learners on full dataset...")
            tuned_params = tune_xgboost(x_train_pp, y_train, top_frac=0.10)

            lr_f = make_logistic()
            xgb_f = make_xgboost(y_train, **tuned_params)
            rf_f = make_random_forest(y_train)
            et_f = make_extratrees(y_train)

            if sample_weights is not None:
                lr_f.fit(x_train_pp, y_train, clf__sample_weight=sample_weights)
                xgb_f.fit(x_train_pp, y_train, sample_weight=sample_weights)
                rf_f.fit(x_train_pp, y_train, sample_weight=sample_weights)
                et_f.fit(x_train_pp, y_train, sample_weight=sample_weights)
            else:
                lr_f.fit(x_train_pp, y_train)
                xgb_f.fit(x_train_pp, y_train)
                rf_f.fit(x_train_pp, y_train)
                et_f.fit(x_train_pp, y_train)

            # ── Score via base learners → meta-learner ──
            base_score = np.column_stack([
                lr_f.predict_proba(x_score_pp)[:, 1],
                xgb_f.predict_proba(x_score_pp)[:, 1],
                rf_f.predict_proba(x_score_pp)[:, 1],
                et_f.predict_proba(x_score_pp)[:, 1],
            ])
            prob = meta.predict_proba(base_score)[:, 1]

            # ── Feature importances: meta-weight × base-learner importance ──
            meta_w = np.abs(meta.coef_[0])  # [lr, xgb, rf, et]
            meta_w = meta_w / (meta_w.sum() or 1.0)
            print(f"[STACKING] Meta-weights: LR={meta_w[0]:.3f} XGB={meta_w[1]:.3f} RF={meta_w[2]:.3f} ET={meta_w[3]:.3f}")

            lr_coefs = np.abs(lr_f.named_steps["clf"].coef_[0])
            xgb_raw = xgb_f.feature_importances_
            rf_raw = rf_f.feature_importances_
            et_raw = et_f.feature_importances_

            def _norm(arr: np.ndarray) -> np.ndarray:
                s = arr.sum()
                return arr / s if s > 0 else arr

            blended = (
                meta_w[0] * _norm(lr_coefs)
                + meta_w[1] * _norm(xgb_raw)
                + meta_w[2] * _norm(rf_raw)
                + meta_w[3] * _norm(et_raw)
            )
            importances = {fn: round(float(v * 100), 2) for fn, v in zip(feature_names, blended)}

    else:
        raise ValueError(f"Unsupported model_type: {model_type}")

    return prob, tuned_params, importances


def save_snapshot(path: str, rows: list[dict]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fields = [
        "picked_at_utc",
        "bucket_timestamp",
        "rank",
        "symbol",
        "name",
        "token_address",
        "chain",
        "score",
        "entry_close_price",
    ]

    def _normalize_row(row: dict) -> dict:
        out = dict(row)
        chain_val = out.get("chain")
        chain = str(chain_val).strip().lower() if chain_val is not None else ""
        if not chain:
            score_val = out.get("score")
            entry_val = out.get("entry_close_price")
            try:
                float(score_val)
            except Exception:
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

    exists = os.path.exists(path)
    file_has_content = exists and os.path.getsize(path) > 0
    if file_has_content:
        with open(path, "r", newline="", encoding="utf-8") as f:
            existing_rows = list(csv.DictReader(f))
        needs_migration = bool(existing_rows) and "chain" not in existing_rows[0]
        if needs_migration:
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                for row in existing_rows:
                    normalized = _normalize_row(row)
                    w.writerow({k: normalized.get(k) for k in fields})

    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not file_has_content:
            w.writeheader()
        for row in rows:
            w.writerow(row)


def verify_snapshot(conn: psycopg.Connection, snapshot_path: str, min_minutes: int):
    with open(snapshot_path, "r", newline="", encoding="utf-8") as f:
        picks = list(csv.DictReader(f))

    if not picks:
        raise ValueError("Snapshot file has no picks")

    latest_pick_time = max(datetime.fromisoformat(r["picked_at_utc"]) for r in picks)
    cutoff = latest_pick_time + timedelta(minutes=min_minutes)
    now = datetime.now(timezone.utc)

    if now < cutoff:
        remaining = int((cutoff - now).total_seconds())
        raise ValueError(f"Too early to verify. Wait ~{remaining} seconds.")

    print(f"VERIFYING picks from {latest_pick_time.isoformat()} (min +{min_minutes}m)")

    for row in picks:
        token = row["token_address"]
        symbol = row["symbol"]
        name = row["name"]
        bucket_ts = datetime.fromisoformat(row["bucket_timestamp"])
        entry = float(row["entry_close_price"]) if row["entry_close_price"] else float("nan")

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT close_price::DOUBLE PRECISION, bucket_timestamp
                FROM token_price_5m
                WHERE token_address = %s
                  AND bucket_timestamp > %s
                ORDER BY bucket_timestamp ASC
                LIMIT 1
                """,
                (token, bucket_ts),
            )
            nxt = cursor.fetchone()

        if not nxt or nxt[0] is None or np.isnan(entry):
            print(f"{symbol:10s} {name[:20]:20s}  entry={entry:.8f}  next=NA  change=NA")
            continue

        next_price = float(nxt[0])
        pct = ((next_price - entry) / entry) * 100 if entry != 0 else float("nan")
        direction = "UP" if pct > 0 else ("DOWN" if pct < 0 else "FLAT")
        print(f"{symbol:10s} {name[:20]:20s}  entry={entry:.8f}  next={next_price:.8f}  change={pct:+.2f}%  {direction}")


def fetch_coinstats_market_by_addresses(addresses: list[str]) -> dict[str, dict]:
    api_key = os.getenv("COINSTATS_API_KEY", "").strip()
    if not api_key:
        print("CoinStats: COINSTATS_API_KEY not set, skipping market enrichment")
        return {}

    # Build lower→original mapping; EVM addresses are already lowercase in DB,
    # Solana addresses are case-sensitive base58 and must not be lowercased.
    lower_to_original: dict[str, str] = {}
    for a in addresses:
        if a:
            lower_to_original[a.lower()] = a
    clean_addresses = list(lower_to_original.values())  # original case preserved
    requested = set(lower_to_original.keys())  # lowercase set for matching
    if not clean_addresses:
        return {}

    blockchains = os.getenv("COINSTATS_BLOCKCHAINS", "").strip()
    query_params = {
        "contractAddresses": ",".join(clean_addresses),  # original case for API
        "limit": str(max(20, len(clean_addresses))),
    }
    if blockchains:
        query_params["blockchains"] = blockchains

    query = urllib.parse.urlencode(query_params)
    url = f"https://openapiv1.coinstats.app/coins?{query}"

    try:
        req = urllib.request.Request(
            url,
            headers={
                "X-API-KEY": api_key,
                "User-Agent": "Mozilla/5.0",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except Exception as err:
        print(f"CoinStats fetch failed: {err}")
        return {}

    result = payload.get("result") or []
    mapped: dict[str, dict] = {}
    for coin in result:
        addr_list = []
        single_addr = coin.get("contractAddress")
        if isinstance(single_addr, str) and single_addr:
            addr_list.append(single_addr.lower())

        multi_addrs = coin.get("contractAddresses") or []
        for entry in multi_addrs:
            if isinstance(entry, str) and entry:
                addr_list.append(entry.lower())
                continue

            if isinstance(entry, dict):
                addr_val = entry.get("contractAddress") or entry.get("address")
                if isinstance(addr_val, str) and addr_val:
                    addr_list.append(addr_val.lower())

        for addr in addr_list:
            if requested and addr not in requested:
                continue
            data = {
                "name": coin.get("name"),
                "symbol": coin.get("symbol"),
                "price": coin.get("price"),
                "marketCap": coin.get("marketCap"),
                "volume": coin.get("volume"),
            }
            mapped[addr] = data  # keyed by lowercase (EVM norm)
            orig = lower_to_original.get(addr)
            if orig and orig != addr:
                mapped[orig] = data  # also keyed by original case (Solana)
    return mapped


def main() -> None:
    parser = argparse.ArgumentParser(description="Live top-coins scorer from latest 5m bucket")
    parser.add_argument("--mode", choices=["pick", "verify"], default="pick")
    parser.add_argument("--model", choices=["logistic", "xgboost_tuned", "ensemble", "stacking"], default="stacking")
    parser.add_argument("--feature-set", choices=["v2", "cross_rank", "base"], default="v2")
    parser.add_argument("--label-target", choices=["adaptive", "fixed"], default="adaptive")
    parser.add_argument("--preprocessing", choices=["robust", "none"], default="robust")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--market-api", choices=["none", "coinstats"], default="coinstats")
    parser.add_argument("--snapshot-path", default="research/live_picks_snapshot.csv")
    parser.add_argument("--verify-minutes", type=int, default=5)
    args = parser.parse_args()

    load_dotenv()

    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")

        if args.mode == "verify":
            verify_snapshot(conn, args.snapshot_path, args.verify_minutes)
            return

        bucket = latest_bucket(conn)
        x_train, y_train, feature_names, train_addrs, train_buckets = load_training_data(
            conn, args.feature_set, args.label_target, before_bucket=bucket
        )
        meta, x_score, _ = load_scoring_rows(conn, args.feature_set, bucket)

        # ── Feedback loop: auto-verify old picks & compute sample weights ──
        sample_weights = None
        try:
            from feedback_loop import ensure_table, verify_and_store, load_feedback_weights

            ensure_table(conn)
            stats = verify_and_store(conn, args.snapshot_path)
            if stats["verified"] > 0:
                print(f"[FEEDBACK] Verified {stats['verified']} picks — "
                      f"WR: {stats['winRate']:.1f}%")

            sample_weights = load_feedback_weights(
                conn, train_addrs, train_buckets
            )
        except ImportError:
            print("[FEEDBACK] feedback_loop module not found — skipping")
        except Exception as err:
            print(f"[FEEDBACK] Warning: {err} — continuing without weights")

        probs, tuned, importances = score_live(
            model_type=args.model,
            x_train=x_train,
            y_train=y_train,
            x_score=x_score,
            robust=(args.preprocessing == "robust"),
            feature_names=feature_names,
            sample_weights=sample_weights,
        )

        ranked_idx = np.argsort(probs)[::-1]
        now_utc = datetime.now(timezone.utc).isoformat()

        market_map: dict[str, dict] = {}
        if args.market_api == "coinstats":
            market_map = fetch_coinstats_market_by_addresses([m["token_address"] for m in meta])

        # ── Pump guard: cap already-pumped tokens to neutral (keep data, avoid buy signal) ──
        PUMP_THRESHOLD = float(os.getenv("PUMP_FILTER_THRESHOLD", "30"))
        pumped_addrs: set[str] = set()
        if market_map and PUMP_THRESHOLD > 0:
            capped: list[str] = []
            for idx in ranked_idx:
                m = meta[idx]
                addr = m["token_address"]
                mk = market_map.get(addr) or market_map.get(addr.lower(), {})
                change_24h = mk.get("priceChange1d") or mk.get("price_change_24h")
                if isinstance(change_24h, (int, float)) and change_24h > PUMP_THRESHOLD:
                    pumped_addrs.add(addr)
                    capped.append(f"{m.get('symbol', addr[:8])} (+{change_24h:.0f}%)")
            if capped:
                print(f"PUMP GUARD: {len(capped)} tokens already up >{PUMP_THRESHOLD}% → capped to neutral: {', '.join(capped[:10])}")

        picks = []
        print(f"LIVE PICKS at bucket={bucket.isoformat()} model={args.model} feature_set={args.feature_set}")
        if tuned:
            print(f"XGB tuned params: {tuned}")
        if args.market_api == "coinstats":
            print("Market API: CoinStats")
        print("-" * 110)
        print(f"{'RANK':<5} {'SYMBOL':<10} {'NAME':<24} {'SCORE':>8} {'PRICE_USD':>12} {'TOKEN_ADDRESS':<40}")
        print("-" * 110)

        limit = min(args.top_n, len(ranked_idx))
        for rank, idx in enumerate(ranked_idx[:limit], 1):
            m = meta[idx]
            score = float(probs[idx])
            addr = m["token_address"]

            # Cap pumped tokens to neutral (score ≤ 0.35 → neutral, not buy/strong_buy)
            if addr in pumped_addrs:
                score = min(score, 0.35)

            close_price = m["close_price"]
            market = market_map.get(addr) or market_map.get(addr.lower(), {})
            m_symbol = market.get("symbol") or m["symbol"]
            m_name = market.get("name") or m["name"]
            market_price = market.get("price")
            display_price = market_price if isinstance(market_price, (int, float)) else close_price

            print(
                f"{rank:<5} {str(m_symbol)[:10]:<10} {str(m_name)[:24]:<24} "
                f"{score:>8.4f} {display_price:>12.8f} {m['token_address'][:40]:<40}",
                flush=True,
            )

            picks.append(
                {
                    "picked_at_utc": now_utc,
                    "bucket_timestamp": m["bucket_timestamp"].isoformat(),
                    "rank": rank,
                    "symbol": m_symbol,
                    "name": m_name,
                    "token_address": m["token_address"],
                    "chain": m.get("chain", "base"),
                    "score": score,
                    "entry_close_price": display_price,
                }
            )

        save_snapshot(args.snapshot_path, picks)
        print("-" * 110)
        print(f"Saved snapshot: {args.snapshot_path}")

        # ── Save feature importances JSON ──
        importance_path = os.path.join(os.path.dirname(args.snapshot_path) or ".", "feature_importance.json")
        imp_payload = {
            "timestamp": now_utc,
            "model": args.model,
            "featureSet": args.feature_set,
            "trainRows": int(x_train.shape[0]),
            "scoringRows": int(x_score.shape[0]),
            "features": importances,
        }
        with open(importance_path, "w", encoding="utf-8") as fj:
            json.dump(imp_payload, fj, indent=2)
        print(f"Feature importances: {importance_path}")
        for fn, pct in sorted(importances.items(), key=lambda x: x[1], reverse=True):
            print(f"  {fn:30s}  {pct:6.2f}%")

        print(
            "Verify after 5 min with: "
            f"python research/live_top_coins.py --mode verify --snapshot-path {args.snapshot_path} --verify-minutes 5"
        )


if __name__ == "__main__":
    main()
