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
from sklearn.isotonic import IsotonicRegression


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

STABLES = {"USDC","USDT","DAI","BUSD","TUSD","USDP","FDUSD"}
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

def build_isotonic_calibrator(conn: psycopg.Connection) -> IsotonicRegression | None:
    print("[CALIBRATION] Fetching 14-day history for Isotonic Regression...")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT model_score, (CASE WHEN effective_return > 0 THEN 1 ELSE 0 END) AS label
                FROM pick_outcomes
                WHERE picked_at_utc > NOW() - INTERVAL '14 days'
                  AND recommendation IN ('buy', 'strong_buy')
                  AND model_score IS NOT NULL
            """)
            rows = cur.fetchall()
            
        if len(rows) < 100:
            print(f"[CALIBRATION] Insufficient history ({len(rows)}/100 needed) — skipping")
            return None
            
        scores = np.array([float(r[0]) for r in rows])
        labels = np.array([int(r[1]) for r in rows])
        
        calibrator = IsotonicRegression(out_of_bounds='clip')
        calibrator.fit(scores, labels)
        print(f"[CALIBRATION] Fitted IsotonicRegression on {len(rows)} outcomes")
        return calibrator
    except Exception as e:
        print(f"[CALIBRATION] Warning: Isotonic calibration failed: {e}")
        return None

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
        INNER JOIN tokens t
        ON t.token_address = f.token_address
        WHERE {label_sql} IN (0,1)
        AND f.bucket_timestamp < %s
        AND UPPER(t.symbol) NOT IN (
            'USDC','USDT','DAI','BUSD','TUSD','USDP','FDUSD','XAUT'
        )
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
            t.created_at,
            f.bucket_timestamp,
            tp.close_price::DOUBLE PRECISION AS close_price,
            tm.total_volume::DOUBLE PRECISION AS total_volume_5m,
            fp.first_price_ts,
            {feature_sql}
        FROM features_5m f
        INNER JOIN tokens t
            ON t.token_address = f.token_address
        INNER JOIN (
            SELECT token_address, MAX(bucket_timestamp) AS latest_ts
            FROM features_5m
            WHERE bucket_timestamp >= %s - INTERVAL '30 minutes'
            GROUP BY token_address
        ) latest
            ON f.token_address = latest.token_address
           AND f.bucket_timestamp = latest.latest_ts
        LEFT JOIN token_price_5m tp
            ON tp.token_address = f.token_address
           AND tp.bucket_timestamp = f.bucket_timestamp
        LEFT JOIN token_metrics_5m tm
            ON tm.token_address = f.token_address
           AND tm.bucket_timestamp = f.bucket_timestamp
        LEFT JOIN LATERAL (
            SELECT MIN(tp0.bucket_timestamp) AS first_price_ts
            FROM token_price_5m tp0
            WHERE tp0.token_address = f.token_address
        ) fp ON TRUE
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
        token_address, symbol, name, chain, token_created_at, bucket_ts, close_price, total_volume_5m, first_price_ts, *feature_vals = r
        meta.append(
            {
                "token_address": token_address,
                "symbol": symbol,
                "name": name,
                "chain": chain or "base",
                "token_created_at": token_created_at,
                "bucket_timestamp": bucket_ts,
                "close_price": float(close_price) if close_price is not None else float("nan"),
                "total_volume_5m": float(total_volume_5m) if total_volume_5m is not None else None,
                "first_price_ts": first_price_ts,
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
    model_save_path: str | None = None,
):
    _bundle: dict = {}
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
        _bundle = {"model": lr}

    elif model_type == "xgboost_tuned":
        tuned_params = tune_xgboost(x_train_pp, y_train, top_frac=0.10)
        xgb = make_xgboost(y_train, **tuned_params)
        xgb.fit(x_train_pp, y_train, **xgb_fit_kw)
        prob = xgb.predict_proba(x_score_pp)[:, 1]
        raw = xgb.feature_importances_
        total = raw.sum() or 1.0
        importances = {fn: round(float(v / total * 100), 2) for fn, v in zip(feature_names, raw)}
        _bundle = {"model": xgb}

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
        _bundle = {"lr": lr, "xgb": xgb}

    elif model_type == "stacking":
        # ── Level-0: time-aware OOF predictions from 4 diverse base learners ──
        # Learners: Logistic Regression, XGBoost, Random Forest, Extra Trees
        # Uses expanding-window TimeSeriesSplit to prevent look-ahead bias.
        from sklearn.linear_model import LogisticRegression as MetaLR

        print("[STACKING] Generating out-of-fold meta-features (5-fold time-split)...")
        oof_preds, has_all = stacking_oof_predictions(
            x_train if robust else x_train_pp,
            y_train,
            n_folds=4,
            sample_weights=sample_weights,
            robust_fold_preprocess=bool(robust),
            feature_names=feature_names if robust else None,
        )
        oof_coverage = int(has_all.sum())
        print(f"[STACKING] OOF coverage: {oof_coverage}/{len(y_train)} rows")

        # ── OOF AUC per base learner + ensemble ──
        if has_all.sum() > 0 and len(np.unique(y_train[has_all])) > 1:
            try:
                from sklearn.metrics import roc_auc_score as _roc_auc
                _yv = y_train[has_all]
                _auc_lr  = _roc_auc(_yv, oof_preds[has_all, 0])
                _auc_xgb = _roc_auc(_yv, oof_preds[has_all, 1])
                _auc_rf  = _roc_auc(_yv, oof_preds[has_all, 2])
                _auc_et  = _roc_auc(_yv, oof_preds[has_all, 3])
                _auc_ens = _roc_auc(_yv, oof_preds[has_all].mean(axis=1))
                _flag = "OK >= 0.62" if _auc_ens >= 0.62 else "BELOW 0.62 target"
                print(
                    f"[STACKING] OOF AUC  "
                    f"LR={_auc_lr:.4f}  XGB={_auc_xgb:.4f}  "
                    f"RF={_auc_rf:.4f}  ET={_auc_et:.4f}  "
                    f"Ensemble={_auc_ens:.4f}  [{_flag}]"
                )
            except Exception:
                pass

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
            # Keep stronger linear influence when OOF rows are insufficient.
            prob = 0.55 * lr_fb.predict_proba(x_score_pp)[:, 1] + 0.45 * xgb_fb.predict_proba(x_score_pp)[:, 1]
            lr_c = np.abs(lr_fb.named_steps["clf"].coef_[0])
            xgb_r = xgb_fb.feature_importances_
            blended = 0.55 * (lr_c / (lr_c.sum() or 1)) + 0.45 * (xgb_r / (xgb_r.sum() or 1))
            importances = {fn: round(float(v * 100), 2) for fn, v in zip(feature_names, blended)}
            _bundle = {"lr": lr_fb, "xgb": xgb_fb}
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
            print(
                f"[STACKING] Meta-weights: LR={meta_w[0]:.3f} XGB={meta_w[1]:.3f} "
                f"RF={meta_w[2]:.3f} ET={meta_w[3]:.3f}"
            )

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
            _bundle = {"lr": lr_f, "xgb": xgb_f, "rf": rf_f, "et": et_f, "meta": meta}

    else:
        raise ValueError(f"Unsupported model_type: {model_type}")

    if model_save_path:
        # Intentionally disabled: models are retrained every cycle and never reused from disk.
        print("[MODEL] Persistence disabled - ignoring --model-path and using fresh training each cycle")

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

    # Rolling 72-hour window: prune rows older than this cutoff on every write
    _SNAPSHOT_WINDOW_HOURS = 72
    from datetime import timezone as _tz
    _cutoff = datetime.now(_tz.utc) - timedelta(hours=_SNAPSHOT_WINDOW_HOURS)

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
            # Re-read after migration
            with open(path, "r", newline="", encoding="utf-8") as f:
                existing_rows = list(csv.DictReader(f))

        # Prune to rolling 72-hour window
        def _keep_row(row: dict) -> bool:
            try:
                ts = datetime.fromisoformat(row.get("picked_at_utc", ""))
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=_tz.utc)
                return ts >= _cutoff
            except Exception:
                return True  # keep rows with unparseable timestamps

        pruned_rows = [r for r in existing_rows if _keep_row(r)]
        pruned_count = len(existing_rows) - len(pruned_rows)
        if pruned_count > 0:
            print(f"[snapshot] Pruned {pruned_count} rows older than {_SNAPSHOT_WINDOW_HOURS}h (kept {len(pruned_rows)})")
            # Rewrite file with only recent rows
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                for row in pruned_rows:
                    w.writerow({k: row.get(k) for k in fields})
            file_has_content = bool(pruned_rows)

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


def _enrich_tokens_from_market_map(conn: psycopg.Connection, market_map: dict[str, dict]) -> int:
    """
    Write real symbol/name from CoinStats back into the tokens table for any
    row that still has a placeholder value (symbol LIKE 'TKN_%').
    Returns the number of rows updated.
    """
    if not market_map:
        return 0
    updated = 0
    with conn.cursor() as cur:
        for addr, data in market_map.items():
            real_symbol = data.get("symbol")
            real_name = data.get("name")
            if not real_symbol:
                continue
            cur.execute(
                """
                UPDATE tokens
                   SET symbol = %s,
                       name   = COALESCE(%s, name)
                 WHERE token_address = %s
                   AND (symbol LIKE 'TKN_%%' OR name LIKE 'Token %%')
                """,
                (real_symbol, real_name, addr),
            )
            updated += cur.rowcount
    return updated


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
    parser.add_argument("--feature-set", choices=["v2", "cross_rank", "base", "momentum_plus"], default="cross_rank")
    parser.add_argument("--label-target", choices=["adaptive", "fixed"], default="adaptive")
    parser.add_argument("--preprocessing", choices=["robust", "none"], default="robust")
    parser.add_argument("--top-n", type=int, default=50)
    parser.add_argument("--market-api", choices=["none", "coinstats"], default="coinstats")
    parser.add_argument("--snapshot-path", default="research/live_picks_snapshot.csv")
    parser.add_argument("--model-path", default="", help="Deprecated: ignored, models are not persisted")
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
        # split training rows by chain
        chain_map = {"eth": [], "solana": [], "bsc": [], "base": []}

        with conn.cursor() as c:
            c.execute(
                "SELECT token_address, chain FROM tokens WHERE token_address = ANY(%s)",
                (train_addrs,)
            )
            addr_chain = {r[0]: (r[1] or "base").lower() for r in c.fetchall()}

        for i, addr in enumerate(train_addrs):
            ch = addr_chain.get(addr, "base")
            if ch == "sol":
                ch = "solana"
            if ch not in chain_map:
                ch = "base"
            chain_map[ch].append(i)

        chain_train = {}
        for ch, idx in chain_map.items():
            if len(idx) > 50:
                chain_train[ch] = (
                    x_train[idx],
                    y_train[idx]
                )
        meta, x_score, _ = load_scoring_rows(conn, args.feature_set, bucket)

        # ── Momentum trigger gate REMOVED (scoring all valid tokens natively) ──

        # ── Feedback loop: auto-verify old picks & compute sample weights ──
        sample_weights = None
        try:
            from feedback_loop import ensure_table, verify_and_store, load_feedback_weights

            ensure_table(conn)
            stats = verify_and_store(conn, args.snapshot_path)
            if stats["verified"] > 0:
                print(f"[FEEDBACK] Verified {stats['verified']} picks — "
                      f"WR: {stats['winRate']:.1f}%")

            # ── Auto-calibrate score thresholds from real win rates ──
            try:
                from feedback_loop import compute_adaptive_thresholds
                compute_adaptive_thresholds(conn)
            except Exception as _te:
                print(f"[THRESHOLDS] Calibration skipped: {_te}")

            sample_weights = load_feedback_weights(
                conn, train_addrs, train_buckets
            )
        except ImportError:
            print("[FEEDBACK] feedback_loop module not found — skipping")
        except Exception as err:
            print(f"[FEEDBACK] Warning: {err} — continuing without weights")

        calibrator = build_isotonic_calibrator(conn)

        # train one model per chain
        chain_models = {}
        importances_per_chain: dict[str, dict[str, float]] = {}
        importances = {}
        tuned = {}
        probs = np.zeros(len(meta))
        chain_thresholds = {}
        
        
        for ch, idx in chain_map.items():
            n_rows = len(idx)
            if n_rows < 100:
                print(f"[{ch}] Only {n_rows} rows — skipping, no model trained")
                continue

            cx = x_train[idx]
            cy = y_train[idx]
            sw = sample_weights[idx] if sample_weights is not None else None

            curr_model = args.model
            if n_rows < 400 and curr_model == "stacking":
                curr_model = "logistic"
                print(f"[{ch}] Only {n_rows} rows — using fallback simple setup")

            # Calibrate threshold on validation split
            split_idx = int(n_rows * 0.8)
            cx_train, cx_val = cx[:split_idx], cx[split_idx:]
            cy_train, cy_val = cy[:split_idx], cy[split_idx:]
            sw_train = sw[:split_idx] if sw is not None else None

            p_val, _, _ = score_live(
                model_type=curr_model,
                x_train=cx_train,
                y_train=cy_train,
                x_score=cx_val,
                robust=(args.preprocessing == "robust"),
                feature_names=feature_names,
                sample_weights=sw_train,
            )

            if calibrator is not None:
                p_val = calibrator.transform(p_val)

            try:
                from sklearn.metrics import precision_recall_curve
                precision, recall, ths = precision_recall_curve(cy_val, p_val)
                viable = np.where(precision[:-1] >= 0.65)[0]
                if len(viable) > 0:
                    optimal_threshold = ths[viable[0]]
                else:
                    opt_idx = np.argmax(2 * precision[:-1] * recall[:-1] / (precision[:-1] + recall[:-1] + 1e-9))
                    optimal_threshold = ths[opt_idx]
            except Exception as e:
                print(f"[{ch}] Threshold calibration error: {e}. Defaulting to 0.55")
                optimal_threshold = 0.55
            
            chain_thresholds[ch] = float(optimal_threshold)
            print(f"[{ch}] Calibrated threshold: {float(optimal_threshold):.4f}")

            # Train on full block
            p, tuned_params, imp = score_live(
                model_type=curr_model,
                x_train=cx,
                y_train=cy,
                x_score=x_score,
                robust=(args.preprocessing == "robust"),
                feature_names=feature_names,
                sample_weights=sw,
                model_save_path=args.model_path,
            )

            if calibrator is not None:
                p = calibrator.transform(p)

            chain_models[ch] = p
            tuned = tuned_params
            importances_per_chain[ch] = imp
            importances = imp  # fallback: last chain for combined view

        # choose score based on token chain

        for i, m in enumerate(meta):
            ch = (m.get("chain") or "base").lower()

            # Ensure ch is string and correctly default if necessary
            if not chain_models:
                probs[i] = 0.0
                continue
                
            if ch not in chain_models:
                ch = next(iter(chain_models))

            # Apply a heavy penalty if under threshold to prevent it from out-ranking true BUY signals
            p_val = chain_models[ch][i]
            if p_val < chain_thresholds.get(ch, 0.55):
                p_val = p_val * 0.1
            probs[i] = p_val

        ranked_idx = np.argsort(probs)[::-1]
        now_utc = datetime.now(timezone.utc).isoformat()

        market_map: dict[str, dict] = {}
        if args.market_api == "coinstats":
            market_map = fetch_coinstats_market_by_addresses([m["token_address"] for m in meta])
            if market_map:
                enriched = _enrich_tokens_from_market_map(conn, market_map)
                if enriched:
                    print(f"[ENRICH] Updated {enriched} token name(s) from CoinStats")

        # ── Tradability guardrails: reduce rug-prone picks before ranking output ──
        # MIN_VOL_24H: minimum 24-hour USD volume ($20k default).
        # We use CoinStats 24h volume when available; otherwise extrapolate
        # from total_volume_5m × 288 (number of 5m buckets in 24 hours).
        MIN_VOL_24H = float(os.getenv("TRADABLE_MIN_VOLUME_24H", os.getenv("TRADABLE_MIN_VOLUME_5M", "20000")))
        MIN_TOKEN_AGE_MINUTES = float(os.getenv("TRADABLE_MIN_TOKEN_AGE_MINUTES", "60"))
        MIN_MARKET_CAP = float(os.getenv("TRADABLE_MIN_MARKET_CAP", "200000"))

        def _token_age_minutes(m: dict) -> float:
            # Prefer first_price_ts (earliest price we've tracked) over
            # token_created_at (DB insertion time, unreliable after resets).
            born = m.get("first_price_ts") or m.get("token_created_at")
            if not born:
                return float("inf")  # unknown age → assume old enough
            try:
                dt = born
                if getattr(dt, "tzinfo", None) is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                age = (m["bucket_timestamp"] - dt).total_seconds() / 60.0
                return max(age, 0.0)
            except Exception:
                return float("inf")  # on error, assume old enough

        tradable_idx: list[int] = []
        rejected_low_vol = 0
        rejected_low_age = 0
        rejected_low_mcap = 0
        for idx in ranked_idx:
            m = meta[idx]
            addr = m["token_address"]
            mk = market_map.get(addr) or market_map.get(addr.lower(), {})

            # Prefer 24h volume from CoinStats; fall back to 5m bucket × 288
            cs_vol_24h = mk.get("volume")
            if cs_vol_24h is not None:
                vol_24h = float(cs_vol_24h)
            else:
                vol_5m = float(m.get("total_volume_5m") or 0.0)
                vol_24h = vol_5m * 288  # extrapolate to 24h

            age_min = _token_age_minutes(m)
            market_cap = mk.get("marketCap")
            if market_cap is None:
                market_cap = mk.get("market_cap")
            # If no market cap data, skip the check (don't penalise unlisted tokens)
            market_cap_ok = True if market_cap is None else float(market_cap) >= MIN_MARKET_CAP

            if vol_24h < MIN_VOL_24H:
                rejected_low_vol += 1
                continue
            if age_min < MIN_TOKEN_AGE_MINUTES:
                rejected_low_age += 1
                continue
            if not market_cap_ok:
                rejected_low_mcap += 1
                continue
            tradable_idx.append(idx)

        if rejected_low_vol or rejected_low_age or rejected_low_mcap:
            print(
                "[TRADABILITY] filtered "
                f"vol24h<{MIN_VOL_24H:.0f}: {rejected_low_vol}, "
                f"age<{MIN_TOKEN_AGE_MINUTES:.0f}m: {rejected_low_age}, "
                f"mcap<{MIN_MARKET_CAP:.0f}: {rejected_low_mcap}"
            )
        if not tradable_idx:
            print("[TRADABILITY] No tokens passed filters — skipping this cycle")
            return

        # ── Pump guard: cap already-pumped tokens to neutral (keep data, avoid buy signal) ──
        PUMP_THRESHOLD = float(os.getenv("PUMP_FILTER_THRESHOLD", "25"))
        pumped_addrs: set[str] = set()
        if market_map and PUMP_THRESHOLD > 0:
            capped: list[str] = []
            for idx in tradable_idx:
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

        limit = min(args.top_n, len(tradable_idx))
        for rank, idx in enumerate(tradable_idx[:limit], 1):
            m = meta[idx]
            score = float(probs[idx]) 
            addr = m["token_address"]

            # Cap pumped tokens to neutral (score ≤ 0.35 → neutral, not buy/strong_buy)
            if addr in pumped_addrs:
                score = min(score, 0.35)

            close_price = m["close_price"]
            if close_price is None or close_price <= 0 or np.isnan(close_price):
                continue
            market = market_map.get(addr) or market_map.get(addr.lower(), {})
            m_symbol = market.get("symbol") or m["symbol"]
            if str(m_symbol).upper() in STABLES:
                continue
            m_name = market.get("name") or m["name"]
            display_price = close_price

            ui_price = market.get("price") or close_price
            print(
                f"{rank:<5} {str(m_symbol)[:10]:<10} {str(m_name)[:24]:<24} "
                f"{score:>8.4f} {ui_price:>12.8f} {m['token_address'][:40]:<40}",
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

        # ── Save feature importances JSON (per-chain + combined) ──
        imp_dir = os.path.dirname(args.snapshot_path) or "."

        # Per-chain importance files
        for ch, ch_imp in importances_per_chain.items():
            ch_path = os.path.join(imp_dir, f"feature_importance_{ch}.json")
            ch_payload = {
                "timestamp": now_utc,
                "model": args.model,
                "chain": ch,
                "featureSet": args.feature_set,
                "trainRows": len(chain_map.get(ch, [])),
                "scoringRows": int(x_score.shape[0]),
                "features": ch_imp,
            }
            with open(ch_path, "w", encoding="utf-8") as fj:
                json.dump(ch_payload, fj, indent=2)

        # Combined importance (average across chains)
        if importances_per_chain:
            all_features = set()
            for ch_imp in importances_per_chain.values():
                all_features.update(ch_imp.keys())
            combined = {}
            for fn in all_features:
                vals = [ch_imp.get(fn, 0.0) for ch_imp in importances_per_chain.values()]
                combined[fn] = round(sum(vals) / len(vals), 2)
            importances = combined

        importance_path = os.path.join(imp_dir, "feature_importance.json")
        imp_payload = {
            "timestamp": now_utc,
            "model": args.model,
            "featureSet": args.feature_set,
            "trainRows": int(x_train.shape[0]),
            "scoringRows": int(x_score.shape[0]),
            "features": importances,
            "perChain": {ch: imp for ch, imp in importances_per_chain.items()},
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
