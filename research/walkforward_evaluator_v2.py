"""
Walk-forward evaluator v2 — all improvements integrated.

Improvements over v1:
  1. Regime-conditioned features (market_momentum_regime, volume_relative_to_median)
  2. Adaptive percentile-based labels (target_adaptive_top20)
  3. Expanded feature set (order_flow_imbalance, hour_sin/cos, regime)
  4. Robust preprocessing (winsorization, zero-indicators, log1p for skewed)
  5. Walk-forward hyperparameter tuning for XGBoost
  6. Ensemble model (logistic + xgboost averaged probabilities)
  7. Regime-stratified evaluation (high/low momentum split)

Usage:
  python research/walkforward_evaluator_v2.py --help
"""
from __future__ import annotations

import argparse
import csv
import math
import os
from dataclasses import dataclass, field
from datetime import datetime
from getpass import getpass
from typing import Any

import numpy as np
import psycopg
from dotenv import load_dotenv
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier


# ---------------------------------------------------------------------------
# Feature sets
# ---------------------------------------------------------------------------

BASE_FEATURES = [
    "volume_velocity",
    "buy_sell_ratio",
    "trade_intensity",
    "wallet_growth_delta",
    "wallet_momentum",
]

CROSS_RANK_FEATURES = BASE_FEATURES + [
    "volume_velocity_rank_pct",
    "buy_sell_ratio_rank_pct",
    "trade_intensity_rank_pct",
    "volume_relative_to_median",
]

V2_FEATURES = CROSS_RANK_FEATURES + [
    "market_momentum_regime",
    "hour_sin",
    "hour_cos",
    "order_flow_imbalance",
    "minutes_since_last_spike",
]

MOMENTUM_PLUS_FEATURES = V2_FEATURES + [
    "relative_momentum",
    "volume_shock",
    "macd_proxy",
    "rsi_14",
]

FEATURE_SETS = {
    "base": BASE_FEATURES,
    "cross_rank": CROSS_RANK_FEATURES,
    "v2": V2_FEATURES,
    "momentum_plus": MOMENTUM_PLUS_FEATURES,
}

# Features that benefit from log1p transform (right-skewed or zero-inflated)
SKEWED_FEATURES = {
    "buy_sell_ratio",
    "wallet_growth_delta",
    "volume_accel",
    "volume_relative_to_median",
    # minutes_since_last_spike is right-skewed (most values 0-30, long tail)
    # but also has -1 sentinel; skip log1p for it to preserve the -1 sentinel
}


# ---------------------------------------------------------------------------
# Label targets
# ---------------------------------------------------------------------------

LABEL_TARGETS = {
    "fixed": "l.target_up_5pct_2h::INTEGER",
    "adaptive": "l.target_adaptive_top20::INTEGER",
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Dataset:
    features: np.ndarray
    labels: np.ndarray
    regime_values: np.ndarray    # market_momentum_regime per row (for stratification)
    bucket_timestamps: np.ndarray
    feature_names: list[str]


@dataclass
class FoldResult:
    fold_idx: int
    train_start: int
    train_end: int
    test_start: int
    test_end: int
    train_pos_rate: float
    test_pos_rate: float
    roc_auc: float
    pr_auc: float
    precision_top: float
    regime_label: str = ""
    regime_roc: float = float("nan")
    regime_ptop: float = float("nan")


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

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


def load_dataset(feature_set: str, label_target: str) -> Dataset:
    feature_names = FEATURE_SETS[feature_set]
    label_sql = LABEL_TARGETS[label_target]

    feature_columns = [f"f.{name}::DOUBLE PRECISION" for name in feature_names]
    feature_sql = ",\n                    ".join(feature_columns)

    # Always fetch regime for stratification
    sql = f"""
        SELECT
            {feature_sql},
            {label_sql},
            f.market_momentum_regime::DOUBLE PRECISION,
            f.bucket_timestamp
        FROM features_5m f
        INNER JOIN labels_5m l
            ON f.token_address = l.token_address
           AND f.bucket_timestamp = l.bucket_timestamp
        WHERE {label_sql} IN (0, 1)
        ORDER BY f.bucket_timestamp ASC, f.token_address ASC
    """

    conn = psycopg.connect(
        host=get_env("PGHOST"),
        port=int(get_env("PGPORT", "5432")),
        dbname=get_env("PGDATABASE"),
        user=get_env("PGUSER"),
        password=get_db_password(),
        sslmode=get_env("PGSSLMODE", "disable"),
    )

    with conn:
        with conn.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.execute(sql)
            rows = cursor.fetchall()

    if not rows:
        raise ValueError("No rows found")

    n_feat = len(feature_names)
    return Dataset(
        features=np.asarray([r[:n_feat] for r in rows], dtype=np.float64),
        labels=np.asarray([r[n_feat] for r in rows], dtype=np.int32),
        regime_values=np.asarray([r[n_feat + 1] for r in rows], dtype=np.float64),
        bucket_timestamps=np.asarray([r[n_feat + 2] for r in rows], dtype=object),
        feature_names=feature_names,
    )


# ---------------------------------------------------------------------------
# Robust preprocessing
# ---------------------------------------------------------------------------

def robust_preprocess(
    x_train: np.ndarray,
    x_test: np.ndarray,
    feature_names: list[str],
) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """Apply winsorization, zero-indicators, and log1p for skewed features.

    Returns augmented feature arrays and updated feature name list.
    """
    train_cols: list[np.ndarray] = []
    test_cols: list[np.ndarray] = []
    out_names: list[str] = []

    for i, name in enumerate(feature_names):
        train_col = x_train[:, i].copy()
        test_col = x_test[:, i].copy()

        # Replace NaN with 0
        train_col = np.nan_to_num(train_col, nan=0.0)
        test_col = np.nan_to_num(test_col, nan=0.0)

        # Winsorize at 1st/99th percentile (fit on train only)
        lo = float(np.percentile(train_col, 1))
        hi = float(np.percentile(train_col, 99))
        if lo < hi:
            train_col = np.clip(train_col, lo, hi)
            test_col = np.clip(test_col, lo, hi)

        train_cols.append(train_col)
        test_cols.append(test_col)
        out_names.append(name)

        # Zero-indicator if >=15% zeros in training set
        zero_rate = float(np.mean(train_col == 0))
        if zero_rate >= 0.15:
            train_cols.append((x_train[:, i] == 0).astype(np.float64))
            test_cols.append((x_test[:, i] == 0).astype(np.float64))
            out_names.append(f"{name}_is_zero")

        # Log1p for skewed features
        if name in SKEWED_FEATURES:
            train_cols.append(np.log1p(np.clip(train_col, 0, None)))
            test_cols.append(np.log1p(np.clip(test_col, 0, None)))
            out_names.append(f"{name}_log1p")

    return np.column_stack(train_cols), np.column_stack(test_cols), out_names


# ---------------------------------------------------------------------------
# Walk-forward folds
# ---------------------------------------------------------------------------

def build_folds(total: int, train_sz: int, test_sz: int, step_sz: int) -> list[tuple[int, int, int, int]]:
    folds = []
    start = 0
    while start + train_sz + test_sz <= total:
        folds.append((start, start + train_sz, start + train_sz, start + train_sz + test_sz))
        start += step_sz
    return folds


def precision_at_top_frac(y_true: np.ndarray, y_score: np.ndarray, frac: float) -> float:
    if len(y_true) == 0:
        return float("nan")
    k = max(1, math.ceil(len(y_true) * frac))
    idx = np.argsort(y_score)[::-1][:k]
    return float(np.mean(y_true[idx]))


# ---------------------------------------------------------------------------
# Model builders
# ---------------------------------------------------------------------------

def make_logistic() -> Pipeline:
    return Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(class_weight="balanced", max_iter=1000, solver="lbfgs")),
    ])


def make_xgboost(y_train: np.ndarray, **overrides: Any) -> XGBClassifier:
    pos = int(np.sum(y_train))
    neg = int(len(y_train) - pos)
    spw = neg / pos if pos > 0 else 1.0
    params = dict(
        objective="binary:logistic",
        eval_metric="logloss",
        n_estimators=200,
        learning_rate=0.05,
        max_depth=3,
        min_child_weight=10,
        subsample=0.7,
        colsample_bytree=0.7,
        reg_lambda=1.0,
        random_state=42,
        n_jobs=1,
        scale_pos_weight=spw,
    )
    params.update(overrides)
    return XGBClassifier(**params)


def make_random_forest(y_train: np.ndarray | None = None) -> RandomForestClassifier:
    """Bagging ensemble — captures different non-linear patterns from XGBoost."""
    return RandomForestClassifier(
        n_estimators=200,
        max_depth=5,
        min_samples_leaf=8,
        max_features="sqrt",
        class_weight="balanced",
        random_state=42,
        n_jobs=1,
    )


def make_extratrees(y_train: np.ndarray | None = None) -> ExtraTreesClassifier:
    """Extreme randomization — high variance/low bias, complements RF and XGB."""
    return ExtraTreesClassifier(
        n_estimators=200,
        max_depth=5,
        min_samples_leaf=8,
        max_features="sqrt",
        class_weight="balanced",
        random_state=43,
        n_jobs=1,
    )


def stacking_oof_predictions(
    x_train: np.ndarray,
    y_train: np.ndarray,
    n_folds: int = 5,
    sample_weights: np.ndarray | None = None,
    robust_fold_preprocess: bool = False,
    feature_names: list[str] | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Generate time-aware out-of-fold meta-features from 4 diverse base learners.

    Uses sklearn TimeSeriesSplit (expanding window) to avoid look-ahead bias.
    Base learners: Logistic Regression, XGBoost, Random Forest, Extra Trees.

    Returns
    -------
    oof_preds : (n_train, 4) float array  — NaN where no fold prediction exists
    has_all   : bool mask — True for rows with predictions from all 4 learners
    """
    n_learners = 4
    oof_preds = np.full((len(y_train), n_learners), np.nan)
    tscv = TimeSeriesSplit(n_splits=n_folds)

    for train_idx, test_idx in tscv.split(x_train):
        x_fold_tr = x_train[train_idx]
        y_fold_tr = y_train[train_idx]
        x_fold_te = x_train[test_idx]

        # Skip degenerate folds (too small or single class)
        if len(x_fold_tr) < 80 or len(np.unique(y_fold_tr)) < 2:
            continue

        if robust_fold_preprocess:
            if not feature_names:
                raise ValueError("feature_names is required when robust_fold_preprocess=True")
            # Fit preprocessing on fold-train only to avoid OOF leakage.
            x_fold_tr, x_fold_te, _ = robust_preprocess(x_fold_tr, x_fold_te, feature_names=feature_names)

        sw_fold = sample_weights[train_idx] if sample_weights is not None else None

        learners = [
            (make_logistic(),        "lr"),
            (make_xgboost(y_fold_tr), "xgb"),
            (make_random_forest(),   "rf"),
            (make_extratrees(),      "et"),
        ]

        for j, (model, mtype) in enumerate(learners):
            try:
                if sw_fold is not None:
                    if mtype == "lr":
                        model.fit(x_fold_tr, y_fold_tr, clf__sample_weight=sw_fold)
                    else:
                        model.fit(x_fold_tr, y_fold_tr, sample_weight=sw_fold)
                else:
                    model.fit(x_fold_tr, y_fold_tr)
                oof_preds[test_idx, j] = model.predict_proba(x_fold_te)[:, 1]
            except Exception:
                pass  # leave NaN — will be excluded from meta-learner training

    has_all = ~np.any(np.isnan(oof_preds), axis=1)
    return oof_preds, has_all


# ---------------------------------------------------------------------------
# Hyperparameter tuning (walk-forward inner CV)
# ---------------------------------------------------------------------------

XGBOOST_GRID = [
    {"max_depth": 2, "learning_rate": 0.03, "n_estimators": 300},
    {"max_depth": 3, "learning_rate": 0.05, "n_estimators": 250},
    {"max_depth": 3, "learning_rate": 0.03, "n_estimators": 350},
    {"max_depth": 4, "learning_rate": 0.05, "n_estimators": 200},
    {"max_depth": 2, "learning_rate": 0.05, "n_estimators": 400},
    {"max_depth": 3, "learning_rate": 0.08, "n_estimators": 200},
]


def tune_xgboost(
    x_train: np.ndarray,
    y_train: np.ndarray,
    top_frac: float,
) -> dict[str, Any]:
    """Inner time-split validation to pick best XGB hyperparams."""
    inner_split = int(len(y_train) * 0.75)
    if inner_split < 100 or len(y_train) - inner_split < 50:
        # Not enough data for inner tuning, return default
        return {}

    x_fit, x_val = x_train[:inner_split], x_train[inner_split:]
    y_fit, y_val = y_train[:inner_split], y_train[inner_split:]

    best_score = -1.0
    best_params: dict[str, Any] = {}

    for candidate in XGBOOST_GRID:
        try:
            model = make_xgboost(y_fit, **candidate)
            model.fit(x_fit, y_fit)
            prob = model.predict_proba(x_val)[:, 1]
            roc = roc_auc_score(y_val, prob)
            ptop = precision_at_top_frac(y_val, prob, top_frac)
            # Combined score: 0.5 * ROC + 0.5 * P@Top10
            combined = 0.5 * roc + 0.5 * ptop
            if combined > best_score:
                best_score = combined
                best_params = candidate
        except Exception:
            continue

    return best_params


# ---------------------------------------------------------------------------
# Regime stratification
# ---------------------------------------------------------------------------

def compute_regime_metrics(
    y_true: np.ndarray,
    y_score: np.ndarray,
    regime_vals: np.ndarray,
    top_frac: float,
) -> dict[str, dict[str, float]]:
    """Split test set into hot/cool regime and compute metrics for each."""
    median_regime = float(np.median(regime_vals)) if len(regime_vals) > 0 else 0.5
    hot_mask = regime_vals >= median_regime
    cool_mask = ~hot_mask

    results = {}
    for label, mask in [("hot", hot_mask), ("cool", cool_mask)]:
        if np.sum(mask) < 10:
            results[label] = {"roc": float("nan"), "ptop": float("nan"), "n": 0}
            continue
        y_t = y_true[mask]
        y_s = y_score[mask]
        try:
            roc = float(roc_auc_score(y_t, y_s))
        except ValueError:
            roc = float("nan")
        ptop = precision_at_top_frac(y_t, y_s, top_frac)
        results[label] = {"roc": roc, "ptop": ptop, "n": int(np.sum(mask))}

    return results


# ---------------------------------------------------------------------------
# CSV logging
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "run_id", "run_timestamp_utc", "row_type",
    "label_target", "model", "feature_set", "preprocessing",
    "top_fraction", "train_fraction", "test_fraction", "step_fraction",
    "total_rows", "fold_index",
    "train_start", "train_end", "test_start", "test_end",
    "train_pos_rate", "test_pos_rate",
    "roc_auc", "pr_auc", "precision_top",
    "regime_hot_roc", "regime_hot_ptop", "regime_cool_roc", "regime_cool_ptop",
    "n_features_after_preproc",
    "xgb_tuned_params",
    "roc_mean", "roc_std", "roc_min", "roc_max",
    "pr_mean", "pr_std",
    "ptop_mean", "ptop_std", "ptop_min", "ptop_max",
    "baseline_roc_mean", "baseline_ptop_mean",
    "roc_delta", "ptop_delta", "decision",
]


def append_csv(csv_path: str, rows: list[dict]) -> None:
    if not csv_path:
        return
    d = os.path.dirname(csv_path)
    if d:
        os.makedirs(d, exist_ok=True)
    exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Walk-forward evaluator v2 — all improvements")
    parser.add_argument("--label-target", type=str, default="adaptive", choices=["fixed", "adaptive"])
    parser.add_argument("--model", type=str, default="stacking", choices=["logistic", "xgboost", "xgboost_tuned", "ensemble", "stacking"])
    parser.add_argument("--feature-set", type=str, default="cross_rank", choices=["base", "cross_rank", "v2"])
    parser.add_argument("--preprocessing", type=str, default="robust", choices=["none", "robust"])
    parser.add_argument("--top-fraction", type=float, default=0.10)
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--test-fraction", type=float, default=0.20)
    parser.add_argument("--step-fraction", type=float, default=0.10)
    parser.add_argument("--csv-path", type=str, default="research/walkforward_v2_runs.csv")
    parser.add_argument("--baseline-roc-mean", type=float, default=float("nan"))
    parser.add_argument("--baseline-ptop-mean", type=float, default=float("nan"))
    args = parser.parse_args()

    load_dotenv()
    dataset = load_dataset(feature_set=args.feature_set, label_target=args.label_target)

    total = len(dataset.labels)
    train_sz = int(total * args.train_fraction)
    test_sz = int(total * args.test_fraction)
    step_sz = int(total * args.step_fraction)

    folds = build_folds(total, train_sz, test_sz, step_sz)
    if not folds:
        raise ValueError("No folds can be formed")

    print(
        f"MODEL={args.model} LABEL={args.label_target} FEATURES={args.feature_set} "
        f"PREPROC={args.preprocessing} TOTAL_ROWS={total} FOLDS={len(folds)}"
    )

    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    run_ts = datetime.utcnow().isoformat()

    roc_vals, pr_vals, ptop_vals = [], [], []
    hot_rocs, cool_rocs = [], []
    csv_rows: list[dict] = []

    for fi, (ts, te, vs, ve) in enumerate(folds, 1):
        x_raw_train = dataset.features[ts:te]
        y_train = dataset.labels[ts:te]
        x_raw_test = dataset.features[vs:ve]
        y_test = dataset.labels[vs:ve]
        regime_test = dataset.regime_values[vs:ve]

        # --- Preprocessing ---
        if args.preprocessing == "robust":
            x_train, x_test, feat_names = robust_preprocess(
                x_raw_train, x_raw_test, dataset.feature_names
            )
        else:
            x_train, x_test = x_raw_train, x_raw_test
            feat_names = dataset.feature_names

        n_feats = x_train.shape[1]
        tuned_params_str = ""

        # --- Model ---
        if args.model == "logistic":
            model = make_logistic()
            model.fit(x_train, y_train)
            prob = model.predict_proba(x_test)[:, 1]

        elif args.model == "xgboost":
            model = make_xgboost(y_train)
            model.fit(x_train, y_train)
            prob = model.predict_proba(x_test)[:, 1]

        elif args.model == "xgboost_tuned":
            best_params = tune_xgboost(x_train, y_train, args.top_fraction)
            tuned_params_str = str(best_params)
            model = make_xgboost(y_train, **best_params)
            model.fit(x_train, y_train)
            prob = model.predict_proba(x_test)[:, 1]

        elif args.model == "ensemble":
            # Logistic
            lr = make_logistic()
            lr.fit(x_train, y_train)
            lr_prob = lr.predict_proba(x_test)[:, 1]

            # XGBoost (tuned)
            best_params = tune_xgboost(x_train, y_train, args.top_fraction)
            tuned_params_str = str(best_params)
            xgb = make_xgboost(y_train, **best_params)
            xgb.fit(x_train, y_train)
            xgb_prob = xgb.predict_proba(x_test)[:, 1]

            # Average probabilities
            prob = 0.4 * lr_prob + 0.6 * xgb_prob

        elif args.model == "stacking":
            # --- Level-0: time-aware OOF predictions from 4 diverse base learners ---
            oof_preds, has_all = stacking_oof_predictions(
                x_train, y_train, n_folds=5
            )

            # --- Level-1: meta-learner trained on OOF rows only ---
            meta_x = oof_preds[has_all]
            meta_y = y_train[has_all]
            meta = LogisticRegression(max_iter=1000, solver="lbfgs", random_state=42)
            if len(meta_x) < 20 or len(np.unique(meta_y)) < 2:
                # Fallback: weighted average (same as ensemble)
                lr = make_logistic()
                lr.fit(x_train, y_train)
                xgb = make_xgboost(y_train)
                xgb.fit(x_train, y_train)
                prob = 0.4 * lr.predict_proba(x_test)[:, 1] + 0.6 * xgb.predict_proba(x_test)[:, 1]
            else:
                meta.fit(meta_x, meta_y)

                # --- Retrain all base learners on full training fold ---
                best_params = tune_xgboost(x_train, y_train, args.top_fraction)
                tuned_params_str = str(best_params)
                lr_f = make_logistic()
                xgb_f = make_xgboost(y_train, **best_params)
                rf_f = make_random_forest()
                et_f = make_extratrees()
                for m in (lr_f, xgb_f, rf_f, et_f):
                    m.fit(x_train, y_train)

                # --- Score test set through base learners → meta ---
                base_test = np.column_stack([
                    lr_f.predict_proba(x_test)[:, 1],
                    xgb_f.predict_proba(x_test)[:, 1],
                    rf_f.predict_proba(x_test)[:, 1],
                    et_f.predict_proba(x_test)[:, 1],
                ])
                prob = meta.predict_proba(base_test)[:, 1]

        else:
            raise ValueError(f"Unknown model: {args.model}")

        # --- Metrics ---
        try:
            roc = float(roc_auc_score(y_test, prob))
        except ValueError:
            roc = float("nan")

        try:
            pr = float(average_precision_score(y_test, prob))
        except ValueError:
            pr = float("nan")

        ptop = precision_at_top_frac(y_test, prob, args.top_fraction)

        # --- Regime stratification ---
        regime_metrics = compute_regime_metrics(y_test, prob, regime_test, args.top_fraction)

        train_pos = float(np.mean(y_train))
        test_pos = float(np.mean(y_test))

        print(
            f"FOLD={fi} TRAIN=[{ts},{te}) TEST=[{vs},{ve}) "
            f"TRAIN_POS={train_pos:.4f} TEST_POS={test_pos:.4f} "
            f"ROC={roc:.4f} PR={pr:.4f} P@TOP10={ptop:.4f} "
            f"N_FEATS={n_feats} "
            f"HOT_ROC={regime_metrics['hot']['roc']:.4f} HOT_PTOP={regime_metrics['hot']['ptop']:.4f} "
            f"COOL_ROC={regime_metrics['cool']['roc']:.4f} COOL_PTOP={regime_metrics['cool']['ptop']:.4f}"
        )

        roc_vals.append(roc)
        pr_vals.append(pr)
        ptop_vals.append(ptop)
        if not np.isnan(regime_metrics["hot"]["roc"]):
            hot_rocs.append(regime_metrics["hot"]["roc"])
        if not np.isnan(regime_metrics["cool"]["roc"]):
            cool_rocs.append(regime_metrics["cool"]["roc"])

        csv_rows.append({
            "run_id": run_id, "run_timestamp_utc": run_ts, "row_type": "fold",
            "label_target": args.label_target, "model": args.model,
            "feature_set": args.feature_set, "preprocessing": args.preprocessing,
            "top_fraction": args.top_fraction,
            "train_fraction": args.train_fraction,
            "test_fraction": args.test_fraction,
            "step_fraction": args.step_fraction,
            "total_rows": total, "fold_index": fi,
            "train_start": ts, "train_end": te, "test_start": vs, "test_end": ve,
            "train_pos_rate": train_pos, "test_pos_rate": test_pos,
            "roc_auc": roc, "pr_auc": pr, "precision_top": ptop,
            "regime_hot_roc": regime_metrics["hot"]["roc"],
            "regime_hot_ptop": regime_metrics["hot"]["ptop"],
            "regime_cool_roc": regime_metrics["cool"]["roc"],
            "regime_cool_ptop": regime_metrics["cool"]["ptop"],
            "n_features_after_preproc": n_feats,
            "xgb_tuned_params": tuned_params_str,
        })

    # --- Summary ---
    roc_arr = np.array(roc_vals)
    pr_arr = np.array(pr_vals)
    ptop_arr = np.array(ptop_vals)

    roc_m, roc_s = float(np.nanmean(roc_arr)), float(np.nanstd(roc_arr))
    pr_m, pr_s = float(np.nanmean(pr_arr)), float(np.nanstd(pr_arr))
    ptop_m, ptop_s = float(np.nanmean(ptop_arr)), float(np.nanstd(ptop_arr))

    print(f"\nSUMMARY ROC_MEAN={roc_m:.4f} ROC_STD={roc_s:.4f} ROC_MIN={np.nanmin(roc_arr):.4f} ROC_MAX={np.nanmax(roc_arr):.4f}")
    print(f"SUMMARY PR_MEAN={pr_m:.4f} PR_STD={pr_s:.4f}")
    print(f"SUMMARY P@TOP10_MEAN={ptop_m:.4f} P@TOP10_STD={ptop_s:.4f} MIN={np.nanmin(ptop_arr):.4f} MAX={np.nanmax(ptop_arr):.4f}")

    if hot_rocs:
        print(f"REGIME_HOT ROC_MEAN={np.mean(hot_rocs):.4f} N_FOLDS={len(hot_rocs)}")
    if cool_rocs:
        print(f"REGIME_COOL ROC_MEAN={np.mean(cool_rocs):.4f} N_FOLDS={len(cool_rocs)}")

    # --- Baseline comparison ---
    bl_roc = float(args.baseline_roc_mean)
    bl_ptop = float(args.baseline_ptop_mean)
    roc_delta = roc_m - bl_roc if not np.isnan(bl_roc) else float("nan")
    ptop_delta = ptop_m - bl_ptop if not np.isnan(bl_ptop) else float("nan")

    if not np.isnan(bl_roc) or not np.isnan(bl_ptop):
        decision = "KEEP" if (
            (np.isnan(roc_delta) or roc_delta >= -0.01) and
            (np.isnan(ptop_delta) or ptop_delta >= 0.0)
        ) else "REJECT"
        print(f"RECOMMENDATION={decision} ROC_DELTA={roc_delta:.4f} PTOP_DELTA={ptop_delta:.4f}")
    else:
        decision = "N/A"
        print("RECOMMENDATION=N/A (no baseline supplied)")

    csv_rows.append({
        "run_id": run_id, "run_timestamp_utc": run_ts, "row_type": "summary",
        "label_target": args.label_target, "model": args.model,
        "feature_set": args.feature_set, "preprocessing": args.preprocessing,
        "top_fraction": args.top_fraction,
        "train_fraction": args.train_fraction,
        "test_fraction": args.test_fraction,
        "step_fraction": args.step_fraction,
        "total_rows": total, "fold_index": len(folds),
        "roc_mean": roc_m, "roc_std": roc_s,
        "roc_min": float(np.nanmin(roc_arr)), "roc_max": float(np.nanmax(roc_arr)),
        "pr_mean": pr_m, "pr_std": pr_s,
        "ptop_mean": ptop_m, "ptop_std": ptop_s,
        "ptop_min": float(np.nanmin(ptop_arr)), "ptop_max": float(np.nanmax(ptop_arr)),
        "baseline_roc_mean": "" if np.isnan(bl_roc) else bl_roc,
        "baseline_ptop_mean": "" if np.isnan(bl_ptop) else bl_ptop,
        "roc_delta": "" if np.isnan(roc_delta) else roc_delta,
        "ptop_delta": "" if np.isnan(ptop_delta) else ptop_delta,
        "decision": decision,
    })

    append_csv(args.csv_path, csv_rows)
    print(f"CSV_LOG_APPENDED={args.csv_path} RUN_ID={run_id}")


if __name__ == "__main__":
    main()
