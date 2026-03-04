from __future__ import annotations

import argparse
import csv
import math
import os
from dataclasses import dataclass
from datetime import datetime
from getpass import getpass

import numpy as np
import psycopg
from dotenv import load_dotenv
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier


@dataclass
class Dataset:
    features: np.ndarray
    labels: np.ndarray
    token_addresses: np.ndarray
    bucket_timestamps: np.ndarray
    feature_names: list[str]


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


def load_dataset(label_table: str, target_name: str | None, feature_set: str) -> Dataset:
    if label_table not in {"labels_5m", "labels_5m_variants"}:
        raise ValueError("--label-table must be 'labels_5m' or 'labels_5m_variants'")
    if feature_set not in {"base", "extended", "cross_rank"}:
        raise ValueError("--feature-set must be 'base', 'extended', or 'cross_rank'")

    label_filter = "WHERE l.target_up_5pct_2h IN (0, 1)"
    label_select = "l.target_up_5pct_2h::INTEGER"

    feature_columns = [
        "f.volume_velocity::DOUBLE PRECISION",
        "f.buy_sell_ratio::DOUBLE PRECISION",
        "f.trade_intensity::DOUBLE PRECISION",
        "f.wallet_growth_delta::DOUBLE PRECISION",
    ]
    feature_names = [
        "volume_velocity",
        "buy_sell_ratio",
        "trade_intensity",
        "wallet_growth_delta",
    ]
    if feature_set == "extended":
        feature_columns.extend(
            [
                "f.return_1h::DOUBLE PRECISION",
                "f.volume_accel::DOUBLE PRECISION",
            ]
        )
        feature_names.extend(["return_1h", "volume_accel"])
    if feature_set == "cross_rank":
        feature_columns.extend(
            [
                "f.volume_velocity_rank_pct::DOUBLE PRECISION",
                "f.buy_sell_ratio_rank_pct::DOUBLE PRECISION",
                "f.trade_intensity_rank_pct::DOUBLE PRECISION",
            ]
        )
        feature_names.extend(
            [
                "volume_velocity_rank_pct",
                "buy_sell_ratio_rank_pct",
                "trade_intensity_rank_pct",
            ]
        )

    if label_table == "labels_5m_variants":
        if not target_name:
            raise ValueError("--target-name is required when --label-table=labels_5m_variants")
        label_filter = "WHERE l.target_name = %s AND l.target_binary IN (0, 1)"
        label_select = "l.target_binary::INTEGER"

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
            feature_sql = ",\n                    ".join(feature_columns)
            sql = f"""
                SELECT
                    {feature_sql},
                    {label_select},
                    f.token_address,
                    f.bucket_timestamp
                FROM features_5m f
                INNER JOIN {label_table} l
                    ON f.token_address = l.token_address
                   AND f.bucket_timestamp = l.bucket_timestamp
                {label_filter}
                ORDER BY f.bucket_timestamp ASC, f.token_address ASC
            """

            if label_table == "labels_5m_variants":
                cursor.execute(sql, (target_name,))
            else:
                cursor.execute(sql)
            rows = cursor.fetchall()

    if not rows:
        raise ValueError("No joined rows found in features_5m + labels table")

    feature_count = len(feature_names)

    return Dataset(
        features=np.asarray([r[:feature_count] for r in rows], dtype=np.float64),
        labels=np.asarray([r[feature_count] for r in rows], dtype=np.int32),
        token_addresses=np.asarray([r[feature_count + 1] for r in rows], dtype=object),
        bucket_timestamps=np.asarray([r[feature_count + 2] for r in rows], dtype=object),
        feature_names=feature_names,
    )


def format_ts(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def precision_at_top_fraction(y_true: np.ndarray, y_score: np.ndarray, fraction: float) -> tuple[int, float]:
    if len(y_true) == 0:
        return 0, float("nan")
    k = max(1, math.ceil(len(y_true) * fraction))
    top_idx = np.argsort(y_score)[::-1][:k]
    precision = float(np.mean(y_true[top_idx]))
    return k, precision


def build_folds(total_rows: int, train_size: int, test_size: int, step_size: int) -> list[tuple[int, int, int, int]]:
    folds: list[tuple[int, int, int, int]] = []
    start = 0
    while start + train_size + test_size <= total_rows:
        train_start = start
        train_end = start + train_size
        test_start = train_end
        test_end = test_start + test_size
        folds.append((train_start, train_end, test_start, test_end))
        start += step_size
    return folds


def append_csv_rows(csv_path: str, rows: list[dict[str, object]]) -> None:
    if not csv_path:
        return

    directory = os.path.dirname(csv_path)
    if directory:
        os.makedirs(directory, exist_ok=True)

    fieldnames = [
        "run_id",
        "run_timestamp_utc",
        "row_type",
        "label_table",
        "target_name",
        "model",
        "feature_set",
        "top_fraction",
        "train_fraction",
        "test_fraction",
        "step_fraction",
        "total_rows",
        "fold_index",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
        "train_pos_rate",
        "test_pos_rate",
        "roc_auc",
        "pr_auc",
        "precision_top",
        "roc_mean",
        "roc_std",
        "roc_min",
        "roc_max",
        "pr_mean",
        "pr_std",
        "pr_min",
        "pr_max",
        "ptop_mean",
        "ptop_std",
        "ptop_min",
        "ptop_max",
        "baseline_roc_mean",
        "baseline_ptop_mean",
        "roc_delta_vs_baseline",
        "ptop_delta_vs_baseline",
        "decision",
        "decision_reason",
    ]

    file_exists = os.path.exists(csv_path)
    with open(csv_path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Walk-forward strict time-split logistic baseline")
    parser.add_argument("--label-table", type=str, default="labels_5m")
    parser.add_argument("--target-name", type=str, default="")
    parser.add_argument("--top-fraction", type=float, default=0.10)
    parser.add_argument("--train-fraction", type=float, default=0.60)
    parser.add_argument("--test-fraction", type=float, default=0.20)
    parser.add_argument("--step-fraction", type=float, default=0.20)
    parser.add_argument("--model", type=str, default="logistic", choices=["logistic", "xgboost"])
    parser.add_argument("--feature-set", type=str, default="base", choices=["base", "extended", "cross_rank"])
    parser.add_argument("--csv-path", type=str, default="research/walkforward_runs.csv")
    parser.add_argument("--baseline-roc-mean", type=float, default=float("nan"))
    parser.add_argument("--baseline-ptop-mean", type=float, default=float("nan"))
    parser.add_argument("--max-roc-drop", type=float, default=0.01)
    parser.add_argument("--min-ptop-delta", type=float, default=0.0)
    args = parser.parse_args()

    if not (0.0 < args.top_fraction <= 1.0):
        raise ValueError("--top-fraction must be between 0 and 1")
    if not (0.0 < args.train_fraction < 1.0):
        raise ValueError("--train-fraction must be between 0 and 1")
    if not (0.0 < args.test_fraction < 1.0):
        raise ValueError("--test-fraction must be between 0 and 1")
    if not (0.0 < args.step_fraction <= 1.0):
        raise ValueError("--step-fraction must be between 0 and 1")
    if args.max_roc_drop < 0:
        raise ValueError("--max-roc-drop must be >= 0")

    load_dotenv()
    dataset = load_dataset(
        label_table=args.label_table,
        target_name=(args.target_name.strip() or None),
        feature_set=args.feature_set,
    )

    total_rows = len(dataset.labels)
    train_size = int(total_rows * args.train_fraction)
    test_size = int(total_rows * args.test_fraction)
    step_size = int(total_rows * args.step_fraction)

    if train_size <= 0 or test_size <= 0 or step_size <= 0:
        raise ValueError("Fractions produced zero-sized windows; increase dataset size or fractions")

    folds = build_folds(total_rows, train_size, test_size, step_size)
    if not folds:
        raise ValueError("No folds can be formed with current train/test/step fractions")

    print(
        f"MODEL={args.model} TOTAL_ROWS={total_rows} TRAIN_SIZE={train_size} TEST_SIZE={test_size} "
        f"STEP_SIZE={step_size} FOLDS={len(folds)}"
    )

    roc_values: list[float] = []
    pr_values: list[float] = []
    ptop_values: list[float] = []
    coef_values: list[np.ndarray] = []
    csv_rows: list[dict[str, object]] = []
    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    run_timestamp_utc = datetime.utcnow().isoformat()
    target_name_text = args.target_name.strip()

    for fold_idx, (train_start, train_end, test_start, test_end) in enumerate(folds, start=1):
        x_train = dataset.features[train_start:train_end]
        y_train = dataset.labels[train_start:train_end]
        x_test = dataset.features[test_start:test_end]
        y_test = dataset.labels[test_start:test_end]

        if args.model == "logistic":
            model = Pipeline(
                steps=[
                    ("scaler", StandardScaler()),
                    ("clf", LogisticRegression(class_weight="balanced", max_iter=1000, solver="lbfgs")),
                ]
            )
            model.fit(x_train, y_train)
        else:
            pos_count = int(np.sum(y_train))
            neg_count = int(len(y_train) - pos_count)
            scale_pos_weight = (neg_count / pos_count) if pos_count > 0 else 1.0
            model = XGBClassifier(
                objective="binary:logistic",
                eval_metric="logloss",
                n_estimators=250,
                learning_rate=0.05,
                max_depth=3,
                min_child_weight=10,
                subsample=0.8,
                colsample_bytree=0.8,
                reg_lambda=1.0,
                random_state=42,
                n_jobs=1,
                scale_pos_weight=scale_pos_weight,
            )
            model.fit(x_train, y_train)

        prob_test = model.predict_proba(x_test)[:, 1]

        try:
            roc_auc = float(roc_auc_score(y_test, prob_test))
        except ValueError:
            roc_auc = float("nan")

        try:
            pr_auc = float(average_precision_score(y_test, prob_test))
        except ValueError:
            pr_auc = float("nan")

        _, precision_top = precision_at_top_fraction(y_test, prob_test, args.top_fraction)

        train_pos_rate = float(np.mean(y_train)) if len(y_train) else float("nan")
        test_pos_rate = float(np.mean(y_test)) if len(y_test) else float("nan")

        train_ts_start = format_ts(dataset.bucket_timestamps[train_start])
        train_ts_end = format_ts(dataset.bucket_timestamps[train_end - 1])
        test_ts_start = format_ts(dataset.bucket_timestamps[test_start])
        test_ts_end = format_ts(dataset.bucket_timestamps[test_end - 1])

        print(
            f"FOLD={fold_idx} TRAIN_RANGE=[{train_start},{train_end}) TEST_RANGE=[{test_start},{test_end}) "
            f"TRAIN_TS=[{train_ts_start} -> {train_ts_end}] TEST_TS=[{test_ts_start} -> {test_ts_end}] "
            f"TRAIN_POS_RATE={train_pos_rate:.4f} TEST_POS_RATE={test_pos_rate:.4f} "
            f"ROC_AUC={roc_auc:.4f} PR_AUC={pr_auc:.4f} "
            f"PRECISION_AT_TOP{int(args.top_fraction * 100)}PCT={precision_top:.4f}"
        )

        csv_rows.append(
            {
                "run_id": run_id,
                "run_timestamp_utc": run_timestamp_utc,
                "row_type": "fold",
                "label_table": args.label_table,
                "target_name": target_name_text,
                "model": args.model,
                "feature_set": args.feature_set,
                "top_fraction": args.top_fraction,
                "train_fraction": args.train_fraction,
                "test_fraction": args.test_fraction,
                "step_fraction": args.step_fraction,
                "total_rows": total_rows,
                "fold_index": fold_idx,
                "train_start": train_start,
                "train_end": train_end,
                "test_start": test_start,
                "test_end": test_end,
                "train_pos_rate": train_pos_rate,
                "test_pos_rate": test_pos_rate,
                "roc_auc": roc_auc,
                "pr_auc": pr_auc,
                "precision_top": precision_top,
                "roc_mean": "",
                "roc_std": "",
                "roc_min": "",
                "roc_max": "",
                "pr_mean": "",
                "pr_std": "",
                "pr_min": "",
                "pr_max": "",
                "ptop_mean": "",
                "ptop_std": "",
                "ptop_min": "",
                "ptop_max": "",
                "baseline_roc_mean": "",
                "baseline_ptop_mean": "",
                "roc_delta_vs_baseline": "",
                "ptop_delta_vs_baseline": "",
                "decision": "",
                "decision_reason": "",
            }
        )

        roc_values.append(roc_auc)
        pr_values.append(pr_auc)
        ptop_values.append(precision_top)

        if args.model == "logistic":
            clf: LogisticRegression = model.named_steps["clf"]
            coef_values.append(clf.coef_[0].copy())

    roc_arr = np.asarray(roc_values, dtype=np.float64)
    pr_arr = np.asarray(pr_values, dtype=np.float64)
    ptop_arr = np.asarray(ptop_values, dtype=np.float64)

    print(
        f"SUMMARY ROC_MEAN={np.nanmean(roc_arr):.4f} ROC_STD={np.nanstd(roc_arr):.4f} "
        f"ROC_MIN={np.nanmin(roc_arr):.4f} ROC_MAX={np.nanmax(roc_arr):.4f}"
    )
    print(
        f"SUMMARY PR_MEAN={np.nanmean(pr_arr):.4f} PR_STD={np.nanstd(pr_arr):.4f} "
        f"PR_MIN={np.nanmin(pr_arr):.4f} PR_MAX={np.nanmax(pr_arr):.4f}"
    )
    print(
        f"SUMMARY P@TOP{int(args.top_fraction * 100)}_MEAN={np.nanmean(ptop_arr):.4f} "
        f"P@TOP{int(args.top_fraction * 100)}_STD={np.nanstd(ptop_arr):.4f} "
        f"P@TOP{int(args.top_fraction * 100)}_MIN={np.nanmin(ptop_arr):.4f} "
        f"P@TOP{int(args.top_fraction * 100)}_MAX={np.nanmax(ptop_arr):.4f}"
    )

    roc_mean = float(np.nanmean(roc_arr))
    roc_std = float(np.nanstd(roc_arr))
    roc_min = float(np.nanmin(roc_arr))
    roc_max = float(np.nanmax(roc_arr))
    pr_mean = float(np.nanmean(pr_arr))
    pr_std = float(np.nanstd(pr_arr))
    pr_min = float(np.nanmin(pr_arr))
    pr_max = float(np.nanmax(pr_arr))
    ptop_mean = float(np.nanmean(ptop_arr))
    ptop_std = float(np.nanstd(ptop_arr))
    ptop_min = float(np.nanmin(ptop_arr))
    ptop_max = float(np.nanmax(ptop_arr))

    baseline_roc_mean = float(args.baseline_roc_mean)
    baseline_ptop_mean = float(args.baseline_ptop_mean)
    has_roc_baseline = not np.isnan(baseline_roc_mean)
    has_ptop_baseline = not np.isnan(baseline_ptop_mean)

    if has_roc_baseline:
        roc_delta_vs_baseline = roc_mean - baseline_roc_mean
    else:
        roc_delta_vs_baseline = float("nan")

    if has_ptop_baseline:
        ptop_delta_vs_baseline = ptop_mean - baseline_ptop_mean
    else:
        ptop_delta_vs_baseline = float("nan")

    if has_roc_baseline or has_ptop_baseline:
        tolerance = 5e-4
        ptop_ok = True
        roc_ok = True
        if has_ptop_baseline:
            ptop_ok = ptop_delta_vs_baseline >= (args.min_ptop_delta - tolerance)
        if has_roc_baseline:
            roc_ok = roc_delta_vs_baseline >= (-args.max_roc_drop - tolerance)

        decision = "KEEP" if (ptop_ok and roc_ok) else "REJECT"
        decision_parts: list[str] = []
        if has_ptop_baseline:
            decision_parts.append(
                f"PTOP_DELTA={ptop_delta_vs_baseline:.4f} (min_required={args.min_ptop_delta:.4f})"
            )
        if has_roc_baseline:
            decision_parts.append(
                f"ROC_DELTA={roc_delta_vs_baseline:.4f} (max_drop={args.max_roc_drop:.4f})"
            )
        decision_reason = "; ".join(decision_parts)
        print(f"RECOMMENDATION={decision} {decision_reason}")
    else:
        decision = "N/A"
        decision_reason = "No baseline means supplied (--baseline-roc-mean / --baseline-ptop-mean)."
        print(f"RECOMMENDATION={decision} {decision_reason}")

    csv_rows.append(
        {
            "run_id": run_id,
            "run_timestamp_utc": run_timestamp_utc,
            "row_type": "summary",
            "label_table": args.label_table,
            "target_name": target_name_text,
            "model": args.model,
            "feature_set": args.feature_set,
            "top_fraction": args.top_fraction,
            "train_fraction": args.train_fraction,
            "test_fraction": args.test_fraction,
            "step_fraction": args.step_fraction,
            "total_rows": total_rows,
            "fold_index": len(folds),
            "train_start": "",
            "train_end": "",
            "test_start": "",
            "test_end": "",
            "train_pos_rate": "",
            "test_pos_rate": "",
            "roc_auc": "",
            "pr_auc": "",
            "precision_top": "",
            "roc_mean": roc_mean,
            "roc_std": roc_std,
            "roc_min": roc_min,
            "roc_max": roc_max,
            "pr_mean": pr_mean,
            "pr_std": pr_std,
            "pr_min": pr_min,
            "pr_max": pr_max,
            "ptop_mean": ptop_mean,
            "ptop_std": ptop_std,
            "ptop_min": ptop_min,
            "ptop_max": ptop_max,
            "baseline_roc_mean": "" if np.isnan(baseline_roc_mean) else baseline_roc_mean,
            "baseline_ptop_mean": "" if np.isnan(baseline_ptop_mean) else baseline_ptop_mean,
            "roc_delta_vs_baseline": "" if np.isnan(roc_delta_vs_baseline) else roc_delta_vs_baseline,
            "ptop_delta_vs_baseline": "" if np.isnan(ptop_delta_vs_baseline) else ptop_delta_vs_baseline,
            "decision": decision,
            "decision_reason": decision_reason,
        }
    )

    append_csv_rows(args.csv_path, csv_rows)
    if args.csv_path:
        print(f"CSV_LOG_APPENDED={args.csv_path} RUN_ID={run_id} ROWS={len(csv_rows)}")

    if args.model == "logistic":
        coef_arr = np.vstack(coef_values)
        sign_arr = np.sign(coef_arr)
        print("COEF_SIGN_STABILITY")
        for idx, name in enumerate(dataset.feature_names):
            positive_folds = int(np.sum(sign_arr[:, idx] > 0))
            negative_folds = int(np.sum(sign_arr[:, idx] < 0))
            zero_folds = int(np.sum(sign_arr[:, idx] == 0))
            print(
                f"FEATURE={name} POS_FOLDS={positive_folds} NEG_FOLDS={negative_folds} ZERO_FOLDS={zero_folds} "
                f"COEF_MEAN={np.mean(coef_arr[:, idx]):.6f} COEF_STD={np.std(coef_arr[:, idx]):.6f}"
            )
    else:
        print("COEF_SIGN_STABILITY_SKIPPED model=xgboost")


if __name__ == "__main__":
    main()
