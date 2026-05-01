from __future__ import annotations

import argparse
import csv
import math
import os
import json
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


BASE_FEATURES = [
    "volume_velocity",
    "buy_sell_ratio",
    "trade_intensity",
    "wallet_growth_delta",
]

CROSS_RANK_FEATURES = BASE_FEATURES + [
    "volume_velocity_rank_pct",
    "buy_sell_ratio_rank_pct",
    "trade_intensity_rank_pct",
    "volume_relative_to_median",
]

V2_FEATURES = CROSS_RANK_FEATURES + [
    "market_momentum_regime",
    "order_flow_imbalance",
    "return_1h",
    "volume_accel",
    "time_since_launch_log",
]

MOMENTUM_PLUS_FEATURES = V2_FEATURES + [
    "relative_momentum",
    "volume_shock",
    "macd_proxy",
    "rsi_14",
    "momentum_acceleration",
    "rvol_5m",
    "momentum_15m",
    "momentum_30m",
    "momentum_accel",
    "buy_pressure",
    "rvol_rank_pct",
    "momentum_15m_rank_pct",
    "momentum_30m_rank_pct",
    "momentum_accel_rank_pct",
    "buy_pressure_rank_pct",
    "relative_momentum_rank_pct"
]

FEATURE_SETS = {
    "base": BASE_FEATURES,
    "cross_rank": CROSS_RANK_FEATURES,
    "v2": V2_FEATURES,
    "momentum_plus": MOMENTUM_PLUS_FEATURES,
}

SKEWED_FEATURES = {
    "buy_sell_ratio",
    "wallet_growth_delta",
    "volume_accel",
    "volume_relative_to_median",
}


LABEL_TARGETS = {
    "fixed": "l.target_up_5pct_2h::INTEGER",
    "adaptive": "l.target_adaptive_top20::INTEGER",
}


@dataclass
class Dataset:
    features: np.ndarray
    labels: np.ndarray
    regime_values: np.ndarray
    bucket_timestamps: np.ndarray
    chains: np.ndarray
    feature_names: list[str]


def get_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing env variable: {name}")
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
    feature_sql = ",\n".join(feature_columns)

    sql = f"""
        SELECT
            {feature_sql},
            {label_sql},
            f.market_momentum_regime::DOUBLE PRECISION,
            f.bucket_timestamp,
            COALESCE(t.chain, 'base')
        FROM features_5m f
        INNER JOIN labels_5m l
            ON f.token_address=l.token_address
            AND f.bucket_timestamp=l.bucket_timestamp
        INNER JOIN tokens t
            ON f.token_address = t.token_address
        WHERE {label_sql} IN (0,1)
        ORDER BY f.bucket_timestamp ASC
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
            cursor.execute(sql)
            rows = cursor.fetchall()

    n_feat = len(feature_names)

    return Dataset(
        features=np.asarray([r[:n_feat] for r in rows], dtype=np.float64),
        labels=np.asarray([r[n_feat] for r in rows], dtype=np.int32),
        regime_values=np.asarray([r[n_feat+1] for r in rows], dtype=np.float64),
        bucket_timestamps=np.asarray([r[n_feat+2] for r in rows], dtype=object),
        chains=np.asarray([r[n_feat+3] for r in rows], dtype=object),
        feature_names=feature_names,
    )


def robust_preprocess(x_train, x_test, feature_names):

    train_cols=[]
    test_cols=[]
    out_names=[]

    for i,name in enumerate(feature_names):

        tr=x_train[:,i].copy()
        te=x_test[:,i].copy()

        tr=np.nan_to_num(tr)
        te=np.nan_to_num(te)

        lo=np.percentile(tr,1)
        hi=np.percentile(tr,99)

        tr=np.clip(tr,lo,hi)
        te=np.clip(te,lo,hi)

        train_cols.append(tr)
        test_cols.append(te)
        out_names.append(name)

        zero_rate=np.mean(tr==0)

        if zero_rate>=0.15:

            train_cols.append((x_train[:,i]==0).astype(float))
            test_cols.append((x_test[:,i]==0).astype(float)

            )
            out_names.append(name+"_is_zero")

        if name in SKEWED_FEATURES:

            train_cols.append(np.log1p(np.clip(tr,0,None)))
            test_cols.append(np.log1p(np.clip(te,0,None)))
            out_names.append(name+"_log1p")

    return np.column_stack(train_cols),np.column_stack(test_cols),out_names


def precision_at_top_frac(y_true,y_score,frac):

    if len(y_true)==0:
        return float("nan")

    k=max(1,math.ceil(len(y_true)*frac))

    idx=np.argsort(y_score)[::-1][:k]

    return float(np.mean(y_true[idx]))


def make_logistic():

    return Pipeline([
        ("scaler",StandardScaler()),
        ("clf",LogisticRegression(class_weight="balanced",max_iter=1000))
    ])

def make_random_forest(y_train):

    pos=int(np.sum(y_train))
    neg=len(y_train)-pos

    spw=neg/pos if pos>0 else 1.0

    n_rows = len(y_train)
    if n_rows < 600:
        depth, est = 3, 50
    elif n_rows < 1000:
        depth, est = 4, 100
    else:
        depth, est = 10, 200

    return RandomForestClassifier(
        n_estimators=est,
        max_depth=depth,
        min_samples_leaf=3,
        n_jobs=-1,
        class_weight={0:1,1:spw},
        random_state=42
    )

def make_extratrees(y_train):

    pos=int(np.sum(y_train))
    neg=len(y_train)-pos

    spw=neg/pos if pos>0 else 1.0

    n_rows = len(y_train)
    if n_rows < 600:
        depth, est = 3, 50
    elif n_rows < 1000:
        depth, est = 4, 100
    else:
        depth, est = 10, 200

    return ExtraTreesClassifier(
        n_estimators=est,
        max_depth=depth,
        min_samples_leaf=3,
        n_jobs=-1,
        class_weight={0:1,1:spw},
        random_state=42
    )

def make_xgboost(y_train,**overrides):

    pos=int(np.sum(y_train))
    neg=len(y_train)-pos

    spw=neg/pos if pos>0 else 1.0

    n_rows = len(y_train)
    if n_rows < 600:
        depth, est, lr = 3, 50, 0.05
    elif n_rows < 1000:
        depth, est, lr = 4, 100, 0.04
    else:
        depth, est, lr = int(min(10, 5)), 200, 0.03

    params=dict(
        objective="binary:logistic",
        eval_metric="logloss",
        n_estimators=est,
        learning_rate=lr,
        max_depth=depth,
        subsample=0.7,
        colsample_bytree=0.7,
        scale_pos_weight=spw,
        n_jobs=1,
    )

    params.update(overrides)

    return XGBClassifier(**params)

def tune_xgboost(x,y,top_frac=0.1):

    params_list=[
        {"max_depth":3,"learning_rate":0.05},
        {"max_depth":4,"learning_rate":0.05},
        {"max_depth":3,"learning_rate":0.03},
        {"max_depth":5,"learning_rate":0.03}
    ]

    best=None
    best_score=-1

    tscv=TimeSeriesSplit(n_splits=3, gap=10)

    for p in params_list:

        scores=[]

        for tr,te in tscv.split(x):

            m=make_xgboost(y[tr],**p)
            m.fit(x[tr],y[tr])

            pr=m.predict_proba(x[te])[:,1]

            scores.append(precision_at_top_frac(y[te],pr,top_frac))

        s=np.mean(scores)

        if s>best_score:
            best_score=s
            best=p

    if best is None:
        best={}

    return best


# ← REMOVED: apply_smote_if_needed() deleted entirely.
# Imbalance is handled natively by scale_pos_weight in XGBoost
# and class_weight in RandomForest / ExtraTrees.
# SMOTE was causing score clustering (most tokens outputting 0.44)
# because synthetic samples dominated the small per-chain datasets.


def stacking_oof_predictions(
    x,
    y,
    n_folds=4,
    sample_weights=None,
    robust_fold_preprocess=False,
    feature_names=None
):

    n=len(y)
    oof=np.zeros((n,4))
    has_pred=np.zeros(n,dtype=bool)

    tscv=TimeSeriesSplit(n_splits=n_folds, gap=10)

    for tr,te in tscv.split(x):

        xtr=x[tr]
        xte=x[te]

        if robust_fold_preprocess:
            xtr,xte,_=robust_preprocess(xtr,xte,feature_names)

        ytr=y[tr]

        sw=None
        if sample_weights is not None:
            sw=sample_weights[tr]

        lr=make_logistic()
        xgb=make_xgboost(ytr)
        rf=make_random_forest(ytr)
        et=make_extratrees(ytr)

        if sw is not None:
            lr.fit(xtr,ytr,clf__sample_weight=sw)
            xgb.fit(xtr,ytr,sample_weight=sw)
            rf.fit(xtr,ytr,sample_weight=sw)
            et.fit(xtr,ytr,sample_weight=sw)
        else:
            lr.fit(xtr,ytr)
            xgb.fit(xtr,ytr)
            rf.fit(xtr,ytr)
            et.fit(xtr,ytr)

        oof[te,0]=lr.predict_proba(xte)[:,1]
        oof[te,1]=xgb.predict_proba(xte)[:,1]
        oof[te,2]=rf.predict_proba(xte)[:,1]
        oof[te,3]=et.predict_proba(xte)[:,1]

        has_pred[te]=True

    return oof,has_pred

def main():

    parser=argparse.ArgumentParser()

    parser.add_argument("--model",default="stacking")
    parser.add_argument("--feature-set",default="momentum_plus")
    parser.add_argument("--label-target",default="adaptive")

    args=parser.parse_args()

    load_dotenv()

    dataset=load_dataset(args.feature_set,args.label_target)

    total=len(dataset.labels)

    all_chains = np.unique(dataset.chains)
    per_chain_results = {}
    global_feat_imp = {}

    for chain in all_chains:
        idx = np.where(dataset.chains == chain)[0]
        if len(idx) < 100:
            print(f"Skipping {chain} - too few samples ({len(idx)})")
            continue

        print(f"\n--- Training {chain.upper()} Model ({len(idx)} rows) ---")
        x_c = dataset.features[idx]
        y_c = dataset.labels[idx]

        model = make_xgboost(y_c)
        model.fit(x_c, y_c)

        prob = model.predict_proba(x_c)[:, 1]
        auc = roc_auc_score(y_c, prob)
        print(f"{chain.upper()} Training ROC-AUC: {auc:.4f}")

        imp = model.feature_importances_
        feat_imp = {n: float(imp[i]) for i, n in enumerate(dataset.feature_names)}

        per_chain_results[chain] = {
            "auc": float(auc),
            "trainRows": int(len(idx)),
            "features": feat_imp
        }

        for n, v in feat_imp.items():
            global_feat_imp[n] = global_feat_imp.get(n, 0.0) + v / len(all_chains)

    out = {
        "timestamp": datetime.utcnow().isoformat(),
        "model": args.model,
        "featureSet": args.feature_set,
        "trainRows": int(total),
        "features": global_feat_imp,
        "perChain": per_chain_results
    }

    with open("research/feature_importance.json", "w") as f:
        json.dump(out, f, indent=2)
    print("\nSaved research/feature_importance.json")


if __name__=="__main__":
    main()