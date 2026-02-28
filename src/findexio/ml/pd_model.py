from __future__ import annotations

import json
import logging
import pickle
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from psycopg.rows import dict_row
from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from ..db import get_conn

log = logging.getLogger("findexio.ml_pd_prod")

FEATURES_NUM = [
    "current_ratio", "quick_ratio", "cash_ratio",
    "equity_ratio", "debt_ratio", "debt_to_equity",
    "roa", "roe", "net_margin", "asset_turnover",
    "interest_coverage",
    "x04_noncurrent_indebtedness", "x07_interest_burden",
    "x08_debt_to_cf", "x09_equity_leverage", "x10_insolvency",
    "model_sk_raw", "model_sk_pct",
]
FEATURES_BOOL = [
    "negative_equity_flag", "liquidity_breach_flag", "high_leverage_flag", "loss_flag"
]
TARGET_COL = "default_within_12m"

SQL_FETCH_LABELED = """
SELECT
  report_id, ico, fiscal_year, period_end,
  {cols},
  {target}::int AS y
FROM {view}
"""

SQL_FETCH_SCORE = """
SELECT
  report_id, ico, fiscal_year, period_end,
  {cols}
FROM {view}
"""


def _fetch_labeled(view: str) -> pd.DataFrame:
    cols = ", ".join(FEATURES_NUM + FEATURES_BOOL)
    q = SQL_FETCH_LABELED.format(cols=cols, target=TARGET_COL, view=view)
    with get_conn() as conn:
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(q)
        rows = cur.fetchall()
    return pd.DataFrame(rows)


def _fetch_score(view: str) -> pd.DataFrame:
    cols = ", ".join(FEATURES_NUM + FEATURES_BOOL)
    q = SQL_FETCH_SCORE.format(cols=cols, view=view)
    with get_conn() as conn:
        cur = conn.cursor(row_factory=dict_row)
        cur.execute(q)
        rows = cur.fetchall()
    return pd.DataFrame(rows)


def _prepare_Xy(df: pd.DataFrame) -> Tuple[pd.DataFrame, np.ndarray]:
    X = df[FEATURES_NUM + FEATURES_BOOL].copy()
    for c in FEATURES_BOOL:
        X[c] = X[c].astype("float64")
    y = df["y"].to_numpy(dtype=int)
    return X, y


def _prepare_X(df: pd.DataFrame) -> pd.DataFrame:
    X = df[FEATURES_NUM + FEATURES_BOOL].copy()
    for c in FEATURES_BOOL:
        X[c] = X[c].astype("float64")
    return X


def _recall_at_top_k(y_true: np.ndarray, y_score: np.ndarray, top_frac: float) -> float:
    k = max(1, int(np.ceil(len(y_true) * top_frac)))
    idx = np.argsort(-y_score)[:k]
    tp = y_true[idx].sum()
    total_pos = y_true.sum()
    return float(tp / total_pos) if total_pos > 0 else 0.0


def _metrics(y: np.ndarray, p: np.ndarray) -> Dict[str, Any]:
    return {
        "roc_auc": float(roc_auc_score(y, p)) if len(np.unique(y)) > 1 else None,
        "pr_auc": float(average_precision_score(y, p)) if len(np.unique(y)) > 1 else None,
        "recall_top_1pct": _recall_at_top_k(y, p, 0.01),
        "recall_top_5pct": _recall_at_top_k(y, p, 0.05),
        "n": int(len(y)),
        "n_pos": int(y.sum()),
    }


def _build_logreg() -> Pipeline:
    pre = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])
    clf = LogisticRegression(
        max_iter=2000,
        class_weight="balanced",
        solver="lbfgs",
    )
    return Pipeline(steps=[("pre", pre), ("clf", clf)])


def _build_hgb() -> Pipeline:
    pre = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
    ])
    clf = HistGradientBoostingClassifier(
        max_depth=6,
        learning_rate=0.05,
        max_iter=400,
        random_state=42,
    )
    return Pipeline(steps=[("pre", pre), ("clf", clf)])


def _insert_model(
    *,
    name: str,
    horizon: str,
    algo: str,
    trained_from: int,
    trained_to: int,
    feature_list: List[str],
    metrics: Dict[str, Any],
    pipe: Any,  # Pipeline or CalibratedClassifierCV
) -> int:
    blob = pickle.dumps(pipe)
    sql = """
    INSERT INTO core.ml_model_registry
      (name, horizon, algo, trained_from, trained_to, feature_list, metrics, model_blob)
    VALUES
      (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
    RETURNING id
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            sql,
            (
                name, horizon, algo, trained_from, trained_to,
                json.dumps(feature_list),
                json.dumps(metrics),
                blob,
            ),
        )
        model_id = cur.fetchone()[0]
        conn.commit()
    return int(model_id)


def _upsert_predictions(df_meta: pd.DataFrame, pd_hat: np.ndarray, model_id: int) -> None:
    sql = """
    INSERT INTO core.ml_pd_predictions (report_id, ico, fiscal_year, period_end, pd_12m, model_id)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (report_id) DO UPDATE SET
      ico = EXCLUDED.ico,
      fiscal_year = EXCLUDED.fiscal_year,
      period_end = EXCLUDED.period_end,
      pd_12m = EXCLUDED.pd_12m,
      model_id = EXCLUDED.model_id,
      created_at = now()
    """
    rows = [
        (
            int(r.report_id),
            str(r.ico),
            int(r.fiscal_year),
            r.period_end,
            float(p),
            int(model_id),
        )
        for r, p in zip(df_meta.itertuples(index=False), pd_hat)
    ]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.executemany(sql, rows)
        conn.commit()


def run() -> None:
    # Load splits
    df_tr = _fetch_labeled("core.ml_train_set")
    df_va = _fetch_labeled("core.ml_valid_set")
    df_te = _fetch_labeled("core.ml_test_set")

    Xtr, ytr = _prepare_Xy(df_tr)
    Xva, yva = _prepare_Xy(df_va)
    Xte, yte = _prepare_Xy(df_te)

    n_pos = int(ytr.sum())
    n_neg = int(len(ytr) - n_pos)
    pos_weight = n_neg / max(1, n_pos)
    log.info("Train n=%d pos=%d neg=%d pos_weight=%.2f", len(ytr), n_pos, n_neg, pos_weight)

    # ---- Train base models (raw) on TRAIN only ----
    lr = _build_logreg()
    lr.fit(Xtr, ytr)

    hgb = _build_hgb()
    sw_tr = np.where(ytr == 1, pos_weight, 1.0)
    hgb.fit(Xtr, ytr, clf__sample_weight=sw_tr)

    # ---- Evaluate raw on train/valid/test ----
    lr_raw = {
        "train": _metrics(ytr, lr.predict_proba(Xtr)[:, 1]),
        "valid": _metrics(yva, lr.predict_proba(Xva)[:, 1]),
        "test": _metrics(yte, lr.predict_proba(Xte)[:, 1]),
    }
    hgb_raw = {
        "train": _metrics(ytr, hgb.predict_proba(Xtr)[:, 1]),
        "valid": _metrics(yva, hgb.predict_proba(Xva)[:, 1]),
        "test": _metrics(yte, hgb.predict_proba(Xte)[:, 1]),
    }

    log.info("LogReg RAW metrics: %s", json.dumps(lr_raw, ensure_ascii=False))
    log.info("HGB   RAW metrics: %s", json.dumps(hgb_raw, ensure_ascii=False))

    # ---- Select best by VALID PR AUC (raw) ----
    pr_lr = lr_raw["valid"]["pr_auc"] or -1.0
    pr_hgb = hgb_raw["valid"]["pr_auc"] or -1.0

    best_algo = "hgb" if pr_hgb >= pr_lr else "logreg"
    best_raw_metrics = hgb_raw if best_algo == "hgb" else lr_raw

    # ---- Refit chosen base estimator on TRAIN+VALID ----
    df_trva = pd.concat([df_tr, df_va], ignore_index=True)
    Xtrva, ytrva = _prepare_Xy(df_trva)

    trained_from = int(df_trva["fiscal_year"].min())
    trained_to = int(df_trva["fiscal_year"].max())

    if best_algo == "hgb":
        best_base = _build_hgb()
        n_pos_tv = int(ytrva.sum())
        n_neg_tv = int(len(ytrva) - n_pos_tv)
        pos_weight_tv = n_neg_tv / max(1, n_pos_tv)
        sw_trva = np.where(ytrva == 1, pos_weight_tv, 1.0)

        # Fit base on all train+valid
        best_base.fit(Xtrva, ytrva, clf__sample_weight=sw_trva)

        # ---- Calibrate with CV=5 on train+valid (sigmoid is stable) ----
        cal_method = "sigmoid"
        calibrated = CalibratedClassifierCV(
            estimator=best_base,
            method=cal_method,
            cv=5,
        )
        calibrated.fit(Xtrva, ytrva, clf__sample_weight=sw_trva)

    else:
        best_base = _build_logreg()
        best_base.fit(Xtrva, ytrva)

        cal_method = "sigmoid"
        calibrated = CalibratedClassifierCV(
            estimator=best_base,
            method=cal_method,
            cv=5,
        )
        calibrated.fit(Xtrva, ytrva)

    # ---- Evaluate calibrated on TEST (and optionally on train/valid too) ----
    cal_metrics = {
        "train_valid": _metrics(ytrva, calibrated.predict_proba(Xtrva)[:, 1]),
        "test": _metrics(yte, calibrated.predict_proba(Xte)[:, 1]),
    }

    log.info("BEST algo=%s CAL(cv=5,%s) metrics: %s", best_algo, cal_method, json.dumps(cal_metrics, ensure_ascii=False))

    metrics_payload = {
        "selection_rule": "best_by_valid_pr_auc_raw",
        "best_algo": best_algo,
        "calibration": {"method": cal_method, "cv": 5, "fit_on": "train_plus_valid"},
        "raw": best_raw_metrics,
        "calibrated": cal_metrics,
    }

    # ---- Persist calibrated model ----
    model_id = _insert_model(
        name="pd_default",
        horizon="12m",
        algo=f"{best_algo}+cal_cv5({cal_method})",
        trained_from=trained_from,
        trained_to=trained_to,
        feature_list=FEATURES_NUM + FEATURES_BOOL,
        metrics=metrics_payload,
        pipe=calibrated,
    )
    log.info("Registered calibrated model: algo=%s id=%d", best_algo, model_id)

    # ---- Score population using calibrated PD ----
    df_sc = _fetch_score("core.ml_score_set")
    Xsc = _prepare_X(df_sc)
    pd_hat = calibrated.predict_proba(Xsc)[:, 1]

    meta = df_sc[["report_id", "ico", "fiscal_year", "period_end"]].copy()
    _upsert_predictions(meta, pd_hat, model_id)

    log.info("Scored %d rows into core.ml_pd_predictions (model_id=%d).", len(df_sc), model_id)