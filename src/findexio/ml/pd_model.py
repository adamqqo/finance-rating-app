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


def _calibrate_prefit(
    fitted_estimator: Any,
    X_valid: pd.DataFrame,
    y_valid: np.ndarray,
    *,
    method: str,
) -> CalibratedClassifierCV:
    """
    Calibrate an already-fitted estimator using the validation set only.
    """
    calibrator = CalibratedClassifierCV(
        estimator=fitted_estimator,
        method=method,
        cv="prefit",
    )
    calibrator.fit(X_valid, y_valid)
    return calibrator


def run() -> None:
    # Load splits (already filtered by your SQL views)
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

    # ---- Train base models (raw) ----
    lr = _build_logreg()
    lr.fit(Xtr, ytr)

    hgb = _build_hgb()
    sample_weight = np.where(ytr == 1, pos_weight, 1.0)
    hgb.fit(Xtr, ytr, clf__sample_weight=sample_weight)

    # ---- Evaluate raw ----
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

    # ---- Pick best by VALID PR AUC (raw) ----
    pr_lr = lr_raw["valid"]["pr_auc"] or -1.0
    pr_hgb = hgb_raw["valid"]["pr_auc"] or -1.0

    best_algo = "hgb" if pr_hgb >= pr_lr else "logreg"
    best_base = hgb if best_algo == "hgb" else lr
    best_raw_metrics = hgb_raw if best_algo == "hgb" else lr_raw

    # ---- Calibrate on VALID only ----
    # Trees -> isotonic, LogReg -> sigmoid (more stable)
    cal_method = "isotonic" if best_algo == "hgb" else "sigmoid"
    calibrated = _calibrate_prefit(best_base, Xva, yva, method=cal_method)

    # ---- Evaluate calibrated (important for interpretability) ----
    best_cal_metrics = {
        "train": _metrics(ytr, calibrated.predict_proba(Xtr)[:, 1]),
        "valid": _metrics(yva, calibrated.predict_proba(Xva)[:, 1]),
        "test": _metrics(yte, calibrated.predict_proba(Xte)[:, 1]),
    }

    log.info("BEST algo=%s CAL(%s) metrics: %s", best_algo, cal_method, json.dumps(best_cal_metrics, ensure_ascii=False))

    trained_from = int(df_tr["fiscal_year"].min())
    trained_to = int(df_tr["fiscal_year"].max())

    # Store both raw + calibrated metrics for transparency
    metrics_payload = {
        "selection_rule": "best_by_valid_pr_auc_raw",
        "best_algo": best_algo,
        "calibration": {"method": cal_method, "fit_on": "valid_set_only"},
        "raw": best_raw_metrics,
        "calibrated": best_cal_metrics,
    }

    # Persist CALIBRATED model (so scoring uses calibrated probs)
    model_id = _insert_model(
        name="pd_default",
        horizon="12m",
        algo=f"{best_algo}+cal({cal_method})",
        trained_from=trained_from,
        trained_to=trained_to,
        feature_list=FEATURES_NUM + FEATURES_BOOL,
        metrics=metrics_payload,
        pipe=calibrated,
    )
    log.info("Registered calibrated model: algo=%s+cal(%s) id=%d", best_algo, cal_method, model_id)

    # ---- Score latest population using calibrated PD ----
    df_sc = _fetch_score("core.ml_score_set")
    Xsc = _prepare_X(df_sc)

    pd_hat = calibrated.predict_proba(Xsc)[:, 1]

    meta = df_sc[["report_id", "ico", "fiscal_year", "period_end"]].copy()
    _upsert_predictions(meta, pd_hat, model_id)

    log.info("Scored %d rows into core.ml_pd_predictions (model_id=%d).", len(df_sc), model_id)