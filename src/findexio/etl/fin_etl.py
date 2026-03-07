from __future__ import annotations

import logging
from typing import List, Sequence, Tuple

from ..db import get_conn

log = logging.getLogger("findexio.fin_etl")

# -----------------------------
# Tuning knobs
# -----------------------------
BATCH_SIZE_SINGLE = 5000   # 699 + 687 report_id batch
BATCH_SIZE_PAIRS = 5000    # number of (21,22) pairs per batch

# -----------------------------
# Rebuild truncates
# -----------------------------
SQL_TRUNCATE_ALL = """
TRUNCATE TABLE
  core.fin_annual_features,
  core.fin_annual_aggregates,
  core.fin_health_grade
RESTART IDENTITY;
"""

# -----------------------------
# Helper tables (unlogged)
# -----------------------------
SQL_DROP_HELPERS = """
DROP TABLE IF EXISTS core.fin_etl_queue_single;
DROP TABLE IF EXISTS core.fin_etl_pairs;
"""

SQL_CREATE_HELPERS = """
CREATE UNLOGGED TABLE IF NOT EXISTS core.fin_etl_queue_single (
  report_id bigint PRIMARY KEY
);

CREATE UNLOGGED TABLE IF NOT EXISTS core.fin_etl_pairs (
  bs_report_id bigint NOT NULL,
  is_report_id bigint NOT NULL,
  ico text NOT NULL,
  statement_id bigint NOT NULL,
  period_end date NOT NULL,
  norm_period smallint NOT NULL,
  PRIMARY KEY (bs_report_id, norm_period)
);

-- Helpful indexes for batch fetch
CREATE INDEX IF NOT EXISTS ix_fin_etl_pairs_is ON core.fin_etl_pairs (is_report_id);
CREATE INDEX IF NOT EXISTS ix_fin_etl_pairs_key ON core.fin_etl_pairs (ico, statement_id, period_end, norm_period);
"""

# -----------------------------
# Populate queues
# -----------------------------
# 699 + 687: queue report_ids directly (using minimal filters to avoid useless rows)
SQL_POPULATE_QUEUE_SINGLE = """
INSERT INTO core.fin_etl_queue_single (report_id)
SELECT DISTINCT i.report_id
FROM core.ruz_report_items i
WHERE i.template_id IN (699, 687)
ON CONFLICT DO NOTHING;
"""

# 21/22: build strict pairs into core.fin_etl_pairs
SQL_POPULATE_PAIRS_21_22 = """
WITH bs AS (
  SELECT DISTINCT
    i.report_id AS bs_report_id,
    i.ico,
    r.id_uctovnej_zavierky AS statement_id,
    core.parse_ruztxt_date(i.obdobie_do) AS period_end,
    CASE
      WHEN i.table_name = 'Strana aktív' AND i.period_col = 3 THEN 1
      WHEN i.table_name = 'Strana aktív' AND i.period_col = 4 THEN 2
      WHEN i.table_name <> 'Strana aktív' AND i.period_col = 1 THEN 1
      WHEN i.table_name <> 'Strana aktív' AND i.period_col = 2 THEN 2
    END AS norm_period
  FROM core.ruz_report_items i
  JOIN core.ruz_reports r ON r.id = i.report_id
  WHERE i.template_id = 21
    AND core.parse_ruztxt_date(i.obdobie_do) IS NOT NULL
),
isr AS (
  SELECT DISTINCT
    i.report_id AS is_report_id,
    i.ico,
    r.id_uctovnej_zavierky AS statement_id,
    core.parse_ruztxt_date(i.obdobie_do) AS period_end,
    CASE
      WHEN i.table_name = 'Strana aktív' AND i.period_col = 3 THEN 1
      WHEN i.table_name = 'Strana aktív' AND i.period_col = 4 THEN 2
      WHEN i.table_name <> 'Strana aktív' AND i.period_col = 1 THEN 1
      WHEN i.table_name <> 'Strana aktív' AND i.period_col = 2 THEN 2
    END AS norm_period
  FROM core.ruz_report_items i
  JOIN core.ruz_reports r ON r.id = i.report_id
  WHERE i.template_id = 22
    AND core.parse_ruztxt_date(i.obdobie_do) IS NOT NULL
),
pairs AS (
  SELECT
    bs.bs_report_id,
    isr.is_report_id,
    bs.ico,
    bs.statement_id,
    bs.period_end,
    bs.norm_period
  FROM bs
  JOIN isr
    ON isr.ico = bs.ico
   AND isr.statement_id = bs.statement_id
   AND isr.period_end = bs.period_end
   AND isr.norm_period = bs.norm_period
  WHERE bs.norm_period IS NOT NULL
)
INSERT INTO core.fin_etl_pairs (bs_report_id, is_report_id, ico, statement_id, period_end, norm_period)
SELECT bs_report_id, is_report_id, ico, statement_id, period_end, norm_period
FROM pairs
ON CONFLICT (bs_report_id, norm_period) DO UPDATE SET
  is_report_id = EXCLUDED.is_report_id,
  ico = EXCLUDED.ico,
  statement_id = EXCLUDED.statement_id,
  period_end = EXCLUDED.period_end,
  norm_period = EXCLUDED.norm_period;
"""

# -----------------------------
# Batch fetch helpers
# -----------------------------
SQL_FETCH_BATCH_SINGLE = """
SELECT report_id
FROM core.fin_etl_queue_single
ORDER BY report_id
LIMIT %s;
"""

SQL_DELETE_BATCH_SINGLE = """
DELETE FROM core.fin_etl_queue_single
WHERE report_id = ANY(%s);
"""

SQL_FETCH_BATCH_PAIRS = """
SELECT bs_report_id, is_report_id
FROM core.fin_etl_pairs
ORDER BY bs_report_id
LIMIT %s;
"""

SQL_DELETE_BATCH_PAIRS = """
DELETE FROM core.fin_etl_pairs
WHERE bs_report_id = ANY(%s);
"""

# -----------------------------
# Aggregates: batch for 699/687 (single report_id)
# -----------------------------
SQL_REFRESH_AGGREGATES_SINGLE_BATCH = """
WITH base AS (
  SELECT
      i.report_id,
      CASE
          WHEN i.template_id = 699 THEN
            CASE
              WHEN i.table_name = 'Strana aktív' AND i.period_col = 3 THEN 1
              WHEN i.table_name = 'Strana aktív' AND i.period_col = 4 THEN 2
              WHEN i.table_name <> 'Strana aktív' AND i.period_col = 1 THEN 1
              WHEN i.table_name <> 'Strana aktív' AND i.period_col = 2 THEN 2
            END
          WHEN i.template_id = 687 THEN
            CASE
              WHEN i.period_col = 1 THEN 1
              WHEN i.period_col = 2 THEN 2
            END
      END AS norm_period,

      i.ico,
      r.id_uctovnej_zavierky AS statement_id,
      i.template_id,

      core.parse_ruztxt_date(i.obdobie_do) AS period_end,
      EXTRACT(YEAR FROM core.parse_ruztxt_date(i.obdobie_do))::INT AS fiscal_year,

      r.mena AS currency,
      i.pravna_forma AS legal_form,

      i.table_name,
      m.metric_key,
      (COALESCE(i.value_num, 0) * m.sign_mult * m.weight) AS v
  FROM core.ruz_report_items i
  JOIN core.ruz_reports r
    ON r.id = i.report_id
  JOIN core.fin_item_map m
    ON m.template_id = i.template_id
   AND m.table_name  = i.table_name
   AND m.row_number  = i.row_number
  WHERE i.report_id = ANY(%s)
    AND i.template_id IN (699,687)
    AND core.parse_ruztxt_date(i.obdobie_do) IS NOT NULL
    AND (
      (i.template_id = 699 AND (
           (i.table_name = 'Strana aktív' AND i.period_col IN (3,4))
        OR (i.table_name <> 'Strana aktív' AND i.period_col IN (1,2))
      ))
      OR
      (i.template_id = 687 AND i.period_col IN (1,2))
    )
),
pivot AS (
  SELECT
      report_id,
      norm_period,
      MAX(ico) AS ico,
      MAX(statement_id) AS statement_id,
      MAX(template_id) AS template_id,
      MAX(period_end) AS period_end,
      MAX(fiscal_year) AS fiscal_year,
      MAX(currency) AS currency,
      MAX(legal_form) AS legal_form,

      SUM(CASE WHEN metric_key='TotalAssets' AND table_name='Strana aktív' THEN v END) AS total_assets,

      SUM(CASE WHEN metric_key='Equity' THEN v END) AS equity,
      SUM(CASE WHEN metric_key='TotalLiabilities' THEN v END) AS total_liabilities,

      SUM(CASE WHEN metric_key='CurrentAssets' THEN v END) AS current_assets,
      SUM(CASE WHEN metric_key='Cash' THEN v END) AS cash,
      SUM(CASE WHEN metric_key='Receivables' THEN v END) AS receivables,
      SUM(CASE WHEN metric_key='Inventory' THEN v END) AS inventory,

      SUM(CASE WHEN metric_key='CurrentLiabilities' THEN v END) AS current_liabilities,
      SUM(CASE WHEN metric_key='LongTermLiabilities' THEN v END) AS longterm_liabilities,

      SUM(CASE WHEN metric_key='Revenue' THEN v END) AS revenue,
      SUM(CASE WHEN metric_key='InterestExpense' THEN v END) AS interest_expense,
      SUM(CASE WHEN metric_key='Depreciation' THEN v END) AS depreciation,
      SUM(CASE WHEN metric_key='ProfitBeforeTax' THEN v END) AS profit_before_tax,
      SUM(CASE WHEN metric_key='NetIncome' THEN v END) AS net_income
  FROM base
  WHERE norm_period IS NOT NULL
    AND period_end IS NOT NULL
  GROUP BY report_id, norm_period
)
INSERT INTO core.fin_annual_aggregates (
  report_id, norm_period,
  ico, statement_id, template_id,
  period_end, fiscal_year, currency, legal_form,
  total_assets, equity, total_liabilities,
  current_assets, cash, receivables, inventory,
  current_liabilities, longterm_liabilities,
  revenue, interest_expense, depreciation, profit_before_tax, net_income
)
SELECT
  report_id, norm_period,
  ico, statement_id, template_id,
  period_end, fiscal_year, currency, legal_form,
  total_assets, equity, total_liabilities,
  current_assets, cash, receivables, inventory,
  current_liabilities, longterm_liabilities,
  revenue, interest_expense, depreciation, profit_before_tax, net_income
FROM pivot
ON CONFLICT (ico, fiscal_year, norm_period, report_id) DO UPDATE SET
  ico = EXCLUDED.ico,
  statement_id = EXCLUDED.statement_id,
  template_id = EXCLUDED.template_id,
  period_end = EXCLUDED.period_end,
  fiscal_year = EXCLUDED.fiscal_year,
  currency = EXCLUDED.currency,
  legal_form = EXCLUDED.legal_form,
  total_assets = EXCLUDED.total_assets,
  equity = EXCLUDED.equity,
  total_liabilities = EXCLUDED.total_liabilities,
  current_assets = EXCLUDED.current_assets,
  cash = EXCLUDED.cash,
  receivables = EXCLUDED.receivables,
  inventory = EXCLUDED.inventory,
  current_liabilities = EXCLUDED.current_liabilities,
  longterm_liabilities = EXCLUDED.longterm_liabilities,
  revenue = EXCLUDED.revenue,
  interest_expense = EXCLUDED.interest_expense,
  depreciation = EXCLUDED.depreciation,
  profit_before_tax = EXCLUDED.profit_before_tax,
  net_income = EXCLUDED.net_income,
  updated_at = now();
"""

# -----------------------------
# Aggregates: batch for paired 21/22
# We canonicalize all 22 rows to bs_report_id; strict pairs only.
# -----------------------------
SQL_REFRESH_AGGREGATES_PAIRS_BATCH = """
WITH pair_batch AS (
  SELECT
    p.bs_report_id,
    p.is_report_id,
    p.ico,
    p.statement_id,
    p.period_end,
    p.norm_period
  FROM core.fin_etl_pairs p
  WHERE p.bs_report_id = ANY(%s)
),
items AS (
  SELECT
    -- canonical report id = bs_report_id for both 21 and 22
    CASE
      WHEN i.template_id = 21 THEN i.report_id
      WHEN i.template_id = 22 THEN pb.bs_report_id
    END AS report_id,

    CASE
      WHEN i.table_name = 'Strana aktív' AND i.period_col = 3 THEN 1
      WHEN i.table_name = 'Strana aktív' AND i.period_col = 4 THEN 2
      WHEN i.table_name <> 'Strana aktív' AND i.period_col = 1 THEN 1
      WHEN i.table_name <> 'Strana aktív' AND i.period_col = 2 THEN 2
    END AS norm_period,

    i.ico,
    r.id_uctovnej_zavierky AS statement_id,
    -- store BS template_id for continuity
    21::bigint AS template_id,

    core.parse_ruztxt_date(i.obdobie_do) AS period_end,
    EXTRACT(YEAR FROM core.parse_ruztxt_date(i.obdobie_do))::INT AS fiscal_year,

    r.mena AS currency,
    i.pravna_forma AS legal_form,

    i.table_name,
    m.metric_key,
    (COALESCE(i.value_num, 0) * m.sign_mult * m.weight) AS v

  FROM pair_batch pb
  JOIN core.ruz_report_items i
    ON (i.report_id = pb.bs_report_id AND i.template_id = 21)
    OR (i.report_id = pb.is_report_id AND i.template_id = 22)
  JOIN core.ruz_reports r
    ON r.id = i.report_id
  JOIN core.fin_item_map m
    ON m.template_id = i.template_id
   AND m.table_name  = i.table_name
   AND m.row_number  = i.row_number
  WHERE core.parse_ruztxt_date(i.obdobie_do) IS NOT NULL
    AND (
         (i.table_name = 'Strana aktív' AND i.period_col IN (3,4))
      OR (i.table_name <> 'Strana aktív' AND i.period_col IN (1,2))
    )
),
pivot AS (
  SELECT
    report_id,
    norm_period,
    MAX(ico) AS ico,
    MAX(statement_id) AS statement_id,
    MAX(template_id) AS template_id,
    MAX(period_end) AS period_end,
    MAX(fiscal_year) AS fiscal_year,
    MAX(currency) AS currency,
    MAX(legal_form) AS legal_form,

    SUM(CASE WHEN metric_key='TotalAssets' AND table_name='Strana aktív' THEN v END) AS total_assets,

    SUM(CASE WHEN metric_key='Equity' THEN v END) AS equity,
    SUM(CASE WHEN metric_key='TotalLiabilities' THEN v END) AS total_liabilities,

    SUM(CASE WHEN metric_key='CurrentAssets' THEN v END) AS current_assets,
    SUM(CASE WHEN metric_key='Cash' THEN v END) AS cash,
    SUM(CASE WHEN metric_key='Receivables' THEN v END) AS receivables,
    SUM(CASE WHEN metric_key='Inventory' THEN v END) AS inventory,

    SUM(CASE WHEN metric_key='CurrentLiabilities' THEN v END) AS current_liabilities,
    SUM(CASE WHEN metric_key='LongTermLiabilities' THEN v END) AS longterm_liabilities,

    SUM(CASE WHEN metric_key='Revenue' THEN v END) AS revenue,
    SUM(CASE WHEN metric_key='InterestExpense' THEN v END) AS interest_expense,
    SUM(CASE WHEN metric_key='Depreciation' THEN v END) AS depreciation,
    SUM(CASE WHEN metric_key='ProfitBeforeTax' THEN v END) AS profit_before_tax,
    SUM(CASE WHEN metric_key='NetIncome' THEN v END) AS net_income
  FROM items
  WHERE norm_period IS NOT NULL
    AND period_end IS NOT NULL
  GROUP BY report_id, norm_period
)
INSERT INTO core.fin_annual_aggregates (
  report_id, norm_period,
  ico, statement_id, template_id,
  period_end, fiscal_year, currency, legal_form,
  total_assets, equity, total_liabilities,
  current_assets, cash, receivables, inventory,
  current_liabilities, longterm_liabilities,
  revenue, interest_expense, depreciation, profit_before_tax, net_income
)
SELECT
  report_id, norm_period,
  ico, statement_id, template_id,
  period_end, fiscal_year, currency, legal_form,
  total_assets, equity, total_liabilities,
  current_assets, cash, receivables, inventory,
  current_liabilities, longterm_liabilities,
  revenue, interest_expense, depreciation, profit_before_tax, net_income
FROM pivot
ON CONFLICT (ico, fiscal_year, norm_period, report_id) DO UPDATE SET
  ico = EXCLUDED.ico,
  statement_id = EXCLUDED.statement_id,
  template_id = EXCLUDED.template_id,
  period_end = EXCLUDED.period_end,
  fiscal_year = EXCLUDED.fiscal_year,
  currency = EXCLUDED.currency,
  legal_form = EXCLUDED.legal_form,
  total_assets = EXCLUDED.total_assets,
  equity = EXCLUDED.equity,
  total_liabilities = EXCLUDED.total_liabilities,
  current_assets = EXCLUDED.current_assets,
  cash = EXCLUDED.cash,
  receivables = EXCLUDED.receivables,
  inventory = EXCLUDED.inventory,
  current_liabilities = EXCLUDED.current_liabilities,
  longterm_liabilities = EXCLUDED.longterm_liabilities,
  revenue = EXCLUDED.revenue,
  interest_expense = EXCLUDED.interest_expense,
  depreciation = EXCLUDED.depreciation,
  profit_before_tax = EXCLUDED.profit_before_tax,
  net_income = EXCLUDED.net_income,
  updated_at = now();
"""

# -----------------------------
# Features: batch insert/update for report_ids
# (based on your original SQL, but filtered to report_ids)
# -----------------------------
SQL_REFRESH_FEATURES_BATCH = """
WITH a1 AS (
  SELECT
    a.*,
    LAG(a.cash) OVER (
      PARTITION BY a.ico
      ORDER BY a.fiscal_year, a.period_end, a.report_id
    ) AS cash_begin
  FROM core.fin_annual_aggregates a
  WHERE a.norm_period = 1
    AND a.report_id = ANY(%s)
),
calc AS (
  SELECT
    a1.*,

    (COALESCE(a1.profit_before_tax,0) + COALESCE(a1.interest_expense,0)) AS ebit_proxy_calc,

    (COALESCE(a1.profit_before_tax,0)
     + COALESCE(a1.interest_expense,0)
     + COALESCE(a1.depreciation,0)
    ) AS ebitda_calc,

    (
      (COALESCE(a1.profit_before_tax,0)
       + COALESCE(a1.interest_expense,0)
       + COALESCE(a1.depreciation,0)
      )
      + COALESCE(a1.cash,0)
      - COALESCE(a1.cash_begin,0)
    ) AS cf_kralicek_calc

  FROM a1
)
INSERT INTO core.fin_annual_features (
    report_id, norm_period,
    ico, fiscal_year, statement_id, template_id, period_end, currency, legal_form,

    current_ratio, quick_ratio, cash_ratio,
    net_working_capital, nwc_to_assets, cash_to_assets,

    equity_ratio, debt_ratio, debt_to_equity,
    roa, roe, net_margin,
    asset_turnover,
    ebit_proxy, interest_coverage,

    cash_begin, ebitda, cf_kralicek,
    period_debt_payment_years, ebit_to_assets, cf_to_revenue,

    x04_noncurrent_indebtedness, x07_interest_burden, x08_debt_to_cf, x09_equity_leverage, x10_insolvency,
    model_sk_raw,

    negative_equity_flag, liquidity_breach_flag, high_leverage_flag, loss_flag,

    total_assets, equity, total_liabilities,
    current_assets, current_liabilities,
    cash, inventory, receivables,
    revenue, interest_expense, depreciation, profit_before_tax, net_income
)
SELECT
    c.report_id,
    c.norm_period,

    c.ico,
    c.fiscal_year,
    c.statement_id,
    c.template_id,
    c.period_end,
    c.currency,
    c.legal_form,

    CASE
      WHEN c.current_liabilities IS NULL OR c.current_liabilities <= 0 THEN NULL
      WHEN c.current_assets IS NULL THEN NULL
      ELSE c.current_assets / c.current_liabilities
    END AS current_ratio,

    CASE
      WHEN c.current_liabilities IS NULL OR c.current_liabilities <= 0 THEN NULL
      ELSE (COALESCE(c.current_assets,0) - COALESCE(c.inventory,0)) / c.current_liabilities
    END AS quick_ratio,

    CASE
      WHEN c.current_liabilities IS NULL OR c.current_liabilities <= 0 THEN NULL
      ELSE COALESCE(c.cash,0) / c.current_liabilities
    END AS cash_ratio,

    (COALESCE(c.current_assets,0) - COALESCE(c.current_liabilities,0)) AS net_working_capital,

    CASE
      WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN NULL
      ELSE (COALESCE(c.current_assets,0) - COALESCE(c.current_liabilities,0)) / c.total_assets
    END AS nwc_to_assets,

    CASE
      WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN NULL
      ELSE COALESCE(c.cash,0) / c.total_assets
    END AS cash_to_assets,

    CASE
      WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN NULL
      ELSE c.equity / c.total_assets
    END AS equity_ratio,

    CASE
      WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN NULL
      ELSE c.total_liabilities / c.total_assets
    END AS debt_ratio,

    CASE
      WHEN c.equity IS NULL OR c.equity = 0 THEN NULL
      ELSE c.total_liabilities / c.equity
    END AS debt_to_equity,

    CASE
      WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN NULL
      ELSE c.net_income / c.total_assets
    END AS roa,

    CASE
      WHEN c.equity IS NULL OR c.equity = 0 THEN NULL
      ELSE c.net_income / c.equity
    END AS roe,

    CASE
      WHEN c.revenue IS NULL OR c.revenue = 0 THEN NULL
      ELSE c.net_income / c.revenue
    END AS net_margin,

    CASE
      WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN NULL
      ELSE c.revenue / c.total_assets
    END AS asset_turnover,

    c.ebit_proxy_calc AS ebit_proxy,

    CASE
      WHEN c.interest_expense IS NULL OR c.interest_expense = 0 THEN NULL
      ELSE c.ebit_proxy_calc / c.interest_expense
    END AS interest_coverage,

    c.cash_begin,
    c.ebitda_calc AS ebitda,
    c.cf_kralicek_calc AS cf_kralicek,

    CASE
      WHEN c.cf_kralicek_calc IS NULL OR c.cf_kralicek_calc = 0 THEN NULL
      ELSE (COALESCE(c.total_liabilities,0) - COALESCE(c.cash,0)) / c.cf_kralicek_calc
    END AS period_debt_payment_years,

    CASE
      WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN NULL
      ELSE c.ebit_proxy_calc / c.total_assets
    END AS ebit_to_assets,

    CASE
      WHEN c.revenue IS NULL OR c.revenue = 0 THEN NULL
      ELSE c.cf_kralicek_calc / c.revenue
    END AS cf_to_revenue,

    CASE
      WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN NULL
      ELSE COALESCE(c.longterm_liabilities,0) / c.total_assets
    END AS x04_noncurrent_indebtedness,

    CASE
      WHEN c.ebit_proxy_calc = 0 THEN NULL
      ELSE COALESCE(c.interest_expense,0) / c.ebit_proxy_calc
    END AS x07_interest_burden,

    CASE
      WHEN (COALESCE(c.net_income,0) + COALESCE(c.depreciation,0) + COALESCE(c.interest_expense,0)) = 0 THEN NULL
      ELSE COALESCE(c.total_liabilities,0) / (COALESCE(c.net_income,0) + COALESCE(c.depreciation,0) + COALESCE(c.interest_expense,0))
    END AS x08_debt_to_cf,

    CASE
      WHEN c.equity IS NULL OR c.equity = 0 THEN NULL
      ELSE COALESCE(c.total_assets,0) / c.equity
    END AS x09_equity_leverage,

    COALESCE(c.total_liabilities / NULLIF(c.receivables,0), 0) AS x10_insolvency,

    (
      -0.520
      + 4.439 * (CASE WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN NULL ELSE COALESCE(c.total_liabilities,0) / c.total_assets END)
      - 8.107 * (CASE WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN NULL ELSE COALESCE(c.equity,0) / c.total_assets END)
      - 0.494 * (CASE WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN NULL ELSE COALESCE(c.longterm_liabilities,0) / c.total_assets END)
      - 0.594 * (CASE WHEN c.ebit_proxy_calc = 0 THEN NULL ELSE COALESCE(c.interest_expense,0) / c.ebit_proxy_calc END)
      - 0.022 * (CASE
                   WHEN (COALESCE(c.net_income,0) + COALESCE(c.depreciation,0) + COALESCE(c.interest_expense,0)) = 0 THEN NULL
                   ELSE COALESCE(c.total_liabilities,0) / (COALESCE(c.net_income,0) + COALESCE(c.depreciation,0) + COALESCE(c.interest_expense,0))
                 END)
      - 0.116 * (CASE WHEN c.equity IS NULL OR c.equity = 0 THEN NULL ELSE COALESCE(c.total_assets,0) / c.equity END)
      + 1.787 * COALESCE(c.total_liabilities / NULLIF(c.receivables,0), 0)
    ) AS model_sk_raw,

    (c.equity IS NOT NULL AND c.equity < 0) AS negative_equity_flag,
    (COALESCE(c.current_liabilities,0) > 0 AND COALESCE(c.current_assets,0) <= 0) AS liquidity_breach_flag,
    (CASE
        WHEN c.total_assets IS NULL OR c.total_assets = 0 THEN FALSE
        ELSE (c.total_liabilities / c.total_assets) > 0.9
     END) AS high_leverage_flag,
    (c.net_income IS NOT NULL AND c.net_income < 0) AS loss_flag,

    c.total_assets, c.equity, c.total_liabilities,
    c.current_assets, c.current_liabilities,
    c.cash, c.inventory, c.receivables,
    c.revenue, c.interest_expense, c.depreciation, c.profit_before_tax, c.net_income

FROM calc c
ON CONFLICT (report_id, norm_period) DO UPDATE SET
  ico = EXCLUDED.ico,
  fiscal_year = EXCLUDED.fiscal_year,
  statement_id = EXCLUDED.statement_id,
  template_id = EXCLUDED.template_id,
  period_end = EXCLUDED.period_end,
  currency = EXCLUDED.currency,
  legal_form = EXCLUDED.legal_form,
  current_ratio = EXCLUDED.current_ratio,
  quick_ratio = EXCLUDED.quick_ratio,
  cash_ratio = EXCLUDED.cash_ratio,
  net_working_capital = EXCLUDED.net_working_capital,
  nwc_to_assets = EXCLUDED.nwc_to_assets,
  cash_to_assets = EXCLUDED.cash_to_assets,
  equity_ratio = EXCLUDED.equity_ratio,
  debt_ratio = EXCLUDED.debt_ratio,
  debt_to_equity = EXCLUDED.debt_to_equity,
  roa = EXCLUDED.roa,
  roe = EXCLUDED.roe,
  net_margin = EXCLUDED.net_margin,
  asset_turnover = EXCLUDED.asset_turnover,
  ebit_proxy = EXCLUDED.ebit_proxy,
  interest_coverage = EXCLUDED.interest_coverage,
  cash_begin = EXCLUDED.cash_begin,
  ebitda = EXCLUDED.ebitda,
  cf_kralicek = EXCLUDED.cf_kralicek,
  period_debt_payment_years = EXCLUDED.period_debt_payment_years,
  ebit_to_assets = EXCLUDED.ebit_to_assets,
  cf_to_revenue = EXCLUDED.cf_to_revenue,
  x04_noncurrent_indebtedness = EXCLUDED.x04_noncurrent_indebtedness,
  x07_interest_burden = EXCLUDED.x07_interest_burden,
  x08_debt_to_cf = EXCLUDED.x08_debt_to_cf,
  x09_equity_leverage = EXCLUDED.x09_equity_leverage,
  x10_insolvency = EXCLUDED.x10_insolvency,
  model_sk_raw = EXCLUDED.model_sk_raw,
  negative_equity_flag = EXCLUDED.negative_equity_flag,
  liquidity_breach_flag = EXCLUDED.liquidity_breach_flag,
  high_leverage_flag = EXCLUDED.high_leverage_flag,
  loss_flag = EXCLUDED.loss_flag,
  total_assets = EXCLUDED.total_assets,
  equity = EXCLUDED.equity,
  total_liabilities = EXCLUDED.total_liabilities,
  current_assets = EXCLUDED.current_assets,
  current_liabilities = EXCLUDED.current_liabilities,
  cash = EXCLUDED.cash,
  inventory = EXCLUDED.inventory,
  receivables = EXCLUDED.receivables,
  revenue = EXCLUDED.revenue,
  interest_expense = EXCLUDED.interest_expense,
  depreciation = EXCLUDED.depreciation,
  profit_before_tax = EXCLUDED.profit_before_tax,
  net_income = EXCLUDED.net_income,
  updated_at = now();
"""

# -----------------------------
# model_sk_pct, grades, duplicates, MV refresh: run once at end
# (keeping your original SQL, unchanged)
# -----------------------------
SQL_REFRESH_MODEL_SK_PCT = """
WITH ranked AS (
  SELECT
    report_id,
    norm_period,
    PERCENT_RANK() OVER (
      PARTITION BY fiscal_year
      ORDER BY model_sk_raw
    ) AS pr
  FROM core.fin_annual_features
  WHERE norm_period = 1
    AND model_sk_raw IS NOT NULL
)
UPDATE core.fin_annual_features f
SET model_sk_pct = r.pr
FROM ranked r
WHERE f.report_id = r.report_id
  AND f.norm_period = r.norm_period;
"""

SQL_REFRESH_GRADES = """  -- your original (unchanged)
INSERT INTO core.fin_health_grade (
    report_id, norm_period,
    ico, fiscal_year, statement_id, period_end,

    score_total, score_capital, score_profit, score_liq_bonus, score_nwc_pen,
    score_model_adj,

    grade, reason
)
WITH src AS (
  SELECT
    f.report_id,
    f.norm_period,
    f.ico,
    f.fiscal_year,
    f.statement_id,
    f.period_end,

    f.equity_ratio,
    f.period_debt_payment_years,
    f.ebit_to_assets,
    f.cf_to_revenue,

    f.total_assets,
    f.revenue,
    f.cf_kralicek
  FROM core.fin_annual_features f
  WHERE f.norm_period = 1
),
valid AS (
  SELECT
    s.*,
    (s.total_assets IS NOT NULL AND s.total_assets >= 1000)
    AND (s.revenue IS NOT NULL AND s.revenue >= 1000)
    AND (s.cf_kralicek IS NOT NULL AND ABS(s.cf_kralicek) >= 100) AS is_valid
  FROM src s
),
scored AS (
  SELECT
    v.*,

    CASE
      WHEN NOT v.is_valid OR v.equity_ratio IS NULL THEN NULL
      WHEN v.equity_ratio >= 0.30 THEN 4
      WHEN v.equity_ratio >= 0.20 THEN 3
      WHEN v.equity_ratio >= 0.10 THEN 2
      WHEN v.equity_ratio >= 0.00 THEN 1
      ELSE 0
    END AS p1,

    CASE
      WHEN NOT v.is_valid OR v.period_debt_payment_years IS NULL THEN NULL
      WHEN v.period_debt_payment_years < 3 THEN 4
      WHEN v.period_debt_payment_years < 5 THEN 3
      WHEN v.period_debt_payment_years < 12 THEN 2
      WHEN v.period_debt_payment_years < 30 THEN 1
      ELSE 0
    END AS p2,

    CASE
      WHEN NOT v.is_valid OR v.ebit_to_assets IS NULL THEN NULL
      WHEN v.ebit_to_assets >= 0.15 THEN 4
      WHEN v.ebit_to_assets >= 0.12 THEN 3
      WHEN v.ebit_to_assets >= 0.08 THEN 2
      WHEN v.ebit_to_assets >= 0.00 THEN 1
      ELSE 0
    END AS p3,

    CASE
      WHEN NOT v.is_valid OR v.cf_to_revenue IS NULL THEN NULL
      WHEN v.cf_to_revenue >= 0.10 THEN 4
      WHEN v.cf_to_revenue >= 0.08 THEN 3
      WHEN v.cf_to_revenue >= 0.05 THEN 2
      WHEN v.cf_to_revenue >= 0.00 THEN 1
      ELSE 0
    END AS p4

  FROM valid v
),
agg AS (
  SELECT
    s.*,
    CASE WHEN s.p1 IS NULL OR s.p2 IS NULL THEN NULL ELSE (s.p1 + s.p2)/2.0 END AS kqt_a,
    CASE WHEN s.p3 IS NULL OR s.p4 IS NULL THEN NULL ELSE (s.p3 + s.p4)/2.0 END AS kqt_b,
    CASE
      WHEN s.p1 IS NULL OR s.p2 IS NULL OR s.p3 IS NULL OR s.p4 IS NULL THEN NULL
      ELSE (((s.p1 + s.p2)/2.0) + ((s.p3 + s.p4)/2.0))/2.0
    END AS kqt_m,
    CASE
      WHEN s.p1 IS NULL OR s.p2 IS NULL OR s.p3 IS NULL OR s.p4 IS NULL THEN NULL
      ELSE (s.p1 + s.p2 + s.p3 + s.p4)
    END AS kqt_sum
  FROM scored s
),
final AS (
  SELECT
    a.report_id,
    a.norm_period,
    a.ico,
    a.fiscal_year,
    a.statement_id,
    a.period_end,

    a.kqt_sum::numeric AS score_total,

    NULL::numeric AS score_capital,
    NULL::numeric AS score_profit,
    NULL::numeric AS score_liq_bonus,
    NULL::numeric AS score_nwc_pen,
    NULL::numeric AS score_model_adj,

    CASE
      WHEN NOT a.is_valid OR a.kqt_sum IS NULL THEN 'N'
      WHEN a.kqt_sum >= 12 THEN 'A'
      WHEN a.kqt_sum >= 9  THEN 'B'
      WHEN a.kqt_sum >= 5  THEN 'C'
      ELSE 'D'
    END::character(1) AS grade,

    CONCAT(
      'KQT(Table3): valid=', CASE WHEN a.is_valid THEN 'true' ELSE 'false' END,
      '; P1(E/A)=', COALESCE(a.p1::text,'NULL'),
      ', P2(DebtYears)=', COALESCE(a.p2::text,'NULL'),
      ', P3(EBIT/A)=', COALESCE(a.p3::text,'NULL'),
      ', P4(CF/Rev)=', COALESCE(a.p4::text,'NULL'),
      '; A=', COALESCE(ROUND(a.kqt_a::numeric,3)::text,'NULL'),
      ', B=', COALESCE(ROUND(a.kqt_b::numeric,3)::text,'NULL'),
      ', M=', COALESCE(ROUND(a.kqt_m::numeric,3)::text,'NULL'),
      ', SUM=', COALESCE(a.kqt_sum::text,'NULL'),
      '; class=', CASE
        WHEN NOT a.is_valid OR a.kqt_m IS NULL THEN 'NA'
        WHEN a.kqt_m > 3 THEN 'Stable'
        WHEN a.kqt_m >= 1 THEN 'Uncertainty'
        ELSE 'Insolvency'
      END
    ) AS reason

  FROM agg a
)
SELECT
  report_id, norm_period,
  ico, fiscal_year, statement_id, period_end,
  score_total, score_capital, score_profit, score_liq_bonus, score_nwc_pen,
  score_model_adj,
  grade, reason
FROM final
ON CONFLICT (report_id, norm_period) DO UPDATE SET
  ico = EXCLUDED.ico,
  fiscal_year = EXCLUDED.fiscal_year,
  statement_id = EXCLUDED.statement_id,
  period_end = EXCLUDED.period_end,
  score_total = EXCLUDED.score_total,
  score_capital = EXCLUDED.score_capital,
  score_profit = EXCLUDED.score_profit,
  score_liq_bonus = EXCLUDED.score_liq_bonus,
  score_nwc_pen = EXCLUDED.score_nwc_pen,
  score_model_adj = EXCLUDED.score_model_adj,
  grade = EXCLUDED.grade,
  reason = EXCLUDED.reason,
  updated_at = now();
"""

SQL_DELETE_DUPLICATE_GRADES = """
DELETE FROM core.fin_health_grade g
USING (
  SELECT report_id, norm_period
  FROM (
    SELECT
      g.report_id,
      g.norm_period,
      ROW_NUMBER() OVER (
        PARTITION BY g.ico, g.fiscal_year
        ORDER BY g.period_end DESC NULLS LAST, g.report_id DESC
      ) AS rn
    FROM core.fin_health_grade g
    WHERE g.norm_period = 1
  ) t
  WHERE t.rn > 1
) d
WHERE g.report_id = d.report_id
  AND g.norm_period = d.norm_period;
"""

SQL_REFRESH_MV_TOP10_BY_GRADE_YEAR = "REFRESH MATERIALIZED VIEW core.mv_top10_by_grade_year;"
SQL_REFRESH_MV_COMPANY_BENCHMARK_DIM = "REFRESH MATERIALIZED VIEW core.mv_company_benchmark_dim;"
SQL_REFRESH_MV_COMPANY_BENCHMARK_FACT = "REFRESH MATERIALIZED VIEW core.mv_company_benchmark_facts;"

def _fetch_batch_single(conn, limit: int) -> List[int]:
    rows = conn.execute(SQL_FETCH_BATCH_SINGLE, (limit,)).fetchall()
    return [r[0] for r in rows]


def _fetch_batch_pairs(conn, limit: int) -> List[Tuple[int, int]]:
    rows = conn.execute(SQL_FETCH_BATCH_PAIRS, (limit,)).fetchall()
    return [(r[0], r[1]) for r in rows]


def run(rebuild: bool = True) -> None:
    """
    rebuild=True:
      - TRUNCATE aggregates/features/grades
      - rebuild everything in batches (699/687 singles + 21/22 strict pairs)
      - then pct + grades + MV refresh

    This avoids massive temp spills on 100M+ ruz_report_items by batching.
    """
    with get_conn() as conn:
        if rebuild:
            log.info("Rebuild mode: TRUNCATE target tables.")
            conn.execute(SQL_TRUNCATE_ALL)
            conn.commit()

        log.info("Preparing helper tables (unlogged).")
        conn.execute(SQL_DROP_HELPERS)
        conn.execute(SQL_CREATE_HELPERS)
        conn.commit()

        log.info("Building queue for 699/687 (single reports).")
        conn.execute(SQL_POPULATE_QUEUE_SINGLE)
        conn.commit()

        log.info("Building strict pairs for 21/22.")
        conn.execute(SQL_POPULATE_PAIRS_21_22)
        conn.commit()

        # -----------------------------
        # Process 699/687 in batches
        # -----------------------------
        log.info("Processing 699/687 aggregates+features in batches.")
        while True:
            batch_ids = _fetch_batch_single(conn, BATCH_SIZE_SINGLE)
            if not batch_ids:
                break

            conn.execute(SQL_REFRESH_AGGREGATES_SINGLE_BATCH, (batch_ids,))
            conn.execute(SQL_REFRESH_FEATURES_BATCH, (batch_ids,))
            conn.execute(SQL_DELETE_BATCH_SINGLE, (batch_ids,))
            conn.commit()

            log.info("Processed single batch: %s report_ids", len(batch_ids))

        # -----------------------------
        # Process 21/22 pairs in batches
        # -----------------------------
        log.info("Processing 21/22 paired aggregates+features in batches.")
        while True:
            pairs = _fetch_batch_pairs(conn, BATCH_SIZE_PAIRS)
            if not pairs:
                break

            bs_ids = [p[0] for p in pairs]
            # aggregates: canonical uses bs_ids subset; reads both bs & is via fin_etl_pairs
            conn.execute(SQL_REFRESH_AGGREGATES_PAIRS_BATCH, (bs_ids,))
            # features computed from aggregates for bs report_ids (canonical ids)
            conn.execute(SQL_REFRESH_FEATURES_BATCH, (bs_ids,))
            conn.execute(SQL_DELETE_BATCH_PAIRS, (bs_ids,))
            conn.commit()

            log.info("Processed pairs batch: %s pairs", len(bs_ids))

        # -----------------------------
        # Global steps (once)
        # -----------------------------
        log.info("Refreshing model_sk_pct (global).")
        conn.execute(SQL_REFRESH_MODEL_SK_PCT)
        conn.commit()

        log.info("Refreshing grades (global).")
        conn.execute(SQL_REFRESH_GRADES)
        conn.execute(SQL_DELETE_DUPLICATE_GRADES)
        conn.commit()

        # Post-commit MV refresh (nice-to-have)
        try:
            conn.execute(SQL_REFRESH_MV_TOP10_BY_GRADE_YEAR)
            conn.execute(SQL_REFRESH_MV_COMPANY_BENCHMARK_DIM)
            conn.execute(SQL_REFRESH_MV_COMPANY_BENCHMARK_FACT)
            conn.commit()
        except Exception:
            conn.rollback()
            log.exception("MV refresh failed (continuing).")

    log.info("FIN ETL finished (batch rebuild with strict 21/22 pairing).")