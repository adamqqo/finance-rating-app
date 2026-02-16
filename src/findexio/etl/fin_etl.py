from __future__ import annotations

import logging

from ..db import get_conn

log = logging.getLogger("findexio.fin_etl")

SQL_REFRESH_AGGREGATES = """
WITH base AS (
    SELECT
        i.report_id,
        CASE
            WHEN i.table_name = 'Strana aktív' AND i.period_col = 3 THEN 1
            WHEN i.table_name = 'Strana aktív' AND i.period_col = 4 THEN 2
            WHEN i.table_name <> 'Strana aktív' AND i.period_col = 1 THEN 1
            WHEN i.table_name <> 'Strana aktív' AND i.period_col = 2 THEN 2
            ELSE NULL
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
    WHERE i.template_id = 699
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

        -- TotalAssets only from Assets side
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


SQL_REFRESH_FEATURES = """
INSERT INTO core.fin_annual_features (
    report_id, norm_period,
    ico, fiscal_year, statement_id, template_id, period_end, currency, legal_form,

    current_ratio, quick_ratio, cash_ratio,
    net_working_capital, nwc_to_assets, cash_to_assets,

    equity_ratio, debt_ratio, debt_to_equity,
    roa, roe, net_margin,
    asset_turnover,
    ebit_proxy, interest_coverage,

    -- New: model inputs / intermediates
    x04_noncurrent_indebtedness, x07_interest_burden, x08_debt_to_cf, x09_equity_leverage, x10_insolvency,
    model_sk_raw,

    negative_equity_flag, liquidity_breach_flag, high_leverage_flag, loss_flag,

    total_assets, equity, total_liabilities,
    current_assets, current_liabilities,
    cash, inventory, receivables,
    revenue, interest_expense, depreciation, profit_before_tax, net_income
)
SELECT
    a.report_id,
    a.norm_period,

    a.ico,
    a.fiscal_year,
    a.statement_id,
    a.template_id,
    a.period_end,
    a.currency,
    a.legal_form,

    CASE
      WHEN a.current_liabilities IS NULL OR a.current_liabilities <= 0 THEN NULL
      WHEN a.current_assets IS NULL THEN NULL
      ELSE a.current_assets / a.current_liabilities
    END AS current_ratio,

    CASE
      WHEN a.current_liabilities IS NULL OR a.current_liabilities <= 0 THEN NULL
      ELSE (COALESCE(a.current_assets,0) - COALESCE(a.inventory,0)) / a.current_liabilities
    END AS quick_ratio,

    CASE
      WHEN a.current_liabilities IS NULL OR a.current_liabilities <= 0 THEN NULL
      ELSE COALESCE(a.cash,0) / a.current_liabilities
    END AS cash_ratio,

    (COALESCE(a.current_assets,0) - COALESCE(a.current_liabilities,0)) AS net_working_capital,

    CASE
      WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL
      ELSE (COALESCE(a.current_assets,0) - COALESCE(a.current_liabilities,0)) / a.total_assets
    END AS nwc_to_assets,

    CASE
      WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL
      ELSE COALESCE(a.cash,0) / a.total_assets
    END AS cash_to_assets,

    CASE
      WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL
      ELSE a.equity / a.total_assets
    END AS equity_ratio,

    CASE
      WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL
      ELSE a.total_liabilities / a.total_assets
    END AS debt_ratio,

    CASE
      WHEN a.equity IS NULL OR a.equity = 0 THEN NULL
      ELSE a.total_liabilities / a.equity
    END AS debt_to_equity,

    CASE
      WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL
      ELSE a.net_income / a.total_assets
    END AS roa,

    CASE
      WHEN a.equity IS NULL OR a.equity = 0 THEN NULL
      ELSE a.net_income / a.equity
    END AS roe,

    CASE
      WHEN a.revenue IS NULL OR a.revenue = 0 THEN NULL
      ELSE a.net_income / a.revenue
    END AS net_margin,

    CASE
      WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL
      ELSE a.revenue / a.total_assets
    END AS asset_turnover,

    (COALESCE(a.profit_before_tax,0) + COALESCE(a.interest_expense,0)) AS ebit_proxy,

    CASE
      WHEN a.interest_expense IS NULL OR a.interest_expense = 0 THEN NULL
      ELSE (COALESCE(a.profit_before_tax,0) + COALESCE(a.interest_expense,0)) / a.interest_expense
    END AS interest_coverage,

    -- X4 = longterm_liabilities / total_assets
    CASE
      WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL
      ELSE COALESCE(a.longterm_liabilities,0) / a.total_assets
    END AS x04_noncurrent_indebtedness,

    -- X7 = interest_expense / EBIT, EBIT proxy = (profit_before_tax + interest_expense)
    CASE
      WHEN (COALESCE(a.profit_before_tax,0) + COALESCE(a.interest_expense,0)) = 0 THEN NULL
      ELSE COALESCE(a.interest_expense,0) / (COALESCE(a.profit_before_tax,0) + COALESCE(a.interest_expense,0))
    END AS x07_interest_burden,

    -- X8 = total_liabilities / cash_flow_proxy
    -- cash_flow_proxy = net_income + depreciation + interest_expense
    CASE
      WHEN (COALESCE(a.net_income,0) + COALESCE(a.depreciation,0) + COALESCE(a.interest_expense,0)) = 0 THEN NULL
      ELSE COALESCE(a.total_liabilities,0) / (COALESCE(a.net_income,0) + COALESCE(a.depreciation,0) + COALESCE(a.interest_expense,0))
    END AS x08_debt_to_cf,

    -- X9 = total_assets / equity
    CASE
      WHEN a.equity IS NULL OR a.equity = 0 THEN NULL
      ELSE COALESCE(a.total_assets,0) / a.equity
    END AS x09_equity_leverage,

    -- X10 = total_liabilities / receivables
    COALESCE(a.total_liabilities / NULLIF(a.receivables,0), 0) AS x10_insolvency,

    -- ===== Slovak discriminant model (Gajdosikova et al., 2025): y_SK =====
    (
      -0.520
      + 4.439 * (CASE WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL ELSE COALESCE(a.total_liabilities,0) / a.total_assets END) -- X1
      - 8.107 * (CASE WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL ELSE COALESCE(a.equity,0) / a.total_assets END)            -- X2
      - 0.494 * (CASE WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL ELSE COALESCE(a.longterm_liabilities,0) / a.total_assets END) -- X4
      - 0.594 * (CASE WHEN (COALESCE(a.profit_before_tax,0) + COALESCE(a.interest_expense,0)) = 0 THEN NULL ELSE COALESCE(a.interest_expense,0) / (COALESCE(a.profit_before_tax,0) + COALESCE(a.interest_expense,0)) END) -- X7
      - 0.022 * (CASE WHEN (COALESCE(a.net_income,0) + COALESCE(a.depreciation,0) + COALESCE(a.interest_expense,0)) = 0 THEN NULL ELSE COALESCE(a.total_liabilities,0) / (COALESCE(a.net_income,0) + COALESCE(a.depreciation,0) + COALESCE(a.interest_expense,0)) END) -- X8
      - 0.116 * (CASE WHEN a.equity IS NULL OR a.equity = 0 THEN NULL ELSE COALESCE(a.total_assets,0) / a.equity END) -- X9
      + 1.787 * COALESCE(a.total_liabilities / NULLIF(a.receivables,0), 0) -- X10
    ) AS model_sk_raw,

    (a.equity IS NOT NULL AND a.equity < 0) AS negative_equity_flag,
    (COALESCE(a.current_liabilities,0) > 0 AND COALESCE(a.current_assets,0) <= 0) AS liquidity_breach_flag,
    (CASE
        WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN FALSE
        ELSE (a.total_liabilities / a.total_assets) > 0.9
     END) AS high_leverage_flag,
    (a.net_income IS NOT NULL AND a.net_income < 0) AS loss_flag,

    a.total_assets, a.equity, a.total_liabilities,
    a.current_assets, a.current_liabilities,
    a.cash, a.inventory, a.receivables,
    a.revenue, a.interest_expense, a.depreciation, a.profit_before_tax, a.net_income

FROM core.fin_annual_aggregates a
WHERE a.norm_period = 1
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


SQL_REFRESH_GRADES = """
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
        f.roa,
        f.loss_flag,
        f.negative_equity_flag,
        f.cash_to_assets,
        f.nwc_to_assets,

        f.model_sk_raw,
        f.model_sk_pct
    FROM core.fin_annual_features f
    WHERE f.norm_period = 1
),
scored AS (
    SELECT
        s.*,

        CASE
          WHEN s.negative_equity_flag IS TRUE THEN TRUE
          WHEN s.equity_ratio IS NOT NULL AND s.equity_ratio < -1 THEN TRUE
          ELSE FALSE
        END AS hard_fail_f,

        CASE
          WHEN s.loss_flag IS TRUE
           AND s.equity_ratio IS NOT NULL
           AND s.equity_ratio < 0
           AND (s.negative_equity_flag IS NOT TRUE)
           AND NOT (s.equity_ratio < -1)
          THEN TRUE
          ELSE FALSE
        END AS hard_fail_e,

        CASE
          WHEN s.equity_ratio IS NULL THEN 0
          WHEN s.equity_ratio >= 0.50 THEN 50
          WHEN s.equity_ratio >= 0.30 THEN 40
          WHEN s.equity_ratio >= 0.10 THEN 30
          WHEN s.equity_ratio >= 0.00 THEN 20
          WHEN s.equity_ratio >= -1.00 THEN 10
          ELSE 0
        END AS score_capital,

        CASE
          WHEN s.roa IS NULL THEN 0
          WHEN s.roa >= 0.10 THEN 30
          WHEN s.roa >= 0.05 THEN 25
          WHEN s.roa >= 0.00 THEN 15
          ELSE 0
        END AS score_profit,

        CASE
          WHEN s.cash_to_assets IS NULL THEN 0
          WHEN s.cash_to_assets >= 0.10 THEN 20
          WHEN s.cash_to_assets >= 0.05 THEN 10
          ELSE 0
        END AS score_liq_bonus,

        CASE
          WHEN s.nwc_to_assets IS NULL THEN 0
          WHEN s.nwc_to_assets < -1.00 THEN -10
          WHEN s.nwc_to_assets < -0.50 THEN -5
          ELSE 0
        END AS score_nwc_pen,

        -- Model-based adjustment:
        -- model_sk_pct close to 1 => worse (more distressed) => penalize
        CASE
          WHEN s.model_sk_pct IS NULL THEN 0
          WHEN s.model_sk_pct >= 0.95 THEN -15
          WHEN s.model_sk_pct >= 0.85 THEN -10
          WHEN s.model_sk_pct >= 0.70 THEN -5
          WHEN s.model_sk_pct <= 0.10 THEN 2
          ELSE 0
        END AS score_model_adj
    FROM src s
),
final AS (
    SELECT
        report_id,
        norm_period,
        ico,
        fiscal_year,
        statement_id,
        period_end,

        score_capital,
        score_profit,
        score_liq_bonus,
        score_nwc_pen,
        score_model_adj,

        GREATEST(
            0,
            LEAST(
                100,
                (score_capital + score_profit + score_liq_bonus + score_nwc_pen + score_model_adj)
            )
        )::NUMERIC AS score_total,

        CASE
          WHEN hard_fail_f THEN 'F'
          WHEN hard_fail_e THEN 'E'
          WHEN (score_capital + score_profit + score_liq_bonus + score_nwc_pen + score_model_adj) >= 80 THEN 'A'
          WHEN (score_capital + score_profit + score_liq_bonus + score_nwc_pen + score_model_adj) >= 65 THEN 'B'
          WHEN (score_capital + score_profit + score_liq_bonus + score_nwc_pen + score_model_adj) >= 50 THEN 'C'
          WHEN (score_capital + score_profit + score_liq_bonus + score_nwc_pen + score_model_adj) >= 35 THEN 'D'
          WHEN (score_capital + score_profit + score_liq_bonus + score_nwc_pen + score_model_adj) >= 20 THEN 'E'
          ELSE 'F'
        END AS grade,

        CASE
          WHEN hard_fail_f AND negative_equity_flag IS TRUE THEN 'F: negative equity'
          WHEN hard_fail_f AND (equity_ratio < -1) THEN 'F: equity_ratio < -1'
          WHEN hard_fail_f THEN 'F: hard fail'
          WHEN hard_fail_e THEN 'E: loss + negative equity_ratio'
          ELSE
            'OK: score=' ||
            GREATEST(
                0,
                LEAST(
                    100,
                    (score_capital + score_profit + score_liq_bonus + score_nwc_pen + score_model_adj)
                )
            )::text ||
            '; capital=' || score_capital::text ||
            '; profit=' || score_profit::text ||
            '; cash_bonus=' || score_liq_bonus::text ||
            '; nwc_pen=' || score_nwc_pen::text ||
            '; model_adj=' || score_model_adj::text ||
            '; model_pct=' || COALESCE(model_sk_pct::text, 'NULL') ||
            '; model_raw=' || COALESCE(model_sk_raw::text, 'NULL')
        END AS reason
    FROM scored
)
SELECT
    report_id,
    norm_period,
    ico,
    fiscal_year,
    statement_id,
    period_end,

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


def run() -> None:
    with get_conn() as conn:
        # jedna transakcia: buď všetko, alebo nič
        conn.execute(SQL_REFRESH_AGGREGATES)
        conn.execute(SQL_REFRESH_FEATURES)

        # NEW: normalize Slovak model score to percentiles per fiscal year
        conn.execute(SQL_REFRESH_MODEL_SK_PCT)

        conn.execute(SQL_REFRESH_GRADES)
        conn.execute(SQL_DELETE_DUPLICATE_GRADES)
        conn.commit()

    log.info("FIN ETL finished (aggregates -> features -> model pct -> grades).")
