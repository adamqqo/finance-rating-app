INSERT INTO core.fin_annual_features (
    report_id, norm_period,
    ico, fiscal_year, statement_id, template_id, period_end, currency, legal_form,

    current_ratio, quick_ratio, cash_ratio,
    net_working_capital, nwc_to_assets, cash_to_assets,

    equity_ratio, debt_ratio, debt_to_equity,
    roa, roe, net_margin,
    asset_turnover,
    ebit_proxy, interest_coverage,

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

    -- Ratios: compute only when meaningful
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

    CASE WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL
         ELSE a.equity / a.total_assets END AS equity_ratio,

    CASE WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL
         ELSE a.total_liabilities / a.total_assets END AS debt_ratio,

    CASE WHEN a.equity IS NULL OR a.equity = 0 THEN NULL
         ELSE a.total_liabilities / a.equity END AS debt_to_equity,

    CASE WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL
         ELSE a.net_income / a.total_assets END AS roa,

    CASE WHEN a.equity IS NULL OR a.equity = 0 THEN NULL
         ELSE a.net_income / a.equity END AS roe,

    CASE WHEN a.revenue IS NULL OR a.revenue = 0 THEN NULL
         ELSE a.net_income / a.revenue END AS net_margin,

    CASE WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN NULL
         ELSE a.revenue / a.total_assets END AS asset_turnover,

    (COALESCE(a.profit_before_tax,0) + COALESCE(a.interest_expense,0)) AS ebit_proxy,

    CASE
      WHEN a.interest_expense IS NULL OR a.interest_expense = 0 THEN NULL
      ELSE (COALESCE(a.profit_before_tax,0) + COALESCE(a.interest_expense,0)) / a.interest_expense
    END AS interest_coverage,

    (a.equity IS NOT NULL AND a.equity < 0) AS negative_equity_flag,
    (COALESCE(a.current_liabilities,0) > 0 AND COALESCE(a.current_assets,0) <= 0) AS liquidity_breach_flag,

    (CASE WHEN a.total_assets IS NULL OR a.total_assets = 0 THEN FALSE
          ELSE (a.total_liabilities / a.total_assets) > 0.9 END) AS high_leverage_flag,

    (a.net_income IS NOT NULL AND a.net_income < 0) AS loss_flag,

    -- Raw refs
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
