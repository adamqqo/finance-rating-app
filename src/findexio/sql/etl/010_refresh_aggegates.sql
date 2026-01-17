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

        -- TotalAssets len z aktív
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
ON CONFLICT (report_id, norm_period) DO UPDATE SET
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
