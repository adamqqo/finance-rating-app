/* =========================
   MAP: template row -> metric key
   ========================= */
CREATE TABLE IF NOT EXISTS core.fin_item_map (
    template_id   BIGINT NOT NULL,
    table_name    TEXT NOT NULL,
    row_number    INTEGER NOT NULL,
    metric_key    TEXT NOT NULL,
    sign_mult     SMALLINT NOT NULL DEFAULT 1,
    weight        NUMERIC NOT NULL DEFAULT 1,
    updated_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (template_id, table_name, row_number, metric_key)
);

/* =========================
   AGGREGATES: per report_id + norm_period
   ========================= */
CREATE TABLE IF NOT EXISTS core.fin_annual_aggregates (
    report_id      BIGINT NOT NULL,
    norm_period    SMALLINT NOT NULL,      -- 1=current, 2=previous

    ico            TEXT,
    statement_id   BIGINT,
    template_id    BIGINT,

    period_end     DATE,
    fiscal_year    INT,
    currency       TEXT,
    legal_form     TEXT,

    -- BALANCE SHEET
    total_assets           NUMERIC,
    equity                 NUMERIC,
    total_liabilities      NUMERIC,
    current_assets         NUMERIC,
    cash                   NUMERIC,
    receivables            NUMERIC,
    inventory              NUMERIC,
    current_liabilities    NUMERIC,
    longterm_liabilities   NUMERIC,

    -- INCOME STATEMENT
    revenue               NUMERIC,
    interest_expense      NUMERIC,
    depreciation          NUMERIC,
    profit_before_tax     NUMERIC,
    net_income            NUMERIC,

    updated_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (report_id, norm_period)
);

/* =========================
   FEATURES: ratios + flags (norm_period=1 only used now)
   ========================= */
CREATE TABLE IF NOT EXISTS core.fin_annual_features (
    report_id        BIGINT NOT NULL,
    norm_period      SMALLINT NOT NULL,

    ico              TEXT,
    fiscal_year      INT,
    statement_id     BIGINT,
    template_id      BIGINT,
    period_end       DATE,
    currency         TEXT,
    legal_form       TEXT,

    -- Liquidity
    current_ratio           NUMERIC,
    quick_ratio             NUMERIC,
    cash_ratio              NUMERIC,
    net_working_capital     NUMERIC,
    nwc_to_assets           NUMERIC,
    cash_to_assets          NUMERIC,

    -- Solvency
    equity_ratio            NUMERIC,
    debt_ratio              NUMERIC,
    debt_to_equity          NUMERIC,

    -- Profitability
    roa                     NUMERIC,
    roe                     NUMERIC,
    net_margin              NUMERIC,

    -- Efficiency
    asset_turnover          NUMERIC,

    -- Coverage
    ebit_proxy              NUMERIC,
    interest_coverage       NUMERIC,

    -- Flags
    negative_equity_flag     BOOLEAN,
    liquidity_breach_flag    BOOLEAN,
    high_leverage_flag       BOOLEAN,
    loss_flag                BOOLEAN,

    -- Raw refs
    total_assets             NUMERIC,
    equity                   NUMERIC,
    total_liabilities         NUMERIC,
    current_assets           NUMERIC,
    current_liabilities       NUMERIC,
    cash                     NUMERIC,
    inventory                NUMERIC,
    receivables              NUMERIC,
    revenue                  NUMERIC,
    interest_expense         NUMERIC,
    depreciation             NUMERIC,
    profit_before_tax        NUMERIC,
    net_income               NUMERIC,

    updated_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (report_id, norm_period)
);

/* =========================
   GRADING: score + grade A–F
   ========================= */
CREATE TABLE IF NOT EXISTS core.fin_health_grade (
    report_id        BIGINT NOT NULL,
    norm_period      SMALLINT NOT NULL,

    ico              TEXT,
    fiscal_year      INT,
    statement_id     BIGINT,
    period_end       DATE,

    score_total      NUMERIC,
    score_capital    NUMERIC,
    score_profit     NUMERIC,
    score_liq_bonus  NUMERIC,
    score_nwc_pen    NUMERIC,

    grade            CHAR(1),
    reason           TEXT,

    updated_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (report_id, norm_period)
);
