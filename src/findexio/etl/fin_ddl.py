from __future__ import annotations

import logging

from ..db import get_conn

log = logging.getLogger("findexio.fin_ddl")

SQL_FIN_DDL = """
CREATE SCHEMA IF NOT EXISTS core;

-- =========================
-- Functions
-- =========================
--CREATE OR REPLACE FUNCTION core.parse_ruztxt_date(x TEXT)
--RETURNS DATE
--LANGUAGE sql
--IMMUTABLE
--AS $$
--  SELECT CASE
--    WHEN x IS NULL OR btrim(x) = '' THEN NULL
--    WHEN x ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN x::date
--    WHEN x ~ '^\\d{4}-\\d{2}$' THEN (x || '-01')::date + interval '1 month - 1 day'
--   WHEN x ~ '^\\d{4}$' THEN (x || '-12-31')::date
--    ELSE NULL
--  END;
--$$;

-- =========================
-- Financial tables
-- =========================

-- Table: core.fin_item_map

-- DROP TABLE IF EXISTS core.fin_item_map;

CREATE TABLE IF NOT EXISTS core.fin_item_map
(
    id bigint NOT NULL DEFAULT nextval('core.fin_item_map_id_seq'::regclass),
    template_id bigint,
    table_name text COLLATE pg_catalog."default",
    row_number integer,
    oznacenie text COLLATE pg_catalog."default",
    metric_key text COLLATE pg_catalog."default" NOT NULL,
    sign_mult smallint NOT NULL DEFAULT 1,
    weight numeric NOT NULL DEFAULT 1,
    note text COLLATE pg_catalog."default",
    CONSTRAINT fin_item_map_pkey PRIMARY KEY (id),
    CONSTRAINT fin_item_map_template_id_table_name_row_number_metric_key_key UNIQUE (template_id, table_name, row_number, metric_key)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS core.fin_item_map
    OWNER to postgres;
-- Index: ix_fin_item_map_metric

-- DROP INDEX IF EXISTS core.ix_fin_item_map_metric;

CREATE INDEX IF NOT EXISTS ix_fin_item_map_metric
    ON core.fin_item_map USING btree
    (metric_key COLLATE pg_catalog."default" ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: ix_fin_item_map_tpl

-- DROP INDEX IF EXISTS core.ix_fin_item_map_tpl;

CREATE INDEX IF NOT EXISTS ix_fin_item_map_tpl
    ON core.fin_item_map USING btree
    (template_id ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;

-- Table: core.fin_annual_aggregates

-- DROP TABLE IF EXISTS core.fin_annual_aggregates;

CREATE TABLE IF NOT EXISTS core.fin_annual_aggregates
(
    ico text COLLATE pg_catalog."default" NOT NULL,
    report_id bigint NOT NULL,
    statement_id bigint,
    template_id bigint,
    period_end date NOT NULL,
    fiscal_year integer NOT NULL,
    currency text COLLATE pg_catalog."default",
    legal_form text COLLATE pg_catalog."default",
    total_assets numeric,
    equity numeric,
    total_liabilities numeric,
    current_assets numeric,
    cash numeric,
    receivables numeric,
    inventory numeric,
    current_liabilities numeric,
    longterm_liabilities numeric,
    revenue numeric,
    ebit numeric,
    interest_expense numeric,
    net_income numeric,
    updated_at timestamp with time zone DEFAULT now(),
    norm_period smallint NOT NULL,
    depreciation numeric,
    profit_before_tax numeric,
    CONSTRAINT fin_annual_aggregates_pkey PRIMARY KEY (ico, fiscal_year, norm_period, report_id)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS core.fin_annual_aggregates
    OWNER to postgres;
-- Index: ix_faa_ico_year_norm

-- DROP INDEX IF EXISTS core.ix_faa_ico_year_norm;

CREATE INDEX IF NOT EXISTS ix_faa_ico_year_norm
    ON core.fin_annual_aggregates USING btree
    (ico COLLATE pg_catalog."default" ASC NULLS LAST, fiscal_year ASC NULLS LAST, norm_period ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: ix_faa_report_norm

-- DROP INDEX IF EXISTS core.ix_faa_report_norm;

CREATE INDEX IF NOT EXISTS ix_faa_report_norm
    ON core.fin_annual_aggregates USING btree
    (report_id ASC NULLS LAST, norm_period ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: ix_fin_aggr_report

-- DROP INDEX IF EXISTS core.ix_fin_aggr_report;

CREATE INDEX IF NOT EXISTS ix_fin_aggr_report
    ON core.fin_annual_aggregates USING btree
    (report_id ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
-- Index: ix_fin_aggr_year

-- DROP INDEX IF EXISTS core.ix_fin_aggr_year;

CREATE INDEX IF NOT EXISTS ix_fin_aggr_year
    ON core.fin_annual_aggregates USING btree
    (fiscal_year ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;

-- Table: core.fin_annual_features

-- DROP TABLE IF EXISTS core.fin_annual_features;

CREATE TABLE IF NOT EXISTS core.fin_annual_features
(
    report_id bigint NOT NULL,
    norm_period smallint NOT NULL,
    ico text COLLATE pg_catalog."default",
    fiscal_year integer,
    statement_id bigint,
    template_id bigint,
    period_end date,
    currency text COLLATE pg_catalog."default",
    legal_form text COLLATE pg_catalog."default",
    current_ratio numeric,
    quick_ratio numeric,
    cash_ratio numeric,
    equity_ratio numeric,
    debt_ratio numeric,
    debt_to_equity numeric,
    roa numeric,
    roe numeric,
    net_margin numeric,
    asset_turnover numeric,
    ebit_proxy numeric,
    interest_coverage numeric,
    negative_equity_flag boolean,
    liquidity_breach_flag boolean,
    high_leverage_flag boolean,
    loss_flag boolean,
    total_assets numeric,
    equity numeric,
    total_liabilities numeric,
    current_assets numeric,
    current_liabilities numeric,
    cash numeric,
    inventory numeric,
    receivables numeric,
    revenue numeric,
    interest_expense numeric,
    depreciation numeric,
    profit_before_tax numeric,
    net_income numeric,
    updated_at timestamp with time zone DEFAULT now(),
    net_working_capital numeric,
    nwc_to_assets numeric,
    cash_to_assets numeric,
    CONSTRAINT fin_annual_features_pkey PRIMARY KEY (report_id, norm_period)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS core.fin_annual_features
    OWNER to postgres;

-- Table: core.fin_health_grade

-- DROP TABLE IF EXISTS core.fin_health_grade;

CREATE TABLE IF NOT EXISTS core.fin_health_grade
(
    report_id bigint NOT NULL,
    norm_period smallint NOT NULL,
    ico text COLLATE pg_catalog."default",
    fiscal_year integer,
    statement_id bigint,
    period_end date,
    score_total numeric,
    score_capital numeric,
    score_profit numeric,
    score_liq_bonus numeric,
    score_nwc_pen numeric,
    grade character(1) COLLATE pg_catalog."default",
    reason text COLLATE pg_catalog."default",
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT fin_health_grade_pkey PRIMARY KEY (report_id, norm_period)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS core.fin_health_grade
    OWNER to postgres;
-- Index: ix_fhg_ico_year

-- DROP INDEX IF EXISTS core.ix_fhg_ico_year;

CREATE INDEX IF NOT EXISTS ix_fhg_ico_year
    ON core.fin_health_grade USING btree
    (ico COLLATE pg_catalog."default" ASC NULLS LAST, fiscal_year ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    TABLESPACE pg_default;
"""


def run() -> None:
    # psycopg3: connection is a context manager; on exception it rolls back
    with get_conn() as conn:
        conn.execute(SQL_FIN_DDL)
        conn.commit()
    log.info("FIN DDL applied.")
