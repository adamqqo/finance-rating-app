BEGIN;

-- =========================================================
-- 0) (Optional) Safety: create tables if not exist
-- =========================================================

CREATE TABLE IF NOT EXISTS core.company_default_events (
  ico               text NOT NULL,
  default_date      date NOT NULL,
  default_type      text NOT NULL,
  source_table      text NOT NULL,
  source_id         bigint NOT NULL,
  bulletin_issue_id int4,
  published_at      timestamptz,
  created_at        timestamptz DEFAULT now(),
  PRIMARY KEY (source_table, source_id)
);

CREATE INDEX IF NOT EXISTS ix_cde_ico_date
  ON core.company_default_events (ico, default_date);

CREATE MATERIALIZED VIEW IF NOT EXISTS core.company_first_default AS
SELECT
  ico,
  MIN(default_date) AS first_default_date
FROM core.company_default_events
GROUP BY ico;

CREATE INDEX IF NOT EXISTS ix_cfd_ico
  ON core.company_first_default (ico);

CREATE TABLE IF NOT EXISTS core.ml_bankruptcy_labels (
  report_id    bigint PRIMARY KEY,
  ico          text NOT NULL,
  fiscal_year  int NOT NULL,
  period_end   date NOT NULL,
  first_default_date date,
  default_within_12m boolean NOT NULL,
  default_within_24m boolean NOT NULL,
  created_at timestamptz DEFAULT now()
);

-- =========================================================
-- 1) Wipe old events from this source (clean rebuild)
-- =========================================================
DELETE FROM core.company_default_events
WHERE source_table = 'ov.konkurz_restrukturalizacia_issues';

-- =========================================================
-- 2) Rebuild events with robust default_type classification
--    Uses: debtor_id -> actors.id -> actors.cin (ICO)
--    Date: COALESCE(released_date, published_at)
-- =========================================================
INSERT INTO core.company_default_events (
  ico, default_date, default_type,
  source_table, source_id,
  bulletin_issue_id, published_at
)
SELECT
  a.cin::text AS ico,
  COALESCE(i.released_date::date, b.published_at::date) AS default_date,

  CASE
    -- HARD DEFAULT: bankruptcy declared (exclude cancellation/stoppage/rejection)
    WHEN (
          (i.heading ILIKE '%vyhlásenie konkurzu%'
           OR i.decision ILIKE '%vyhlásenie konkurzu%'
           OR i.heading ILIKE '%vyhlasuje konkurz%'
           OR i.decision ILIKE '%vyhlasuje konkurz%')
          AND NOT (
            COALESCE(i.heading,'')  ~* '(zrušen|zrušuj|zastaven|zastavuj|odmietn|zamietn)'
            OR COALESCE(i.decision,'') ~* '(zrušen|zrušuj|zastaven|zastavuj|odmietn|zamietn)'
          )
         )
    THEN 'bankruptcy_declared'

    -- SOFT DEFAULT: restructuring allowed (exclude stoppage/rejection/cancellation)
    WHEN (
          (i.heading ILIKE '%povolenie reštrukturalizácie%'
           OR i.decision ILIKE '%povolenie reštrukturalizácie%'
           OR i.heading ILIKE '%povoľuje reštrukturalizáciu%'
           OR i.decision ILIKE '%povoľuje reštrukturalizáciu%')
          AND NOT (
            COALESCE(i.heading,'')  ~* '(zrušen|zrušuj|zastaven|zastavuj|odmietn|zamietn)'
            OR COALESCE(i.decision,'') ~* '(zrušen|zrušuj|zastaven|zastavuj|odmietn|zamietn)'
          )
         )
    THEN 'restructuring_allowed'

    ELSE NULL
  END AS default_type,

  'ov.konkurz_restrukturalizacia_issues' AS source_table,
  i.id::bigint AS source_id,
  i.bulletin_issue_id,
  b.published_at
FROM ov.konkurz_restrukturalizacia_issues i
JOIN ov.konkurz_restrukturalizacia_actors a
  ON a.id = i.debtor_id
LEFT JOIN ov.bulletin_issues b
  ON b.id = i.bulletin_issue_id
WHERE a.cin IS NOT NULL
  AND (
    (
      (i.heading ILIKE '%vyhlásenie konkurzu%'
       OR i.decision ILIKE '%vyhlásenie konkurzu%'
       OR i.heading ILIKE '%vyhlasuje konkurz%'
       OR i.decision ILIKE '%vyhlasuje konkurz%')
      AND NOT (
        COALESCE(i.heading,'')  ~* '(zrušen|zrušuj|zastaven|zastavuj|odmietn|zamietn)'
        OR COALESCE(i.decision,'') ~* '(zrušen|zrušuj|zastaven|zastavuj|odmietn|zamietn)'
      )
    )
    OR
    (
      (i.heading ILIKE '%povolenie reštrukturalizácie%'
       OR i.decision ILIKE '%povolenie reštrukturalizácie%'
       OR i.heading ILIKE '%povoľuje reštrukturalizáciu%'
       OR i.decision ILIKE '%povoľuje reštrukturalizáciu%')
      AND NOT (
        COALESCE(i.heading,'')  ~* '(zrušen|zrušuj|zastaven|zastavuj|odmietn|zamietn)'
        OR COALESCE(i.decision,'') ~* '(zrušen|zrušuj|zastaven|zastavuj|odmietn|zamietn)'
      )
    )
  );

-- =========================================================
-- 3) Refresh first-default per ICO
-- =========================================================
REFRESH MATERIALIZED VIEW core.company_first_default;

-- =========================================================
-- 4) Rebuild ML labels (12m / 24m) for norm_period = 1
--    - FALSE when no default date (avoid NULL -> NOT NULL error)
-- =========================================================
TRUNCATE TABLE core.ml_bankruptcy_labels;

INSERT INTO core.ml_bankruptcy_labels (
  report_id, ico, fiscal_year, period_end,
  first_default_date,
  default_within_12m, default_within_24m
)
SELECT
  a.report_id,
  a.ico,
  a.fiscal_year,
  a.period_end,
  d.first_default_date,

  COALESCE(
    (d.first_default_date > a.period_end AND d.first_default_date <= (a.period_end + INTERVAL '12 months')),
    FALSE
  ) AS default_within_12m,

  COALESCE(
    (d.first_default_date > a.period_end AND d.first_default_date <= (a.period_end + INTERVAL '24 months')),
    FALSE
  ) AS default_within_24m

FROM core.fin_annual_aggregates a
LEFT JOIN core.company_first_default d
  ON d.ico = a.ico
WHERE a.norm_period = 1;

COMMIT;

-- =========================================================
-- 5) Sanity checks
-- =========================================================

-- Default-type distribution (events)
SELECT default_type, COUNT(*) AS n
FROM core.company_default_events
WHERE source_table = 'ov.konkurz_restrukturalizacia_issues'
GROUP BY default_type
ORDER BY n DESC;

-- Label rates
SELECT
  COUNT(*) AS n,
  COUNT(*) FILTER (WHERE default_within_12m) AS n_def_12m,
  ROUND(100.0 * COUNT(*) FILTER (WHERE default_within_12m) / NULLIF(COUNT(*),0), 3) AS def_rate_12m_pct,
  COUNT(*) FILTER (WHERE default_within_24m) AS n_def_24m,
  ROUND(100.0 * COUNT(*) FILTER (WHERE default_within_24m) / NULLIF(COUNT(*),0), 3) AS def_rate_24m_pct
FROM core.ml_bankruptcy_labels;

CREATE OR REPLACE VIEW core.ml_train_12m AS
SELECT
  f.*,
  l.default_within_12m,
  l.first_default_date
FROM core.fin_annual_features f
JOIN core.ml_bankruptcy_labels l ON l.report_id = f.report_id
WHERE f.norm_period = 1
  AND (l.first_default_date IS NULL OR l.first_default_date > l.period_end)
  AND l.period_end <= (CURRENT_DATE - INTERVAL '12 months');

CREATE OR REPLACE VIEW core.company_years AS
SELECT DISTINCT
  ico,
  fiscal_year,
  period_end,
  report_id
FROM core.fin_annual_aggregates
WHERE norm_period = 1
  AND period_end IS NOT NULL;

CREATE OR REPLACE VIEW core.companies_with_3y_continuity AS
WITH y AS (
  SELECT ico, fiscal_year
  FROM core.company_years
  GROUP BY ico, fiscal_year
),
seq AS (
  SELECT
    ico,
    fiscal_year,
    fiscal_year - ROW_NUMBER() OVER (PARTITION BY ico ORDER BY fiscal_year) AS grp
  FROM y
),
runs AS (
  SELECT
    ico,
    grp,
    MIN(fiscal_year) AS y_start,
    MAX(fiscal_year) AS y_end,
    COUNT(*) AS len
  FROM seq
  GROUP BY ico, grp
)
SELECT DISTINCT ico
FROM runs
WHERE len >= 3;

CREATE OR REPLACE VIEW core.ml_train_12m_cont3_full AS
WITH base AS (
  SELECT
    f.*,
    l.default_within_12m,
    l.first_default_date
  FROM core.fin_annual_features f
  JOIN core.ml_bankruptcy_labels l
    ON l.report_id = f.report_id
  WHERE f.norm_period = 1
    AND (l.first_default_date IS NULL OR l.first_default_date > f.period_end)
    AND f.period_end <= (CURRENT_DATE - INTERVAL '12 months')
),
valid_years AS (
  SELECT DISTINCT
    ico,
    fiscal_year
  FROM core.fin_annual_aggregates
  WHERE norm_period = 1
    AND total_assets IS NOT NULL AND total_assets <> 0
    AND total_liabilities IS NOT NULL
    AND equity IS NOT NULL
    AND revenue IS NOT NULL
    AND net_income IS NOT NULL
)
SELECT b.*
FROM base b
JOIN valid_years y0 ON y0.ico=b.ico AND y0.fiscal_year=b.fiscal_year
JOIN valid_years y1 ON y1.ico=b.ico AND y1.fiscal_year=b.fiscal_year-1
JOIN valid_years y2 ON y2.ico=b.ico AND y2.fiscal_year=b.fiscal_year-2;

CREATE OR REPLACE VIEW core.ml_train_set AS
SELECT *
FROM core.ml_train_12m_cont3_full
WHERE fiscal_year BETWEEN 2016 AND 2020;

CREATE OR REPLACE VIEW core.ml_valid_set AS
SELECT *
FROM core.ml_train_12m_cont3_full
WHERE fiscal_year BETWEEN 2021 AND 2022;

CREATE OR REPLACE VIEW core.ml_test_set AS
SELECT *
FROM core.ml_train_12m_cont3_full
WHERE fiscal_year = 2023;

CREATE TABLE IF NOT EXISTS core.ml_model_registry (
  id            bigserial PRIMARY KEY,
  name          text NOT NULL,
  horizon       text NOT NULL,
  algo          text NOT NULL,
  trained_from  int,
  trained_to    int,
  feature_list  jsonb NOT NULL,
  metrics       jsonb NOT NULL,
  model_blob    bytea NOT NULL,
  created_at    timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_ml_model_registry_name_created
  ON core.ml_model_registry (name, created_at DESC);

CREATE TABLE IF NOT EXISTS core.ml_pd_predictions (
  report_id    bigint PRIMARY KEY,
  ico          text NOT NULL,
  fiscal_year  int NOT NULL,
  period_end   date NOT NULL,
  pd_12m       numeric NOT NULL,
  model_id     bigint NOT NULL REFERENCES core.ml_model_registry(id),
  created_at   timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_ml_pd_predictions_ico_year
  ON core.ml_pd_predictions (ico, fiscal_year);

CREATE OR REPLACE VIEW core.ml_score_set AS
WITH valid_years AS (
  SELECT DISTINCT
    ico,
    fiscal_year
  FROM core.fin_annual_aggregates
  WHERE norm_period = 1
    AND period_end IS NOT NULL
    AND total_assets IS NOT NULL AND total_assets <> 0
    AND total_liabilities IS NOT NULL
    AND equity IS NOT NULL
    AND revenue IS NOT NULL
    AND net_income IS NOT NULL
),
eligible_years AS (
  -- rok t je eligible, ak existujú aj t-1 a t-2 ako valid
  SELECT
    y0.ico,
    y0.fiscal_year
  FROM valid_years y0
  JOIN valid_years y1 ON y1.ico = y0.ico AND y1.fiscal_year = y0.fiscal_year - 1
  JOIN valid_years y2 ON y2.ico = y0.ico AND y2.fiscal_year = y0.fiscal_year - 2
),
latest_per_ico AS (
  SELECT
    ico,
    MAX(fiscal_year) AS fiscal_year
  FROM eligible_years
  GROUP BY ico
)
SELECT
  f.report_id, f.ico, f.fiscal_year, f.period_end,
  f.current_ratio, f.quick_ratio, f.cash_ratio,
  f.equity_ratio, f.debt_ratio, f.debt_to_equity,
  f.roa, f.roe, f.net_margin, f.asset_turnover,
  f.interest_coverage,
  f.x04_noncurrent_indebtedness, f.x07_interest_burden, f.x08_debt_to_cf,
  f.x09_equity_leverage, f.x10_insolvency,
  f.model_sk_raw, f.model_sk_pct,
  f.negative_equity_flag, f.liquidity_breach_flag, f.high_leverage_flag, f.loss_flag
FROM core.fin_annual_features f
JOIN latest_per_ico l
  ON l.ico = f.ico AND l.fiscal_year = f.fiscal_year
WHERE f.norm_period = 1;

ALTER TABLE core.ml_pd_predictions
ADD COLUMN IF NOT EXISTS pd_pct numeric;

WITH ranked AS (
  SELECT
    report_id,
    PERCENT_RANK() OVER (
      PARTITION BY fiscal_year
      ORDER BY pd_12m
    ) AS pd_pct
  FROM core.ml_pd_predictions
  WHERE model_id = (SELECT id FROM core.ml_model_registry ORDER BY created_at DESC LIMIT 1)
)
UPDATE core.ml_pd_predictions p
SET pd_pct = r.pd_pct
FROM ranked r
WHERE p.report_id = r.report_id;
