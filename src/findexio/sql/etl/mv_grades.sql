DROP MATERIALIZED VIEW IF EXISTS core.mv_top10_by_grade_year;

CREATE MATERIALIZED VIEW core.mv_top10_by_grade_year AS
WITH latest_feat AS (
  -- latest features per (ico, fiscal_year), fast in Postgres
  SELECT DISTINCT ON (f.ico, f.fiscal_year)
    f.ico,
    f.fiscal_year,
    f.total_assets,
    f.revenue,
    f.current_ratio,
    f.debt_ratio,
    f.roa,
    f.roe,
    f.net_margin,
    COALESCE(f.negative_equity_flag, false) AS negative_equity_flag,
    COALESCE(f.liquidity_breach_flag, false) AS liquidity_breach_flag,
    COALESCE(f.high_leverage_flag, false) AS high_leverage_flag,
    COALESCE(f.loss_flag, false) AS loss_flag
  FROM core.fin_annual_features f
  WHERE f.norm_period = 1
  ORDER BY f.ico, f.fiscal_year, f.period_end DESC NULLS LAST, f.report_id DESC
),
joined AS (
  SELECT
    o.ico,
    o.name,
    o.legal_form_name,

    g.fiscal_year,
    g.grade,
    g.score_total,

    f.total_assets,
    f.revenue,
    f.current_ratio,
    f.debt_ratio,
    f.roa,
    f.roe,
    f.net_margin,

    (f.negative_equity_flag::int
     + f.liquidity_breach_flag::int
     + f.high_leverage_flag::int
     + f.loss_flag::int) AS flags_count,

    (LN(1 + GREATEST(COALESCE(f.total_assets,0),0))
     + LN(1 + GREATEST(COALESCE(f.revenue,0),0))) AS size_score

  FROM core.fin_health_grade g
  JOIN core.rpo_all_orgs o ON o.ico = g.ico
  LEFT JOIN latest_feat f
    ON f.ico = g.ico AND f.fiscal_year = g.fiscal_year
  WHERE g.norm_period = 1
    AND o.legal_form_code IN ('112','121')
    AND g.grade IS NOT NULL
    AND g.grade <> 'N'
),
pct AS (
  SELECT
    fiscal_year,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY size_score) AS p75_size
  FROM joined
  GROUP BY fiscal_year
),
filtered AS (
  SELECT j.*
  FROM joined j
  JOIN pct p USING (fiscal_year)
  WHERE j.size_score >= p.p75_size
),
ranked AS (
  SELECT
    f.*,
    ROW_NUMBER() OVER (
      PARTITION BY fiscal_year
      ORDER BY
        CASE grade
          WHEN 'A' THEN 1
          WHEN 'B' THEN 2
          WHEN 'C' THEN 3
          WHEN 'D' THEN 4
          ELSE 99
        END,
        score_total DESC NULLS LAST,
        size_score DESC NULLS LAST,
        flags_count ASC,
        roa DESC NULLS LAST,
        roe DESC NULLS LAST,
        net_margin DESC NULLS LAST,
        current_ratio DESC NULLS LAST,
        debt_ratio ASC NULLS LAST,
        name ASC
    ) AS rn
  FROM filtered f
)
SELECT
  ico, name, legal_form_name,
  fiscal_year, grade, score_total,
  total_assets, revenue,
  current_ratio, debt_ratio, roa, roe, net_margin,
  flags_count
FROM ranked
WHERE rn <= 10;

CREATE INDEX IF NOT EXISTS ix_mv_top10_by_grade_year_year
  ON core.mv_top10_by_grade_year (fiscal_year);

CREATE INDEX IF NOT EXISTS ix_mv_top10_by_grade_year_grade
  ON core.mv_top10_by_grade_year (grade);