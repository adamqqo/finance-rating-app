INSERT INTO core.fin_health_grade (
    report_id, norm_period,
    ico, fiscal_year, statement_id, period_end,
    score_total, score_capital, score_profit, score_liq_bonus, score_nwc_pen,
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
        f.nwc_to_assets
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
        END AS score_nwc_pen
    FROM src s
),
final AS (
    SELECT
        report_id, norm_period, ico, fiscal_year, statement_id, period_end,
        score_capital, score_profit, score_liq_bonus, score_nwc_pen,

        GREATEST(0, LEAST(100, (score_capital + score_profit + score_liq_bonus + score_nwc_pen)))::NUMERIC AS score_total,

        CASE
          WHEN hard_fail_f THEN 'F'
          WHEN hard_fail_e THEN 'E'
          WHEN (score_capital + score_profit + score_liq_bonus + score_nwc_pen) >= 80 THEN 'A'
          WHEN (score_capital + score_profit + score_liq_bonus + score_nwc_pen) >= 65 THEN 'B'
          WHEN (score_capital + score_profit + score_liq_bonus + score_nwc_pen) >= 50 THEN 'C'
          WHEN (score_capital + score_profit + score_liq_bonus + score_nwc_pen) >= 35 THEN 'D'
          WHEN (score_capital + score_profit + score_liq_bonus + score_nwc_pen) >= 20 THEN 'E'
          ELSE 'F'
        END AS grade,

        CASE
          WHEN hard_fail_f AND negative_equity_flag IS TRUE THEN 'F: negative equity'
          WHEN hard_fail_f AND (equity_ratio < -1) THEN 'F: equity_ratio < -1'
          WHEN hard_fail_f THEN 'F: hard fail'
          WHEN hard_fail_e THEN 'E: loss + negative equity_ratio'
          ELSE
            'OK: score=' ||
            GREATEST(0, LEAST(100, (score_capital + score_profit + score_liq_bonus + score_nwc_pen)))::text ||
            '; capital=' || score_capital::text ||
            '; profit=' || score_profit::text ||
            '; cash_bonus=' || score_liq_bonus::text ||
            '; nwc_pen=' || score_nwc_pen::text
        END AS reason
    FROM scored
)
SELECT
    report_id, norm_period,
    ico, fiscal_year, statement_id, period_end,
    score_total, score_capital, score_profit, score_liq_bonus, score_nwc_pen,
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
    grade = EXCLUDED.grade,
    reason = EXCLUDED.reason,
    updated_at = now();
