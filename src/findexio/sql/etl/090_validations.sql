-- 1) Bilancia sa uzatvára (norm_period=1)
SELECT
  percentile_cont(0.50) WITHIN GROUP (ORDER BY abs(total_assets - (equity + total_liabilities))) AS p50_abs_diff,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY abs(total_assets - (equity + total_liabilities))) AS p95_abs_diff
FROM core.fin_annual_aggregates
WHERE norm_period = 1
  AND total_assets IS NOT NULL
  AND equity IS NOT NULL
  AND total_liabilities IS NOT NULL;

-- 2) Distribúcia známok
SELECT grade, COUNT(*) AS n
FROM core.fin_health_grade
WHERE norm_period = 1
GROUP BY grade
ORDER BY grade;
