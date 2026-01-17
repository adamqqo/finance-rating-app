CREATE INDEX IF NOT EXISTS ix_faa_ico_year_norm
ON core.fin_annual_aggregates(ico, fiscal_year, norm_period);

CREATE INDEX IF NOT EXISTS ix_faf_ico_year_norm
ON core.fin_annual_features(ico, fiscal_year, norm_period);

CREATE INDEX IF NOT EXISTS ix_fhg_ico_year
ON core.fin_health_grade(ico, fiscal_year);
