CREATE OR REPLACE FUNCTION core.parse_ruztxt_date(x TEXT)
RETURNS DATE
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN x IS NULL OR btrim(x) = '' THEN NULL
    WHEN x ~ '^\d{4}-\d{2}-\d{2}$' THEN x::date
    WHEN x ~ '^\d{4}-\d{2}$' THEN (x || '-01')::date + interval '1 month - 1 day'
    WHEN x ~ '^\d{4}$' THEN (x || '-12-31')::date
    ELSE NULL
  END;
$$;
