SQL_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS core;

-- =====================
-- RPO
-- =====================
CREATE TABLE IF NOT EXISTS core.rpo_all_orgs (
    ico              TEXT PRIMARY KEY,
    name             TEXT,
    legal_form       TEXT,
    legal_form_code  TEXT,
    legal_form_name  TEXT,
    status           TEXT,
    address          TEXT,
    updated_at       TIMESTAMPTZ DEFAULT now()
);

-- Backward-compatible migrations (safe on existing DBs)
ALTER TABLE IF EXISTS core.rpo_all_orgs
  ADD COLUMN IF NOT EXISTS legal_form_code TEXT;
ALTER TABLE IF EXISTS core.rpo_all_orgs
  ADD COLUMN IF NOT EXISTS legal_form_name TEXT;

CREATE TABLE IF NOT EXISTS core.rpo_bulk_state (
    id                  SMALLINT PRIMARY KEY DEFAULT 1,
    last_init_key       TEXT,
    last_daily_key      TEXT,

    -- Slovensko.Digital (Datahub) sync cursor
    sd_since            TIMESTAMPTZ,
    sd_last_id          BIGINT,

    last_run_at         TIMESTAMPTZ
);

-- Backward-compatible migrations (safe on existing DBs)
ALTER TABLE IF EXISTS core.rpo_bulk_state
  ADD COLUMN IF NOT EXISTS sd_since TIMESTAMPTZ;

ALTER TABLE IF EXISTS core.rpo_bulk_state
  ADD COLUMN IF NOT EXISTS sd_last_id BIGINT;

INSERT INTO core.rpo_bulk_state (id) VALUES (1)
ON CONFLICT (id) DO NOTHING;

-- =====================
-- RÚZ: Units
-- =====================
CREATE TABLE IF NOT EXISTS core.ruz_units (
    id                       BIGINT PRIMARY KEY,
    ico                      TEXT,
    id_uctovnych_zavierok    JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at               TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ruz_units_ico ON core.ruz_units(ico);

CREATE TABLE IF NOT EXISTS core.ruz_unit_zavierky (
    unit_id      BIGINT NOT NULL,
    zavierka_id  BIGINT NOT NULL,
    PRIMARY KEY (unit_id, zavierka_id)
);
CREATE INDEX IF NOT EXISTS ix_ruz_unit_zavierky_zid ON core.ruz_unit_zavierky(zavierka_id);

CREATE TABLE IF NOT EXISTS core.ruz_units_state (
    id                SMALLINT PRIMARY KEY DEFAULT 1,
    zmenene_od        TIMESTAMPTZ,
    pokracovat_za_id  BIGINT,
    last_run_at       TIMESTAMPTZ
);
INSERT INTO core.ruz_units_state (id) VALUES (1)
ON CONFLICT (id) DO NOTHING;

-- =====================
-- RÚZ: Statements (účtovné závierky)
-- =====================
CREATE TABLE IF NOT EXISTS core.ruz_statements (
    id                      BIGINT PRIMARY KEY,
    id_uctovnej_jednotky    BIGINT,
    obdobie_od              DATE,
    obdobie_do              DATE,
    druh_zavierky           TEXT,
    typ_zavierky            TEXT,
    pristupnost_dat         TEXT,
    datum_poslednej_upravy  DATE,
    id_uctovnych_vykazov    JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at              TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_ruz_statements_unit ON core.ruz_statements(id_uctovnej_jednotky);

-- =====================
-- RÚZ: Reports (účtovné výkazy)
-- =====================
CREATE TABLE IF NOT EXISTS core.ruz_reports (
    id                      BIGINT PRIMARY KEY,
    ico                     TEXT,
    id_uctovnej_zavierky    BIGINT,
    id_vyrocnej_spravy      BIGINT,
    id_sablony              BIGINT,
    mena                    TEXT,
    pristupnost             TEXT,
    pocet_stran             INTEGER,
    jazyk                   TEXT,
    zdroj_dat               TEXT,
    datum_poslednej_upravy  DATE,
    titulna                 JSONB,
    tabulky                 JSONB,
    prilohy                 JSONB,
    updated_at              TIMESTAMPTZ DEFAULT now()
);
ALTER TABLE core.ruz_reports
ADD COLUMN IF NOT EXISTS dq_ico_conflict BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS ix_ruz_reports_ico  ON core.ruz_reports(ico);
CREATE INDEX IF NOT EXISTS ix_ruz_reports_year ON core.ruz_reports(((titulna->>'obdobieDo')));

-- (Voliteľné) row-level staging, ak používaš
CREATE TABLE IF NOT EXISTS core.ruz_report_rows (
    row_id        BIGSERIAL PRIMARY KEY,
    report_id     BIGINT NOT NULL,
    table_name    TEXT,
    row_idx       INTEGER,
    row_key       TEXT,
    row_name      TEXT,
    cells         JSONB,
    updated_at    TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_rrows_report ON core.ruz_report_rows(report_id);
CREATE INDEX IF NOT EXISTS ix_rrows_table  ON core.ruz_report_rows(report_id, table_name);

CREATE TABLE IF NOT EXISTS core.ruz_reports_sync_state (
  id            int PRIMARY KEY,
  last_report_id bigint NOT NULL DEFAULT 0,
  updated_at    timestamptz NOT NULL DEFAULT now()
);

INSERT INTO core.ruz_reports_sync_state (id, last_report_id)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS core.ruz_report_template_map (
  report_id   bigint PRIMARY KEY,
  id_sablony  int,
  tombstone   boolean NOT NULL DEFAULT false,
  fetched_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_ruz_template_map_sablony ON core.ruz_report_template_map (id_sablony);


-- =====================
-- RÚZ: Templates (šablóny)
-- =====================
CREATE TABLE IF NOT EXISTS core.ruz_templates (
    id            BIGINT PRIMARY KEY,
    nazov         TEXT,
    nariadenie_mf TEXT,
    platne_od     DATE,
    platne_do     DATE,
    raw           JSONB,
    updated_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.ruz_template_rows (
    template_id     BIGINT NOT NULL,
    table_idx       INTEGER NOT NULL,
    table_name      TEXT,
    row_number      INTEGER NOT NULL,
    oznacenie       TEXT,
    row_text        TEXT,
    PRIMARY KEY (template_id, table_idx, row_number)
);
CREATE INDEX IF NOT EXISTS ix_tpl_rows_template ON core.ruz_template_rows(template_id);

-- =====================
-- Derived: Report items (template row -> numeric value only; NO raw)
-- =====================
CREATE TABLE IF NOT EXISTS core.ruz_report_items (
    report_id       BIGINT NOT NULL,
    template_id     BIGINT,
    ico             TEXT,
    pravna_forma    TEXT,
    obdobie_do      TEXT,
    table_idx       INTEGER NOT NULL,
    table_name      TEXT,
    row_number      INTEGER NOT NULL,
    oznacenie       TEXT,
    period_col      SMALLINT NOT NULL, -- 1=current, 2=previous (per template)
    value_num       NUMERIC,
    updated_at      TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (report_id, table_idx, row_number, period_col)
);
CREATE INDEX IF NOT EXISTS ix_ritems_ico ON core.ruz_report_items(ico);
CREATE INDEX IF NOT EXISTS ix_ritems_pravna_forma ON core.ruz_report_items(pravna_forma);
CREATE INDEX IF NOT EXISTS ix_ritems_obdobie_do ON core.ruz_report_items(obdobie_do);


-- =====================
-- State: ruz_report_items (checkpoint for keyset pagination)
-- =====================
CREATE TABLE IF NOT EXISTS core.ruz_report_items_state (
    id              SMALLINT PRIMARY KEY DEFAULT 1,
    last_report_id  BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ DEFAULT now()
);

INSERT INTO core.ruz_report_items_state (id, last_report_id)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS core.ruz_report_items_done (
  report_id  bigint PRIMARY KEY,
  template_id bigint,
  processed_at timestamptz DEFAULT now(),
  items_written integer DEFAULT 0
);

CREATE INDEX IF NOT EXISTS ruz_report_items_done_tpl_idx
  ON core.ruz_report_items_done (template_id);
  
-- =========================
-- SD – Activity code dim
-- =========================
CREATE TABLE IF NOT EXISTS core.sd_activity_code_dim (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

-- =========================
-- SD – Organization master
-- =========================
CREATE TABLE IF NOT EXISTS core.sd_org (
    ico BIGINT PRIMARY KEY,
    sd_org_id BIGINT UNIQUE NOT NULL,
    established_on DATE,
    terminated_on DATE,
    actualized_at TIMESTAMPTZ,
    main_activity_code_id BIGINT REFERENCES core.sd_activity_code_dim(id),
    main_activity_code_name TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE core.sd_org
ADD COLUMN IF NOT EXISTS nace_division TEXT;

UPDATE core.sd_org
SET nace_division = LEFT(main_activity_code_id::text, 2);

-- =========================
-- SD – Addresses
-- =========================
CREATE TABLE IF NOT EXISTS core.sd_org_address (
    id BIGSERIAL PRIMARY KEY,
    ico BIGINT NOT NULL REFERENCES core.sd_org(ico) ON DELETE CASCADE,
    sd_org_id BIGINT NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT FALSE,
    effective_from DATE,
    effective_to DATE,
    street TEXT,
    building_number TEXT,
    reg_number INT,
    postal_code TEXT,
    municipality TEXT,
    district TEXT,
    country TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sd_org_address_current
    ON core.sd_org_address (ico, is_current);

-- =========================
-- SD – Successors
-- =========================
CREATE TABLE IF NOT EXISTS core.sd_org_successor (
    id BIGSERIAL PRIMARY KEY,
    ico BIGINT NOT NULL REFERENCES core.sd_org(ico) ON DELETE CASCADE,
    sd_org_id BIGINT NOT NULL,
    successor_cin BIGINT,
    successor_name TEXT,
    successor_established_on DATE,
    successor_terminated_on DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""
