from __future__ import annotations

import logging
import os
import sys

from ..db import get_conn, ensure_schema
from . import (
    rpo_bulk,
    ruz_units,
    ruz_statements,
    ruz_reports,
    fin_ddl,
    fin_etl,
    ruz_templates,
    ruz_report_items,
)
from ..ml.pd_model import run as ml_pd_run

# NEW: Slovensko.Digital (Datahub) enrichment
from .sd_org import run_sync as sd_org_sync

# NEW: report items test module (writes into core.ruz_report_items_test)
#from . import ruz_report_items_test


def _setup_logging_if_needed() -> None:
    """
    Configure logging once, safely.
    - If logging was already configured elsewhere (e.g., __main__.py), do nothing.
    - Otherwise log to STDOUT (Railway often flags STDERR as "error").
    """
    root = logging.getLogger()
    if root.handlers:
        return

    logging.basicConfig(
        level=os.getenv("FINDEXIO_LOG_LEVEL", "INFO").upper(),
        stream=sys.stdout,
        format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


_setup_logging_if_needed()
log = logging.getLogger("findexio.runner")


def ensure_db() -> None:
    with get_conn() as conn:
        ensure_schema(conn)


def bootstrap() -> None:
    """
    Full pipeline run.

    Order (dependencies):
      rpo_bulk -> ruz_units -> ruz_statements -> ruz_reports -> ruz_templates -> ruz_report_items -> sd_org

    Notes:
      - ruz_report_items is intentionally limited to legal forms 112 (s.r.o.) and 121 (a.s.).
      - sd_org uses SD/Datahub RPO API; it links by ICO extracted from identifier_entries[].ipo.
    """
    ensure_db()
    log.info("Starting BOOTSTRAP pipeline...")

    # Base registries + RUZ
    rpo_bulk.run_full_sync(apply_daily=False)
    ruz_units.run_sync()
    ruz_statements.run_sync(refresh_all=False)
    ruz_reports.run_sync(refresh_all=False, template_id_only=699)

    # Templates + exploded report items (BI/ML-ready)
    log.info("Running ruz_templates...")
    ruz_templates.run_sync()

    log.info("Running ruz_report_items (legal_forms=112,121)...")
    ruz_report_items.run_sync(legal_forms=("112", "121"))

    # SD enrichment (sync endpoint + DB batch upserts; uses cursor sd_since/sd_last_id)
    log.info("Running sd_org (Slovensko.Digital enrichment)...")
    sd_org_sync()

    log.info("BOOTSTRAP finished.")


def daily() -> None:
    """
    Daily run of the same pipeline.

    Note:
      Keeping the same order ensures templates/report_items stay in sync with newly fetched reports,
      and SD enrichment runs after base data is present.
    """
    ensure_db()
    log.info("Starting DAILY pipeline...")

    # rpo_bulk.run_full_sync(apply_daily=True)    # REGISTER PRAVNICKYCH OSOB
    # ruz_units.run_sync()                        # UCTOVNE JEDNOTKY
    # ruz_statements.run_sync(refresh_all=False)  # UCTOVNE ZAVIERKY PRE JEDNOTLIVE UCTOVNE JEDNOTKY
    ruz_reports.run_sync(refresh_all=False, template_id_only=(699,687,22,21), candidate_limit=600000, reset_cursor=True)  # OBSAH UCTOVNYCH ZAVIEROK

    # log.info("Running ruz_templates...")
    # ruz_templates.run_sync()                    # SABLONY PRE OBSAH UCTOVNYCH ZAVIEROK

    # log.info("Running ruz_report_items (legal_forms=112,121)...")
    # ruz_report_items.run_sync(legal_forms=("112", "121",), hard_limit=100000,use_state_cursor=False)  # NAPAROVANIE SABLON S OBSAHOM UCTOVNYCH ZAVIEROK

    # SD enrichment (incremental)
    # log.info("Running sd_org (Slovensko.Digital enrichment)...")
    # sd_org_sync()

    # log.info("Running FIN_ETL...")
    # fin_etl.run()

    log.info("DAILY finished.")


def update02() -> None:
    # V0.2 additions only
    ensure_db()

    log.info("Running ruz_templates...")
    ruz_templates.run_sync()

    log.info("Running ruz_report_items (legal_forms=112,121)...")
    ruz_report_items.run_sync(legal_forms=("112", "121"), template_ids=699, hard_limit=50000, use_state_cursor=False)

    log.info("V0.2 update finished.")


def fin_ddl_run() -> None:
    """
    Applies DB objects for financial health pipeline (DDL):
      - parse_ruztxt_date()
      - fin_item_map
      - fin_annual_aggregates
      - fin_annual_features
      - fin_health_grade
      - indexes
    """
    ensure_db()
    log.info("Starting FIN_DDL...")
    fin_ddl.run()
    log.info("FIN_DDL finished.")


def fin_etl_run() -> None:
    """
    Refreshes financial health ETL (DML):
      aggregates -> features -> grades

    Requires:
      - ruz_report_items already populated
      - fin_item_map populated for template 699
    """
    ensure_db()
    log.info("Starting FIN_ETL...")
    fin_etl.run()
    log.info("FIN_ETL finished.")


def sd_org_run(*, hard_limit: int | None = None, db_batch_size: int = 200) -> None:
    """
    Manual runner for Slovensko.Digital enrichment.
    - Uses SD sync cursor (sd_since/sd_last_id) stored in core.rpo_bulk_state
    - Writes into:
        core.sd_activity_code_dim
        core.sd_org
        core.sd_org_address
        core.sd_org_successor
    """
    ensure_db()
    log.info("Starting SD_ORG sync...")
    sd_org_sync(hard_limit=hard_limit, db_batch_size=db_batch_size)
    log.info("SD_ORG sync finished.")


def backfill_report_items_year(*, year: int = 2024) -> None:
    """
    One-off backfill of ruz_report_items for a specific year.
    - Does NOT move ruz_report_items_state.last_report_id
    - Safe to run multiple times
    """
    ensure_db()
    log.info("Starting BACKFILL ruz_report_items for year=%s", year)

    with get_conn() as conn:
        rows = conn.execute(
            """
            WITH latest AS (
              SELECT
                r.id AS report_id
              FROM core.ruz_reports r
              LEFT JOIN core.ruz_report_items i
                ON i.report_id = r.id
              WHERE r.id_sablony = 699
                AND (r.titulna->>'obdobieDo') LIKE %s
                AND i.report_id IS NULL
            )
            SELECT report_id FROM latest
            """,
            (f"{year}%",),
        ).fetchall()

    report_ids = [r[0] for r in rows]

    log.info("Found %s reports from %s without items", len(report_ids), year)

    if not report_ids:
        log.info("Nothing to backfill.")
        return

    ruz_report_items.run_sync(
        legal_forms=("112", "121"),
        template_ids=699,
        report_ids=report_ids,
        hard_limit=50000,
        # 🔒 CRITICAL
        use_state_cursor=False,
        update_state=False,
    )

    log.info("BACKFILL %s finished.", year)

"""
def test_items(*, per_template: int = 10, truncate_first: bool = True) -> None:
    
    Production-safe test extractor:
      - Writes ONLY into core.ruz_report_items_test
      - Samples:
          per_template x 699
          per_template x 687
          per_template paired statements => ~2*per_template reports of 21 and 22
      - Does NOT touch ruz_report_items_state
    
    #ensure_db()
    log.info("Starting TEST_ITEMS: per_template=%s truncate_first=%s", per_template, truncate_first)
    ruz_report_items_test.run_test_insert(
        per_template=per_template,
        legal_forms=("112", "121"),
        truncate_first=truncate_first,
    )
    log.info("TEST_ITEMS finished.")
"""

def ml_run() -> None:
    """
    ML PD pipeline:
      - trains models on core.ml_train_set/valid/test
      - registers best model into core.ml_model_registry
      - scores core.ml_score_set into core.ml_pd_predictions
    """
    ensure_db()
    log.info("Starting ML_RUN (PD 12m)...")
    ml_pd_run()
    log.info("ML_RUN finished.")