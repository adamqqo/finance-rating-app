from __future__ import annotations

import logging
import os
import sys

from ..db import get_conn, ensure_schema
from . import rpo_bulk, ruz_units, ruz_statements, ruz_reports, fin_ddl, fin_etl
from . import ruz_templates, ruz_report_items


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
      rpo_bulk -> ruz_units -> ruz_statements -> ruz_reports -> ruz_templates -> ruz_report_items

    Note:
      ruz_report_items is intentionally limited to legal forms 112 (s.r.o.) and 121 (a.s.).
    """
    ensure_db()
    log.info("Starting BOOTSTRAP pipeline...")

    rpo_bulk.run_full_sync(apply_daily=False)
    ruz_units.run_sync()
    ruz_statements.run_sync(refresh_all=False)
    ruz_reports.run_sync(refresh_all=False)

    # NEW: templates + exploded report items (BI/ML-ready)
    log.info("Running ruz_templates...")
    ruz_templates.run_sync()

    log.info("Running ruz_report_items (legal_forms=112,121)...")
    ruz_report_items.run_sync(legal_forms=("112", "121"))

    log.info("BOOTSTRAP finished.")


def daily() -> None:
    """
    Daily run of the same pipeline.

    Note:
      Keeping the same order ensures templates and report_items stay in sync with newly fetched reports.
    """
    ensure_db()
    log.info("Starting DAILY pipeline...")

    rpo_bulk.run_full_sync(apply_daily=True)   ##REGISTER PRAVNICKYCH OSOB
    ruz_units.run_sync()                       ##UCTOVNE JEDNOTKY
    ruz_statements.run_sync(refresh_all=False) ##UCTOVNE ZAVIERKY PRE JEDNOTLIVE UCTOVNE JEDNOTKY
    ruz_reports.run_sync(refresh_all=False)    ##OBSAH UCTOVNYCH ZAVIEROK

    # NEW: templates + exploded report items (BI/ML-ready)
    log.info("Running ruz_templates...")
    ruz_templates.run_sync()                  ##SABLONY PRE OBSAH UCTOVNYCH ZAVIEROK

    log.info("Running ruz_report_items (legal_forms=112,121)...")
    ruz_report_items.run_sync(legal_forms=("112", "121")) ##NAPAROVANIE SABLON S OBSAHOM UCTOVNYCH ZAVIEROK
    log.info("Running FIN_ETL...")
    fin_etl.run()
    log.info("DAILY finished.")

def update02() -> None:
   #V0.2 additions only
    ensure_db()
    log.info("Running ruz_templates...")
    ruz_templates.run_sync()

    log.info("Running ruz_report_items (legal_forms=112,121)...")
    ruz_report_items.run_sync(legal_forms=("112", "121"), template_ids=699, hard_limit=20000)

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

def backfill_report_items_year(*, year: int = 2021) -> None:
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
            """
            ,
            (f"{year}%",)
        ).fetchall()

    report_ids = [r[0] for r in rows]

    log.info(
        "Found %s reports from %s without items",
        len(report_ids),
        year,
    )

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
