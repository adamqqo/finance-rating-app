from __future__ import annotations

import logging
import os
import sys

from ..db import get_conn, ensure_schema
from . import rpo_bulk, ruz_units, ruz_statements, ruz_reports
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

    rpo_bulk.run_full_sync(apply_daily=True)
    ruz_units.run_sync()
    ruz_statements.run_sync(refresh_all=False)
    ruz_reports.run_sync(refresh_all=False)

    # NEW: templates + exploded report items (BI/ML-ready)
    log.info("Running ruz_templates...")
    ruz_templates.run_sync()

    log.info("Running ruz_report_items (legal_forms=112,121)...")
    ruz_report_items.run_sync(legal_forms=("112", "121"))

    log.info("DAILY finished.")
