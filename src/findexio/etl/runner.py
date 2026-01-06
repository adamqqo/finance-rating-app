from __future__ import annotations

import logging

from ..db import get_conn, ensure_schema
from . import rpo_bulk, ruz_units, ruz_statements, ruz_reports

log = logging.getLogger("findexio.runner")
if not log.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%H:%M:%S"))
    log.addHandler(h)
log.setLevel(logging.INFO)


def ensure_db() -> None:
    with get_conn() as conn:
        ensure_schema(conn)


def bootstrap() -> None:
    ensure_db()
    log.info("Starting BOOTSTRAP pipeline...")
    rpo_bulk.run_full_sync(apply_daily=False)
    ruz_units.run_sync()
    ruz_statements.run_sync(refresh_all=False)
    ruz_reports.run_sync(refresh_all=False)
    log.info("BOOTSTRAP finished.")


def daily() -> None:
    ensure_db()
    log.info("Starting DAILY pipeline...")
    rpo_bulk.run_full_sync(apply_daily=True)
    ruz_units.run_sync()
    ruz_statements.run_sync(refresh_all=False)
    ruz_reports.run_sync(refresh_all=False)
    log.info("DAILY finished.")
