from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

from psycopg.rows import dict_row
from psycopg.types.json import Json

from ..config import RUZ_API_BASE
from ..db import get_conn, ensure_schema
from ..http import build_session, DEFAULT_TIMEOUT

SESSION = build_session(user_agent="Findexio/0.1 (ruz-templates)")
API_BASE = RUZ_API_BASE.rstrip("/")
DETAIL_PATH = "/api/sablona"
REQUEST_TIMEOUT = DEFAULT_TIMEOUT

log = logging.getLogger("findexio.ruz_templates")

SQL_COUNT_MISSING = """
SELECT COUNT(DISTINCT r.id_sablony) AS c
FROM core.ruz_reports r
LEFT JOIN core.ruz_templates t ON t.id = r.id_sablony
WHERE r.id_sablony IS NOT NULL AND t.id IS NULL;
"""

SQL_FETCH_MISSING_BATCH = """
SELECT DISTINCT r.id_sablony AS template_id
FROM core.ruz_reports r
LEFT JOIN core.ruz_templates t ON t.id = r.id_sablony
WHERE r.id_sablony IS NOT NULL AND t.id IS NULL
ORDER BY r.id_sablony
LIMIT %s OFFSET %s;
"""

SQL_UPSERT_TEMPLATE = """
INSERT INTO core.ruz_templates (id, nazov, nariadenie_mf, platne_od, platne_do, raw, updated_at)
VALUES (%(id)s, %(nazov)s, %(nariadenieMF)s, %(platneOd)s, %(platneDo)s, %(raw)s, now())
ON CONFLICT (id) DO UPDATE SET
  nazov = EXCLUDED.nazov,
  nariadenie_mf = EXCLUDED.nariadenie_mf,
  platne_od = EXCLUDED.platne_od,
  platne_do = EXCLUDED.platne_do,
  raw = EXCLUDED.raw,
  updated_at = now();
"""

SQL_DELETE_ROWS = "DELETE FROM core.ruz_template_rows WHERE template_id = %s;"

SQL_INSERT_ROW = """
INSERT INTO core.ruz_template_rows (
  template_id, table_idx, table_name, row_number, oznacenie, row_text
) VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (template_id, table_idx, row_number) DO UPDATE SET
  table_name = EXCLUDED.table_name,
  oznacenie = EXCLUDED.oznacenie,
  row_text = EXCLUDED.row_text;
"""


def _url(path: str) -> str:
    return f"{API_BASE}{path}"


def _to_date(s: Any) -> Optional[str]:
    if not s:
        return None
    ss = str(s).strip()[:10]
    try:
        datetime.strptime(ss, "%Y-%m-%d")
        return ss
    except Exception:
        return None


def fetch_template(template_id: int) -> Dict[str, Any]:
    r = SESSION.get(_url(DETAIL_PATH), params={"id": template_id}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def iter_template_rows(tpl: Dict[str, Any]) -> Iterator[Tuple[int, str, int, str, str]]:
    """Yield rows as (table_idx, table_name, row_number, oznacenie, row_text)."""
    tables = tpl.get("tabulky") or []
    if not isinstance(tables, list):
        return
    for tidx, t in enumerate(tables):
        tname = None
        nazov = t.get("nazov")
        if isinstance(nazov, dict):
            tname = nazov.get("sk") or nazov.get("en")
        if not tname:
            tname = t.get("name") or t.get("kod")

        rows = t.get("riadky") or []
        if not isinstance(rows, list):
            continue
        for r in rows:
            if not isinstance(r, dict):
                continue
            rn = r.get("cisloRiadku")
            try:
                rn_i = int(rn)
            except Exception:
                continue

            ozn = r.get("oznacenie")
            txt = r.get("text")
            row_text = None
            if isinstance(txt, dict):
                row_text = txt.get("sk") or txt.get("en")
            if not row_text:
                row_text = txt if isinstance(txt, str) else None

            yield (tidx, tname, rn_i, str(ozn) if ozn is not None else None, row_text)


def run_sync(*, batch_size: int = 2000, hard_limit: Optional[int] = None) -> None:
    """Fetch and store missing templates referenced by ruz_reports."""
    t0 = time.time()
    total = 0
    offset = 0

    with get_conn(row_factory=dict_row) as conn:
        ensure_schema(conn)

        total_candidates = int((conn.execute(SQL_COUNT_MISSING).fetchone() or {"c": 0})["c"])
        log.info("Missing templates=%d", total_candidates)

        while True:
            if hard_limit is not None and total >= hard_limit:
                break

            rows = conn.execute(SQL_FETCH_MISSING_BATCH, (batch_size, offset)).fetchall()
            if not rows:
                break

            ids = [int(r["template_id"]) for r in rows]

            with conn.transaction():
                with conn.cursor() as cur:
                    for tid in ids:
                        try:
                            tpl = fetch_template(tid)

                            params = {
                                "id": int(tpl.get("id")),
                                "nazov": tpl.get("nazov"),
                                "nariadenieMF": tpl.get("nariadenieMF"),
                                "platneOd": _to_date(tpl.get("platneOd")),
                                "platneDo": _to_date(tpl.get("platneDo")),
                                "raw": Json(tpl),
                            }
                            cur.execute(SQL_UPSERT_TEMPLATE, params)

                            cur.execute(SQL_DELETE_ROWS, (tid,))
                            for table_idx, table_name, row_number, oznacenie, row_text in iter_template_rows(tpl):
                                cur.execute(
                                    SQL_INSERT_ROW,
                                    (tid, table_idx, table_name, row_number, oznacenie, row_text),
                                )

                            total += 1
                        except Exception as e:
                            log.warning("Template fetch/store failed (id=%s): %s", tid, e)

                        if hard_limit is not None and total >= hard_limit:
                            break

            offset += len(ids)
            elapsed = time.time() - t0
            rate = total / elapsed if elapsed > 0 else 0.0
            log.info("Stored templates=%d/%d | offset=%d | batch=%d | speed=%.2f/s", total, total_candidates, offset, len(ids), rate)

        log.info("Done. Stored templates=%d | time=%.1fs", total, time.time() - t0)
