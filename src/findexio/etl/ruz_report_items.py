from __future__ import annotations

import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

from psycopg.rows import dict_row

from ..db import get_conn, ensure_schema

log = logging.getLogger("findexio.ruz_report_items")

LEGAL_FORMS_DEFAULT: Tuple[str, ...] = ("112", "121")

# Kandidáti: reporty so šablónou, s tabuľkami, pre zvolené právne formy,
# ktoré ešte nemajú žiadne itemy (inkrementálne).
SQL_COUNT_CANDIDATES = """
SELECT COUNT(*) AS c
FROM core.ruz_reports r
LEFT JOIN core.ruz_report_items i ON i.report_id = r.id
WHERE r.id_sablony IS NOT NULL
  AND (r.titulna->>'pravnaForma') = ANY(%s)
  AND r.tabulky IS NOT NULL
  AND i.report_id IS NULL;
"""

# KEYSET pagination: r.id > last_id, no OFFSET
SQL_FETCH_CANDIDATES = """
SELECT r.id AS report_id,
       r.id_sablony AS template_id,
       r.ico,
       r.titulna,
       r.tabulky
FROM core.ruz_reports r
LEFT JOIN core.ruz_report_items i ON i.report_id = r.id
WHERE r.id_sablony IS NOT NULL
  AND (r.titulna->>'pravnaForma') = ANY(%s)
  AND r.tabulky IS NOT NULL
  AND i.report_id IS NULL
  AND r.id > %s
ORDER BY r.id
LIMIT %s;
"""

SQL_FETCH_TEMPLATE_RAW = """
SELECT raw
FROM core.ruz_templates
WHERE id = %s;
"""

# NOTE: no value_raw
SQL_UPSERT_ITEM = """
INSERT INTO core.ruz_report_items (
  report_id, template_id, ico, pravna_forma, obdobie_do,
  table_idx, table_name, row_number, oznacenie, row_text,
  period_col, value_num, updated_at
) VALUES (
  %(report_id)s, %(template_id)s, %(ico)s, %(pravna_forma)s, %(obdobie_do)s,
  %(table_idx)s, %(table_name)s, %(row_number)s, %(oznacenie)s, %(row_text)s,
  %(period_col)s, %(value_num)s, now()
)
ON CONFLICT (report_id, table_idx, row_number, period_col) DO UPDATE SET
  template_id   = EXCLUDED.template_id,
  ico           = EXCLUDED.ico,
  pravna_forma  = EXCLUDED.pravna_forma,
  obdobie_do    = EXCLUDED.obdobie_do,
  table_name    = EXCLUDED.table_name,
  oznacenie     = EXCLUDED.oznacenie,
  row_text      = EXCLUDED.row_text,
  value_num     = EXCLUDED.value_num,
  updated_at    = now();
"""


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


def _parse_decimal(x: Any) -> Optional[Decimal]:
    """
    Parse value into Decimal.
    If parsing fails (non-numeric / empty), returns None.
    We intentionally do NOT persist raw values for report_items.
    """
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None

    # Normalize common formats conservatively:
    # - remove spaces used as thousands separators
    # - if comma appears and no dot, treat comma as thousands separator (remove)
    s2 = s.replace(" ", "")
    if s2.count(",") > 0 and s2.count(".") == 0:
        s2 = s2.replace(",", "")

    try:
        return Decimal(s2)
    except InvalidOperation:
        return None


def _extract_table_meta(
    tpl: Dict[str, Any],
    table_idx: int,
) -> Tuple[Optional[str], int, List[Dict[str, Any]]]:
    tables = tpl.get("tabulky") or []
    if not isinstance(tables, list) or table_idx >= len(tables):
        return None, 0, []

    t = tables[table_idx]

    tname: Optional[str] = None
    nazov = t.get("nazov")
    if isinstance(nazov, dict):
        tname = nazov.get("sk") or nazov.get("en")
    if not tname:
        tname = t.get("name") or t.get("kod")

    data_cols = t.get("pocetDatovychStlpcov")
    try:
        dc = int(data_cols)
    except Exception:
        dc = 0

    rows = t.get("riadky") or []
    if not isinstance(rows, list):
        rows = []

    return tname, dc, rows


def _index_for(row_number: int, data_cols: int, period_col: int) -> int:
    # row_number is 1-based, period_col is 1..data_cols
    return (row_number - 1) * data_cols + (period_col - 1)


def run_sync(
    *,
    batch_size: int = 200,
    hard_limit: Optional[int] = None,
    legal_forms: Tuple[str, ...] = LEGAL_FORMS_DEFAULT,
) -> None:
    """
    Explode ruz_reports.tabulky[...].data into normalized core.ruz_report_items
    using core.ruz_templates.raw.

    - Incremental: only reports without any report_items are processed.
    - Scoped: by default only legal forms 112 and 121.
    - Keyset pagination: uses last_id instead of OFFSET (much faster at scale).
    - No raw persistence: only numeric value_num is stored.
    """
    t0 = time.time()
    total_reports = 0
    total_items = 0
    last_id = 0  # keyset cursor

    with get_conn(row_factory=dict_row) as conn:
        ensure_schema(conn)

        total_candidates = int(
            (conn.execute(SQL_COUNT_CANDIDATES, (list(legal_forms),)).fetchone() or {"c": 0})["c"]
        )
        log.info("Candidate reports=%d | legal_forms=%s", total_candidates, ",".join(legal_forms))

        while True:
            if hard_limit is not None and total_reports >= hard_limit:
                break

            batch = conn.execute(SQL_FETCH_CANDIDATES, (list(legal_forms), last_id, batch_size)).fetchall()
            if not batch:
                break

            # advance keyset cursor immediately based on fetched rows
            # (so even if some rows fail, we don't get stuck refetching the same batch)
            last_id = int(batch[-1]["report_id"])

            with conn.transaction():
                with conn.cursor() as cur:
                    for r in batch:
                        rid = int(r["report_id"])
                        tid = int(r["template_id"])

                        tpl_row = cur.execute(SQL_FETCH_TEMPLATE_RAW, (tid,)).fetchone()
                        if not tpl_row or not tpl_row[0]:
                            log.warning("Missing template raw for template_id=%s (report_id=%s)", tid, rid)
                            total_reports += 1
                            if hard_limit is not None and total_reports >= hard_limit:
                                break
                            continue

                        tpl = tpl_row[0]
                        titulna = r.get("titulna") or {}
                        tabulky = r.get("tabulky") or []

                        pravna_forma = _safe_str(titulna.get("pravnaForma"))
                        obdobie_do = _safe_str(titulna.get("obdobieDo"))
                        ico = _safe_str(r.get("ico"))

                        if not isinstance(tabulky, list):
                            total_reports += 1
                            if hard_limit is not None and total_reports >= hard_limit:
                                break
                            continue

                        for table_idx, t in enumerate(tabulky):
                            if not isinstance(t, dict):
                                continue

                            data = t.get("data")
                            if not isinstance(data, list) or not data:
                                continue

                            tname, data_cols, tpl_rows = _extract_table_meta(tpl, table_idx)
                            if data_cols <= 0:
                                # conservative fallback (common for POD templates)
                                data_cols = 2

                            # cisloRiadku -> (oznacenie, row_text)
                            rows_meta: Dict[int, Tuple[Optional[str], Optional[str]]] = {}
                            for rr in tpl_rows:
                                if not isinstance(rr, dict):
                                    continue
                                try:
                                    rn = int(rr.get("cisloRiadku"))
                                except Exception:
                                    continue

                                ozn = _safe_str(rr.get("oznacenie"))
                                txt = rr.get("text")
                                row_text: Optional[str] = None
                                if isinstance(txt, dict):
                                    row_text = _safe_str(txt.get("sk") or txt.get("en"))
                                elif isinstance(txt, str):
                                    row_text = _safe_str(txt)

                                rows_meta[rn] = (ozn, row_text)

                            max_rows = (len(data) // data_cols) if data_cols > 0 else 0

                            for row_number in range(1, max_rows + 1):
                                oznacenie, row_text = rows_meta.get(row_number, (None, None))

                                for period_col in range(1, data_cols + 1):
                                    idx = _index_for(row_number, data_cols, period_col)
                                    if idx >= len(data):
                                        continue

                                    val_num = _parse_decimal(data[idx])

                                    cur.execute(
                                        SQL_UPSERT_ITEM,
                                        {
                                            "report_id": rid,
                                            "template_id": tid,
                                            "ico": ico,
                                            "pravna_forma": pravna_forma,
                                            "obdobie_do": obdobie_do,
                                            "table_idx": table_idx,
                                            "table_name": tname,
                                            "row_number": row_number,
                                            "oznacenie": oznacenie,
                                            "row_text": row_text,
                                            "period_col": period_col,
                                            "value_num": val_num,
                                        },
                                    )
                                    total_items += 1

                        total_reports += 1
                        if hard_limit is not None and total_reports >= hard_limit:
                            break

            elapsed = time.time() - t0
            rate_r = total_reports / elapsed if elapsed > 0 else 0.0
            log.info(
                "Reports=%d/%d | items=%d | last_id=%d | batch=%d | speed=%.2f reports/s",
                total_reports,
                total_candidates if hard_limit is None else min(total_candidates, hard_limit),
                total_items,
                last_id,
                len(batch),
                rate_r,
            )

        log.info(
            "Done. Reports extracted=%d | items=%d | last_id=%d | time=%.1fs",
            total_reports,
            total_items,
            last_id,
            time.time() - t0,
        )
