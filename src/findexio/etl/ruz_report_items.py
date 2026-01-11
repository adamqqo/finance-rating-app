from __future__ import annotations

import json
import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple, Union

from psycopg.rows import dict_row

from ..db import ensure_schema, get_conn

log = logging.getLogger("findexio.ruz_report_items")

LEGAL_FORMS_DEFAULT: Tuple[str, ...] = ("112", "121")

SQL_GET_STATE = "SELECT last_report_id FROM core.ruz_report_items_state WHERE id = 1;"
SQL_SET_STATE = "UPDATE core.ruz_report_items_state SET last_report_id = %s, updated_at = now() WHERE id = 1;"

SQL_COUNT_CANDIDATES = """
SELECT COUNT(*) AS c
FROM core.ruz_reports r
JOIN core.rpo_all_orgs o ON o.ico = r.ico
LEFT JOIN core.ruz_report_items i ON i.report_id = r.id
WHERE r.id_sablony IS NOT NULL
  AND o.legal_form_code = ANY(%s)
  AND r.tabulky IS NOT NULL
  AND i.report_id IS NULL;
"""

SQL_FETCH_CANDIDATES = """
SELECT r.id AS report_id,
       r.id_sablony AS template_id,
       r.ico,
       o.legal_form_code,
       r.titulna,
       r.tabulky
FROM core.ruz_reports r
JOIN core.rpo_all_orgs o ON o.ico = r.ico
LEFT JOIN core.ruz_report_items i ON i.report_id = r.id
WHERE r.id_sablony IS NOT NULL
  AND o.legal_form_code = ANY(%s)
  AND r.tabulky IS NOT NULL
  AND i.report_id IS NULL
  AND r.id > %s
ORDER BY r.id
LIMIT %s;
"""

SQL_COUNT_CANDIDATES_TPL = """
SELECT COUNT(*) AS c
FROM core.ruz_reports r
JOIN core.rpo_all_orgs o ON o.ico = r.ico
LEFT JOIN core.ruz_report_items i ON i.report_id = r.id
WHERE r.id_sablony IS NOT NULL
  AND o.legal_form_code = ANY(%s)
  AND r.tabulky IS NOT NULL
  AND i.report_id IS NULL
  AND r.id_sablony = ANY(%s);
"""

SQL_FETCH_CANDIDATES_TPL = """
SELECT r.id AS report_id,
       r.id_sablony AS template_id,
       r.ico,
       o.legal_form_code,
       r.titulna,
       r.tabulky
FROM core.ruz_reports r
JOIN core.rpo_all_orgs o ON o.ico = r.ico
LEFT JOIN core.ruz_report_items i ON i.report_id = r.id
WHERE r.id_sablony IS NOT NULL
  AND o.legal_form_code = ANY(%s)
  AND r.tabulky IS NOT NULL
  AND i.report_id IS NULL
  AND r.id_sablony = ANY(%s)
  AND r.id > %s
ORDER BY r.id
LIMIT %s;
"""

SQL_FETCH_TEMPLATE_RAW = """
SELECT raw AS tpl_raw
FROM core.ruz_templates
WHERE id = %s;
"""

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
    if x is None:
        return None
    s = str(x).strip()
    if s == "":
        return None

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
    return (row_number - 1) * data_cols + (period_col - 1)


def _load_template_payload(tpl_raw: Any, *, template_id: int, report_id: int) -> Optional[Dict[str, Any]]:
    if tpl_raw is None:
        return None

    if isinstance(tpl_raw, dict):
        return tpl_raw

    if isinstance(tpl_raw, str):
        s = tpl_raw.strip()
        if not s:
            return None
        try:
            obj = json.loads(s)
        except Exception:
            log.warning("Template raw is not valid JSON (template_id=%s, report_id=%s)", template_id, report_id)
            return None
        return obj if isinstance(obj, dict) else None

    try:
        obj = json.loads(tpl_raw)  # type: ignore[arg-type]
        return obj if isinstance(obj, dict) else None
    except Exception:
        log.warning(
            "Template raw has unsupported type=%s (template_id=%s, report_id=%s)",
            type(tpl_raw).__name__,
            template_id,
            report_id,
        )
        return None


TemplateIdsType = Optional[Union[int, Tuple[int, ...], List[int]]]


def _normalize_template_ids(template_ids: TemplateIdsType) -> Optional[List[int]]:
    if template_ids is None:
        return None
    if isinstance(template_ids, int):
        return [int(template_ids)]
    if isinstance(template_ids, tuple) or isinstance(template_ids, list):
        out: List[int] = []
        for x in template_ids:
            try:
                out.append(int(x))
            except Exception:
                continue
        return out if out else None
    # fallback: accept string like "699,687"
    if isinstance(template_ids, str):
        parts = [p.strip() for p in template_ids.split(",")]
        out2: List[int] = []
        for p in parts:
            if not p:
                continue
            try:
                out2.append(int(p))
            except Exception:
                continue
        return out2 if out2 else None
    return None


def run_sync(
    *,
    batch_size: int = 1000,
    hard_limit: Optional[int] = None,
    legal_forms: Tuple[str, ...] = LEGAL_FORMS_DEFAULT,
    template_ids: TemplateIdsType = None,
    use_state_cursor: bool = True,
) -> None:
    t0 = time.time()
    total_reports = 0
    total_items = 0

    tpl_ids = _normalize_template_ids(template_ids)

    with get_conn(row_factory=dict_row) as conn:
        ensure_schema(conn)

        last_id = 0
        if use_state_cursor:
            st = conn.execute(SQL_GET_STATE).fetchone() or {"last_report_id": 0}
            try:
                last_id = int(st.get("last_report_id") or 0)
            except Exception:
                last_id = 0

        if tpl_ids:
            total_candidates = int(
                (conn.execute(SQL_COUNT_CANDIDATES_TPL, (list(legal_forms), tpl_ids)).fetchone() or {"c": 0})["c"]
            )
        else:
            total_candidates = int((conn.execute(SQL_COUNT_CANDIDATES, (list(legal_forms),)).fetchone() or {"c": 0})["c"])

        log.info(
            "Candidate reports=%d | legal_forms=%s | template_ids=%s | start_last_id=%d",
            total_candidates,
            ",".join(legal_forms),
            ",".join(map(str, tpl_ids)) if tpl_ids else "ALL",
            last_id,
        )

        while True:
            if hard_limit is not None and total_reports >= hard_limit:
                break

            if tpl_ids:
                batch = conn.execute(SQL_FETCH_CANDIDATES_TPL, (list(legal_forms), tpl_ids, last_id, batch_size)).fetchall()
            else:
                batch = conn.execute(SQL_FETCH_CANDIDATES, (list(legal_forms), last_id, batch_size)).fetchall()

            if not batch:
                break

            last_id = int(batch[-1]["report_id"])

            with conn.transaction():
                with conn.cursor() as cur:
                    for r in batch:
                        rid = int(r["report_id"])
                        tid = int(r["template_id"])

                        tpl_row = cur.execute(SQL_FETCH_TEMPLATE_RAW, (tid,)).fetchone()
                        tpl_raw = tpl_row.get("tpl_raw") if tpl_row else None

                        tpl = _load_template_payload(tpl_raw, template_id=tid, report_id=rid)
                        if not tpl:
                            total_reports += 1
                            if hard_limit is not None and total_reports >= hard_limit:
                                break
                            continue

                        titulna = r.get("titulna") or {}
                        tabulky = r.get("tabulky") or []

                        pravna_forma = _safe_str(r.get("legal_form_code"))
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
                                data_cols = 2

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
                            if max_rows <= 0:
                                continue

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

                if use_state_cursor:
                    conn.execute(SQL_SET_STATE, (last_id,))

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
