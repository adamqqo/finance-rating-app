from __future__ import annotations

import json
import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

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

# NEW: explicit report_ids backfill mode (does not use state cursor)
SQL_COUNT_CANDIDATES_BY_IDS = """
SELECT COUNT(*) AS c
FROM core.ruz_reports r
JOIN core.rpo_all_orgs o ON o.ico = r.ico
LEFT JOIN core.ruz_report_items i ON i.report_id = r.id
WHERE r.id = ANY(%s)
  AND o.legal_form_code = ANY(%s)
  AND r.tabulky IS NOT NULL
  AND i.report_id IS NULL;
"""

SQL_FETCH_CANDIDATES_BY_IDS = """
SELECT r.id AS report_id,
       r.id_sablony AS template_id,
       r.ico,
       o.legal_form_code,
       r.titulna,
       r.tabulky
FROM core.ruz_reports r
JOIN core.rpo_all_orgs o ON o.ico = r.ico
LEFT JOIN core.ruz_report_items i ON i.report_id = r.id
WHERE r.id = ANY(%s)
  AND o.legal_form_code = ANY(%s)
  AND r.tabulky IS NOT NULL
  AND i.report_id IS NULL
ORDER BY r.id
LIMIT %s;
"""

SQL_FETCH_TEMPLATE_RAW = """
SELECT raw AS tpl_raw
FROM core.ruz_templates
WHERE id = %s;
"""

# Removed row_text from writes (do not store it; can be derived from template rows).
# Keep oznacenie as optional lightweight code; if you also want to drop it, remove similarly.
SQL_UPSERT_ITEM = """
INSERT INTO core.ruz_report_items (
  report_id, template_id, ico, pravna_forma, obdobie_do,
  table_idx, table_name, row_number, oznacenie,
  period_col, value_num, updated_at
) VALUES (
  %(report_id)s, %(template_id)s, %(ico)s, %(pravna_forma)s, %(obdobie_do)s,
  %(table_idx)s, %(table_name)s, %(row_number)s, %(oznacenie)s,
  %(period_col)s, %(value_num)s, now()
)
ON CONFLICT (report_id, table_idx, row_number, period_col) DO UPDATE SET
  template_id   = EXCLUDED.template_id,
  ico           = EXCLUDED.ico,
  pravna_forma  = EXCLUDED.pravna_forma,
  obdobie_do    = EXCLUDED.obdobie_do,
  table_name    = EXCLUDED.table_name,
  oznacenie     = EXCLUDED.oznacenie,
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


def _index_for(local_row_number: int, data_cols: int, period_col: int) -> int:
    return (local_row_number - 1) * data_cols + (period_col - 1)


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
ReportIdsType = Optional[Iterable[int]]


def _normalize_template_ids(template_ids: TemplateIdsType) -> Optional[List[int]]:
    if template_ids is None:
        return None
    if isinstance(template_ids, int):
        return [int(template_ids)]
    if isinstance(template_ids, (tuple, list)):
        out: List[int] = []
        for x in template_ids:
            try:
                out.append(int(x))
            except Exception:
                continue
        return out if out else None
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


def _build_rows_meta_with_offset(
    tpl_rows: List[Dict[str, Any]],
    max_rows_local: int,
) -> Tuple[Dict[int, Optional[str]], int, int]:
    """
    Returns:
      - rows_meta_ozn: {global_row_number -> oznacenie}
      - offset: global = local + offset
      - max_rows_effective: capped local row count so we don't go past template max row number :)
    """
    rows_meta_ozn: Dict[int, Optional[str]] = {}
    min_tpl_rn: Optional[int] = None
    max_tpl_rn: Optional[int] = None

    for rr in tpl_rows:
        if not isinstance(rr, dict):
            continue
        try:
            rn = int(rr.get("cisloRiadku"))
        except Exception:
            continue

        if min_tpl_rn is None or rn < min_tpl_rn:
            min_tpl_rn = rn
        if max_tpl_rn is None or rn > max_tpl_rn:
            max_tpl_rn = rn

        rows_meta_ozn[rn] = _safe_str(rr.get("oznacenie"))

    # Detect tables where template row numbers are globally numbered (e.g., 79..145)
    offset = 0
    if min_tpl_rn is not None and min_tpl_rn > 1 and (1 not in rows_meta_ozn):
        offset = min_tpl_rn - 1

    max_rows_effective = max_rows_local
    if max_tpl_rn is not None:
        max_local_allowed = max_tpl_rn - offset
        if max_local_allowed > 0:
            max_rows_effective = min(max_rows_effective, max_local_allowed)

    return rows_meta_ozn, offset, max_rows_effective


def run_sync(
    *,
    batch_size: int = 100,
    hard_limit: Optional[int] = None,
    legal_forms: Tuple[str, ...] = LEGAL_FORMS_DEFAULT,
    template_ids: TemplateIdsType = None,
    use_state_cursor: bool = True,
    report_ids: ReportIdsType = None,
    update_state: bool = True,
) -> None:
    t0 = time.time()
    total_reports = 0
    total_items = 0

    tpl_ids = _normalize_template_ids(template_ids)

    # normalize report_ids to list[int] if provided
    report_ids_list: Optional[List[int]] = None
    if report_ids is not None:
        report_ids_list = [int(x) for x in report_ids if x is not None]
        if not report_ids_list:
            report_ids_list = []

    with get_conn(row_factory=dict_row) as conn:
        ensure_schema(conn)

        # state cursor only applies to NORMAL mode (no explicit report_ids)
        last_id = 0
        if use_state_cursor and report_ids_list is None:
            st = conn.execute(SQL_GET_STATE).fetchone() or {"last_report_id": 0}
            try:
                last_id = int(st.get("last_report_id") or 0)
            except Exception:
                last_id = 0

        # candidate count: depends on mode
        if report_ids_list is not None:
            total_candidates = int(
                (conn.execute(SQL_COUNT_CANDIDATES_BY_IDS, (report_ids_list, list(legal_forms))).fetchone() or {"c": 0})["c"]
            )
        else:
            if tpl_ids:
                total_candidates = int(
                    (conn.execute(SQL_COUNT_CANDIDATES_TPL, (list(legal_forms), tpl_ids)).fetchone() or {"c": 0})["c"]
                )
            else:
                total_candidates = int(
                    (conn.execute(SQL_COUNT_CANDIDATES, (list(legal_forms),)).fetchone() or {"c": 0})["c"]
                )

        log.info(
            "Candidate reports=%d | legal_forms=%s | template_ids=%s | mode=%s | start_last_id=%d",
            total_candidates,
            ",".join(legal_forms),
            ",".join(map(str, tpl_ids)) if tpl_ids else "ALL",
            "report_ids" if report_ids_list is not None else "cursor",
            last_id,
        )

        while True:
            if hard_limit is not None and total_reports >= hard_limit:
                break

            # BACKFILL MODE: explicit report_ids (does not use last_id cursor)
            if report_ids_list is not None:
                batch = conn.execute(
                    SQL_FETCH_CANDIDATES_BY_IDS,
                    (report_ids_list, list(legal_forms), batch_size),
                ).fetchall()

            # NORMAL MODE: cursor-based
            else:
                if tpl_ids:
                    batch = conn.execute(
                        SQL_FETCH_CANDIDATES_TPL,
                        (list(legal_forms), tpl_ids, last_id, batch_size),
                    ).fetchall()
                else:
                    batch = conn.execute(
                        SQL_FETCH_CANDIDATES,
                        (list(legal_forms), last_id, batch_size),
                    ).fetchall()

            if not batch:
                break

            # for logging / progress only; state update is conditional below
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

                            max_rows_local = (len(data) // data_cols) if data_cols > 0 else 0
                            if max_rows_local <= 0:
                                continue

                            rows_meta_ozn, offset, max_rows_effective = _build_rows_meta_with_offset(tpl_rows, max_rows_local)
                            if max_rows_effective <= 0:
                                continue

                            for local_row_number in range(1, max_rows_effective + 1):
                                row_number = local_row_number + offset  # global/template row number
                                oznacenie = rows_meta_ozn.get(row_number)

                                for period_col in range(1, data_cols + 1):
                                    idx = _index_for(local_row_number, data_cols, period_col)
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
                                            "period_col": period_col,
                                            "value_num": val_num,
                                        },
                                    )
                                    total_items += 1

                        total_reports += 1
                        if hard_limit is not None and total_reports >= hard_limit:
                            break

                # Update cursor state ONLY in normal mode (no report_ids) and only if enabled
                if update_state and use_state_cursor and report_ids_list is None:
                    conn.execute(SQL_SET_STATE, (last_id,))

            elapsed = time.time() - t0
            rate_r = total_reports / elapsed if elapsed > 0 else 0.0
            log.info(
                "Reports=%d/%d | items=%d | last_id=%d | batch=%d | speed=%.2f reports/s | mode=%s",
                total_reports,
                total_candidates if hard_limit is None else min(total_candidates, hard_limit),
                total_items,
                last_id,
                len(batch),
                rate_r,
                "report_ids" if report_ids_list is not None else "cursor",
            )

        log.info(
            "Done. Reports extracted=%d | items=%d | last_id=%d | time=%.1fs | mode=%s",
            total_reports,
            total_items,
            last_id,
            time.time() - t0,
            "report_ids" if report_ids_list is not None else "cursor",
        )
