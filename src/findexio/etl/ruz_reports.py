from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, Iterator, Optional

from psycopg.rows import dict_row
from psycopg.types.json import Json

from ..config import RUZ_API_BASE
from ..db import get_conn, ensure_schema
from ..http import build_session, DEFAULT_TIMEOUT

SESSION = build_session(user_agent="Findexio/0.1 (ruz-reports)")
API_BASE = RUZ_API_BASE.rstrip("/")
DETAIL_PATH = "/api/uctovny-vykaz"
REQUEST_TIMEOUT = DEFAULT_TIMEOUT

log = logging.getLogger("findexio.ruz_reports")

SQL_GET_STATE = "SELECT last_report_id FROM core.ruz_reports_sync_state WHERE id = 1;"
SQL_SET_STATE = "UPDATE core.ruz_reports_sync_state SET last_report_id = %s, updated_at = now() WHERE id = 1;"

SQL_FETCH_BATCH_ALL_CURSOR = """
SELECT DISTINCT t.x::bigint AS report_id
FROM core.ruz_statements s
CROSS JOIN LATERAL jsonb_array_elements_text(s.id_uctovnych_vykazov) AS t(x)
WHERE t.x::bigint > %s
ORDER BY t.x::bigint
LIMIT %s;
"""

SQL_FETCH_BATCH_MISSING_CURSOR = """
SELECT DISTINCT t.x::bigint AS report_id
FROM core.ruz_statements s
CROSS JOIN LATERAL jsonb_array_elements_text(s.id_uctovnych_vykazov) AS t(x)
LEFT JOIN core.ruz_reports r ON r.id = t.x::bigint
WHERE t.x::bigint > %s
  AND r.id IS NULL
ORDER BY t.x::bigint
LIMIT %s;
"""

SQL_GET_TEMPLATE_MAP = """
SELECT id_sablony, tombstone
FROM core.ruz_report_template_map
WHERE report_id = %s;
"""

SQL_UPSERT_TEMPLATE_MAP = """
INSERT INTO core.ruz_report_template_map (report_id, id_sablony, tombstone, fetched_at)
VALUES (%s, %s, %s, now())
ON CONFLICT (report_id) DO UPDATE SET
  id_sablony = EXCLUDED.id_sablony,
  tombstone = EXCLUDED.tombstone,
  fetched_at = now();
"""

SQL_COUNT_MISSING = """
SELECT COUNT(*) AS c
FROM core.ruz_statements s
CROSS JOIN LATERAL jsonb_array_elements_text(s.id_uctovnych_vykazov) AS t(x)
LEFT JOIN core.ruz_reports r ON r.id = t.x::bigint
WHERE r.id IS NULL;
"""

SQL_FETCH_BATCH_MISSING = """
SELECT t.x::bigint AS report_id
FROM core.ruz_statements s
CROSS JOIN LATERAL jsonb_array_elements_text(s.id_uctovnych_vykazov) AS t(x)
LEFT JOIN core.ruz_reports r ON r.id = t.x::bigint
WHERE r.id IS NULL
ORDER BY t.x::bigint
LIMIT %s OFFSET %s;
"""

SQL_COUNT_ALL = """
SELECT COUNT(*) AS c
FROM core.ruz_statements s
CROSS JOIN LATERAL jsonb_array_elements_text(s.id_uctovnych_vykazov) AS t(x);
"""

SQL_FETCH_BATCH_ALL = """
SELECT t.x::bigint AS report_id
FROM core.ruz_statements s
CROSS JOIN LATERAL jsonb_array_elements_text(s.id_uctovnych_vykazov) AS t(x)
ORDER BY t.x::bigint
LIMIT %s OFFSET %s;
"""

SQL_UPSERT_REPORT = """
INSERT INTO core.ruz_reports (
    id, ico, id_uctovnej_zavierky, id_vyrocnej_spravy, id_sablony, mena,
    pristupnost, pocet_stran, jazyk, zdroj_dat, datum_poslednej_upravy,
    titulna, tabulky, prilohy, updated_at
) VALUES (
    %(id)s, %(ico)s, %(idUctovnejZavierky)s, %(idVyrocnejSpravy)s, %(idSablony)s, %(mena)s,
    %(pristupnostDat)s, %(pocetStran)s, %(jazyk)s, %(zdrojDat)s, %(datumPoslednejUpravy)s,
    %(titulnaStrana)s, %(tabulky)s, %(prilohy)s, now()
)
ON CONFLICT (id) DO UPDATE SET
    ico = EXCLUDED.ico,
    id_uctovnej_zavierky = EXCLUDED.id_uctovnej_zavierky,
    id_vyrocnej_spravy = EXCLUDED.id_vyrocnej_spravy,
    id_sablony = EXCLUDED.id_sablony,
    mena = EXCLUDED.mena,
    pristupnost = EXCLUDED.pristupnost,
    pocet_stran = EXCLUDED.pocet_stran,
    jazyk = EXCLUDED.jazyk,
    zdroj_dat = EXCLUDED.zdroj_dat,
    datum_poslednej_upravy = EXCLUDED.datum_poslednej_upravy,
    titulna = EXCLUDED.titulna,
    tabulky = EXCLUDED.tabulky,
    prilohy = EXCLUDED.prilohy,
    updated_at = now();
"""

SQL_DELETE_ROWS_FOR_REPORT = "DELETE FROM core.ruz_report_rows WHERE report_id = %s;"

SQL_INSERT_ROW = """
INSERT INTO core.ruz_report_rows (report_id, table_name, row_idx, row_key, row_name, cells, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, now());
"""

_re_y = re.compile(r"^\d{4}$")
_re_ym = re.compile(r"^\d{4}-\d{2}$")
_re_ymd = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _url(p: str) -> str:
    return f"{API_BASE}{p}"


def normalize_date(x: Any) -> Optional[str]:
    if not x:
        return None
    s = str(x).strip().split("T", 1)[0]
    if _re_ymd.match(s):
        return s
    if _re_ym.match(s):
        return s + "-01"
    if _re_y.match(s):
        return s + "-01-01"
    return None


def _is_tombstone(detail: Dict[str, Any]) -> bool:
    return str(detail.get("stav", "")).strip().lower().startswith("zmazan")


def fetch_detail(report_id: int) -> Dict[str, Any]:
    r = SESSION.get(_url(DETAIL_PATH), params={"id": report_id}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def extract_ico(detail: Dict[str, Any]) -> Optional[str]:
    t = (detail.get("obsah") or {}).get("titulnaStrana") or {}
    ico = t.get("ico")
    if not ico:
        return None
    s = str(ico).strip().replace(" ", "")
    return s if (s.isdigit() and len(s) == 8) else None


def prep_upsert(detail: Dict[str, Any]) -> Dict[str, Any]:
    obsah = detail.get("obsah") or {}
    return {
        "id": detail.get("id"),
        "idUctovnejZavierky": detail.get("idUctovnejZavierky"),
        "idVyrocnejSpravy": detail.get("idVyrocnejSpravy"),
        "idSablony": detail.get("idSablony"),
        "mena": detail.get("mena"),
        "pristupnostDat": detail.get("pristupnostDat"),
        "pocetStran": (int(detail.get("pocetStran")) if str(detail.get("pocetStran") or "").isdigit() else None),
        "jazyk": detail.get("jazyk"),
        "zdrojDat": obsah.get("zdrojDat"),
        "datumPoslednejUpravy": normalize_date(detail.get("datumPoslednejUpravy")),
        "titulnaStrana": Json(obsah.get("titulnaStrana")),
        "tabulky": Json(obsah.get("tabulky")),
        "prilohy": Json(detail.get("prilohy")),
        "raw": Json(detail),
        "ico": extract_ico(detail),
    }


def iter_rows_from_obsah(detail: Dict[str, Any]) -> Iterator[tuple]:
    obsah = detail.get("obsah") or {}
    tables = obsah.get("tabulky") or []
    if not isinstance(tables, list):
        return
    for t in tables:
        table_name = t.get("nazov") or t.get("name") or t.get("kod") or None
        rows = t.get("riadky") or t.get("rows") or []
        if not isinstance(rows, list):
            continue
        for idx, r in enumerate(rows):
            row_key = r.get("kod") or r.get("kluc") or r.get("key") or None
            row_name = r.get("nazov") or r.get("name") or r.get("text") or None
            cells = r.get("bunky") or r.get("cells") or r.get("data") or r
            yield (table_name, idx, row_key, row_name, cells)


def run_sync(
    *,
    batch_size: int = 1000,
    refresh_all: bool = False,
    hard_limit: Optional[int] = None,
    template_id_only: Optional[int] = None,
    use_template_cache: bool = True,
    reset_cursor: bool = False,
) -> None:
    t0 = time.time()
    total, skipped_tombstone, skipped_empty, skipped_template = 0, 0, 0, 0
    http_ok, http_fail = 0, 0

    with get_conn(row_factory=dict_row) as conn:
        ensure_schema(conn)

        if reset_cursor:
            conn.execute(SQL_SET_STATE, (0,))
            conn.commit()

        last_id = int((conn.execute(SQL_GET_STATE).fetchone() or {"last_report_id": 0})["last_report_id"])
        log.info("Start cursor last_id=%d | refresh_all=%s | template_id_only=%s", last_id, refresh_all, template_id_only)

        while True:
            rows = conn.execute(
                SQL_FETCH_BATCH_ALL_CURSOR if refresh_all else SQL_FETCH_BATCH_MISSING_CURSOR,
                (last_id, batch_size),
            ).fetchall()
            if not rows:
                break

            ids = [int(r["report_id"]) for r in rows]
            batch_max = ids[-1]

            with conn.transaction():
                for rid in ids:
                    # 1) cache skip (ak je zapnuté)
                    if use_template_cache:
                        m = conn.execute(SQL_GET_TEMPLATE_MAP, (rid,)).fetchone()
                        if m:
                            if m["tombstone"]:
                                skipped_tombstone += 1
                                continue
                            if template_id_only is not None and m["id_sablony"] != template_id_only:
                                skipped_template += 1
                                continue

                    # 2) HTTP fetch
                    try:
                        d = fetch_detail(rid)
                        http_ok += 1
                    except Exception as e:
                        http_fail += 1
                        log.warning("HTTP detail failed (id=%s): %s", rid, e)
                        continue

                    is_tomb = _is_tombstone(d)
                    sabl = d.get("idSablony")

                    # 3) upsert mapu hneď po fetchnutí (aby sa nabudúce skipovalo bez HTTP)
                    if use_template_cache:
                        conn.execute(SQL_UPSERT_TEMPLATE_MAP, (rid, sabl, is_tomb))

                    if is_tomb:
                        skipped_tombstone += 1
                        continue

                    if template_id_only is not None and sabl != template_id_only:
                        skipped_template += 1
                        continue

                    # 4) DB write (len pre “správne” reporty)
                    try:
                        params = prep_upsert(d)
                        with conn.cursor() as curu:
                            curu.execute(SQL_UPSERT_REPORT, params)

                        tables = (d.get("obsah") or {}).get("tabulky")
                        if not isinstance(tables, list):
                            skipped_empty += 1
                            continue

                        with conn.cursor() as curd:
                            curd.execute(SQL_DELETE_ROWS_FOR_REPORT, (rid,))
                            for table_name, row_idx, row_key, row_name, cells in iter_rows_from_obsah(d):
                                curd.execute(SQL_INSERT_ROW, (rid, table_name, row_idx, row_key, row_name, Json(cells)))

                        total += 1
                    except Exception as e:
                        log.error("DB write failed (report_id=%s): %s", rid, e)

                    if hard_limit is not None and total >= hard_limit:
                        break

                # posuň cursor aj keď sa nič neuložilo (ináč sa zasekneš na wrong_template)
                conn.execute(SQL_SET_STATE, (batch_max,))
                last_id = batch_max

            elapsed = time.time() - t0
            store_rate = total / elapsed if elapsed > 0 else 0.0
            http_rate = http_ok / elapsed if elapsed > 0 else 0.0

            log.info(
                "Stored=%d | cursor=%d | batch=%d | store_speed=%.2f/s | http_ok=%d http_fail=%d http_speed=%.2f/s | tomb=%d | empty=%d | wrong_template=%d",
                total, last_id, len(ids), store_rate, http_ok, http_fail, http_rate,
                skipped_tombstone, skipped_empty, skipped_template
            )

            if hard_limit is not None and total >= hard_limit:
                break

        log.info(
            "Done. Stored=%d | http_ok=%d http_fail=%d | tomb=%d | empty=%d | wrong_template=%d | time=%.1fs | last_id=%d",
            total, http_ok, http_fail, skipped_tombstone, skipped_empty, skipped_template, time.time() - t0, last_id
        )


