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


def run_sync(*, batch_size: int = 1000, refresh_all: bool = False, hard_limit: Optional[int] = None) -> None:
    t0 = time.time()
    total, skipped_tombstone, skipped_empty = 0, 0, 0
    offset = 0

    with get_conn(row_factory=dict_row) as conn:
        ensure_schema(conn)

        total_candidates = int((conn.execute(SQL_COUNT_ALL if refresh_all else SQL_COUNT_MISSING).fetchone() or {"c": 0})["c"])
        log.info("Candidates=%d (refresh_all=%s)", total_candidates, refresh_all)

        while True:
            rows = conn.execute(SQL_FETCH_BATCH_ALL if refresh_all else SQL_FETCH_BATCH_MISSING, (batch_size, offset)).fetchall()
            if not rows:
                break

            ids = [int(r["report_id"]) for r in rows]

            with conn.transaction():
                for rid in ids:
                    try:
                        d = fetch_detail(rid)
                    except Exception as e:
                        log.warning("HTTP detail failed (id=%s): %s", rid, e)
                        continue

                    if _is_tombstone(d):
                        skipped_tombstone += 1
                        continue

                    try:
                        with conn.transaction():
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

            offset += len(ids)
            elapsed = time.time() - t0
            rate = total / elapsed if elapsed > 0 else 0.0
            log.info("Processed=%d/%d | offset=%d | batch=%d | speed=%.2f/s | tombstones=%d | empty=%d",
                     total,
                     total_candidates if hard_limit is None else min(total_candidates, hard_limit),
                     offset, len(ids), rate, skipped_tombstone, skipped_empty)

            if hard_limit is not None and total >= hard_limit:
                break

        log.info("Done. Stored reports=%d | tombstones=%d | empty=%d | time=%.1fs",
                 total, skipped_tombstone, skipped_empty, time.time() - t0)
