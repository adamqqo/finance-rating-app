from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, Optional, Tuple

from psycopg.rows import dict_row
from psycopg.types.json import Json

from ..config import RUZ_API_BASE
from ..db import ensure_schema, get_conn
from ..http import DEFAULT_TIMEOUT, build_session

API_BASE = RUZ_API_BASE.rstrip("/")
DETAIL_PATH = "/api/uctovna-zavierka"

SESSION = build_session(user_agent="Findexio/0.1 (ruz-statements)")
REQUEST_TIMEOUT = (DEFAULT_TIMEOUT[0], 60)

log = logging.getLogger("findexio.ruz_statements")

LEGAL_FORMS_DEFAULT: Tuple[str, ...] = ("112", "121")

SQL_FETCH_BATCH_ONLY_MISSING = """
SELECT z.zavierka_id
FROM core.ruz_unit_zavierky z
JOIN core.ruz_units u ON u.id = z.unit_id
JOIN core.rpo_all_orgs o ON o.ico = u.ico
LEFT JOIN core.ruz_statements s ON s.id = z.zavierka_id
WHERE s.id IS NULL
  AND o.legal_form_code = ANY(%s)
ORDER BY z.zavierka_id
LIMIT %s OFFSET %s;
"""

SQL_FETCH_BATCH_ALL = """
SELECT z.zavierka_id
FROM core.ruz_unit_zavierky z
JOIN core.ruz_units u ON u.id = z.unit_id
JOIN core.rpo_all_orgs o ON o.ico = u.ico
WHERE o.legal_form_code = ANY(%s)
ORDER BY z.zavierka_id
LIMIT %s OFFSET %s;
"""

SQL_COUNT_ONLY_MISSING = """
SELECT COUNT(*) AS c
FROM core.ruz_unit_zavierky z
JOIN core.ruz_units u ON u.id = z.unit_id
JOIN core.rpo_all_orgs o ON o.ico = u.ico
LEFT JOIN core.ruz_statements s ON s.id = z.zavierka_id
WHERE s.id IS NULL
  AND o.legal_form_code = ANY(%s);
"""

SQL_COUNT_ALL = """
SELECT COUNT(*) AS c
FROM core.ruz_unit_zavierky z
JOIN core.ruz_units u ON u.id = z.unit_id
JOIN core.rpo_all_orgs o ON o.ico = u.ico
WHERE o.legal_form_code = ANY(%s);
"""

SQL_UPSERT = """
INSERT INTO core.ruz_statements (
    id, id_uctovnej_jednotky, obdobie_od, obdobie_do,
    druh_zavierky, typ_zavierky, pristupnost_dat,
    datum_poslednej_upravy, id_uctovnych_vykazov, updated_at
)
VALUES (
    %(id)s, %(id_uctovnej_jednotky)s, %(obdobie_od)s, %(obdobie_do)s,
    %(druh_zavierky)s, %(typ_zavierky)s, %(pristupnost_dat)s,
    %(datum_poslednej_upravy)s, %(id_uctovnych_vykazov)s, now()
)
ON CONFLICT (id) DO UPDATE SET
    id_uctovnej_jednotky = EXCLUDED.id_uctovnej_jednotky,
    obdobie_od = EXCLUDED.obdobie_od,
    obdobie_do = EXCLUDED.obdobie_do,
    druh_zavierky = EXCLUDED.druh_zavierky,
    typ_zavierky = EXCLUDED.typ_zavierky,
    pristupnost_dat = EXCLUDED.pristupnost_dat,
    datum_poslednej_upravy = EXCLUDED.datum_poslednej_upravy,
    id_uctovnych_vykazov = EXCLUDED.id_uctovnych_vykazov,
    updated_at = now();
"""

_date_y = re.compile(r"^\d{4}$")
_date_ym = re.compile(r"^\d{4}-\d{2}$")
_date_ymd = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def normalize_date(x: Any) -> Optional[str]:
    if not x:
        return None
    s = str(x).strip().split("T", 1)[0]
    if _date_ymd.match(s):
        return s
    if _date_ym.match(s):
        return s + "-01"
    if _date_y.match(s):
        return s + "-01-01"
    return None


def _url(p: str) -> str:
    return f"{API_BASE}{p}"


def _is_tombstone(detail: Dict[str, Any]) -> bool:
    return str(detail.get("stav", "")).strip().lower().startswith("zmazan")


def fetch_detail(statement_id: int) -> Dict[str, Any]:
    r = SESSION.get(_url(DETAIL_PATH), params={"id": statement_id}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def prepare_upsert_params(detail: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": detail.get("id"),
        "id_uctovnej_jednotky": detail.get("idUctovnejJednotky"),
        "obdobie_od": normalize_date(detail.get("obdobieOd")),
        "obdobie_do": normalize_date(detail.get("obdobieDo")),
        "druh_zavierky": detail.get("druhZavierky"),
        "typ_zavierky": detail.get("typZavierky"),
        "pristupnost_dat": detail.get("pristupnostDat"),
        "datum_poslednej_upravy": normalize_date(detail.get("datumPoslednejUpravy")),
        "id_uctovnych_vykazov": Json(detail.get("idUctovnychVykazov") or []),
        "raw": Json(detail),
    }


def run_sync(
    *,
    batch_size: int = 1000,
    refresh_all: bool = False,
    hard_limit: Optional[int] = None,
    legal_forms: Tuple[str, ...] = LEGAL_FORMS_DEFAULT,
) -> None:
    t0 = time.time()
    total, skipped_tombstone = 0, 0
    offset = 0

    with get_conn(row_factory=dict_row) as conn:
        ensure_schema(conn)

        total_candidates = int(
            (
                conn.execute(
                    SQL_COUNT_ALL if refresh_all else SQL_COUNT_ONLY_MISSING,
                    (list(legal_forms),),
                ).fetchone()
                or {"c": 0}
            )["c"]
        )
        log.info("Candidates=%d (refresh_all=%s)", total_candidates, refresh_all)

        while True:
            rows = conn.execute(
                SQL_FETCH_BATCH_ALL if refresh_all else SQL_FETCH_BATCH_ONLY_MISSING,
                (list(legal_forms), batch_size, offset),
            ).fetchall()

            if not rows:
                break

            ids = [int(r["zavierka_id"]) for r in rows]

            with conn.transaction():
                with conn.cursor() as cur:
                    for sid in ids:
                        try:
                            d = fetch_detail(sid)
                            if _is_tombstone(d):
                                skipped_tombstone += 1
                                continue
                            cur.execute(SQL_UPSERT, prepare_upsert_params(d))
                            total += 1
                        except Exception as e:
                            log.warning("Statement detail failed (id=%s): %s", sid, e)

                        if hard_limit is not None and total >= hard_limit:
                            break

            offset += len(ids)

            elapsed = time.time() - t0
            rate = total / elapsed if elapsed > 0 else 0.0
            log.info(
                "Processed=%d/%d | offset=%d | batch=%d | speed=%.2f/s | tombstones=%d",
                total,
                total_candidates if hard_limit is None else min(total_candidates, hard_limit),
                offset,
                len(ids),
                rate,
                skipped_tombstone,
            )

            if hard_limit is not None and total >= hard_limit:
                break

        log.info("Done. Stored statements=%d | tombstones=%d | time=%.1fs", total, skipped_tombstone, time.time() - t0)
