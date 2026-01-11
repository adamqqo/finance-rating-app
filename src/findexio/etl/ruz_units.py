from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from psycopg.rows import dict_row
from psycopg.types.json import Json

from ..config import RUZ_API_BASE
from ..db import ensure_schema, get_conn
from ..http import DEFAULT_TIMEOUT, build_session

API_BASE = RUZ_API_BASE.rstrip("/")
LIST_UNITS = "/api/uctovne-jednotky"
DETAIL_UNIT = "/api/uctovna-jednotka"

SESSION = build_session(user_agent="Findexio/0.1 (ruz-units)")
REQUEST_TIMEOUT = (DEFAULT_TIMEOUT[0], 45)

log = logging.getLogger("findexio.ruz_units")

LEGAL_FORMS_DEFAULT: Tuple[str, ...] = ("112", "121")

SQL_GET_STATE = "SELECT zmenene_od, pokracovat_za_id FROM core.ruz_units_state WHERE id = 1;"

SQL_SET_STATE = """
UPDATE core.ruz_units_state
SET zmenene_od = %s, pokracovat_za_id = %s, last_run_at = now()
WHERE id = 1;
"""

SQL_UPSERT_UNIT = """
INSERT INTO core.ruz_units (id, ico, id_uctovnych_zavierok, updated_at)
VALUES (%(id)s, %(ico)s, %(id_uctovnych_zavierok)s, now())
ON CONFLICT (id) DO UPDATE
SET ico = EXCLUDED.ico,
    id_uctovnych_zavierok = EXCLUDED.id_uctovnych_zavierok,
    updated_at = now();
"""

SQL_DELETE_LINKS_FOR_UNIT = "DELETE FROM core.ruz_unit_zavierky WHERE unit_id = %s;"

SQL_INSERT_LINKS_BULK = """
INSERT INTO core.ruz_unit_zavierky (unit_id, zavierka_id)
SELECT %s AS unit_id, x::bigint AS zavierka_id
FROM jsonb_array_elements_text(%s::jsonb) AS t(x)
ON CONFLICT DO NOTHING;
"""

# Restrict-by-legal-form filter (optional)
SQL_ICO_ALLOWED = """
SELECT 1
FROM core.rpo_all_orgs
WHERE ico = %s
  AND legal_form_code = ANY(%s)
LIMIT 1;
"""


def _url(path: str) -> str:
    return f"{API_BASE}{path}"


def _norm_ico(ico: Any) -> Optional[str]:
    s = str(ico or "").strip().replace(" ", "")
    return s if (s.isdigit() and len(s) == 8) else None


def _parse_since(since: str) -> datetime:
    s = since.strip()
    if len(s) == 10:  # YYYY-MM-DD
        return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    # ISO-ish, berieme prvých 19 a Z ignorujeme (safe)
    s2 = s.replace("Z", "")
    try:
        dt = datetime.fromisoformat(s2)
    except Exception:
        return datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _dt_to_api(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def list_unit_ids(zmenene_od: str, pokracovat_za_id: Optional[int], max_zaznamov: int) -> Tuple[List[int], bool]:
    params = {"zmenene-od": zmenene_od, "max-zaznamov": max_zaznamov}
    if pokracovat_za_id is not None:
        params["pokracovat-za-id"] = str(pokracovat_za_id)

    r = SESSION.get(_url(LIST_UNITS), params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    data = r.json()

    ids: List[int] = []
    more = False

    if isinstance(data, dict) and "id" in data:
        ids = [int(x) for x in (data.get("id") or [])]
        more = bool(data.get("existujeDalsieId"))
    elif isinstance(data, list):
        for row in data:
            if isinstance(row, dict) and "id" in row:
                ids.append(int(row["id"]))
        more = len(data) >= max_zaznamov
    else:
        log.warning("Unexpected list format: %s", type(data))

    return ids, more


def fetch_unit_detail(uid: int) -> Dict[str, Any]:
    r = SESSION.get(_url(DETAIL_UNIT), params={"id": uid}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def extract_min_fields(detail: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(detail.get("id")),
        "ico": _norm_ico(detail.get("ico")),
        "id_uctovnych_zavierok": detail.get("idUctovnychZavierok") or [],
    }


def run_sync(
    *,
    since: Optional[str] = None,
    limit_ids: Optional[int] = None,
    page_size: int = 1000,
    restrict_to_legal_forms: bool = False,
    legal_forms: Tuple[str, ...] = LEGAL_FORMS_DEFAULT,
) -> None:
    """
    Sync RUZ units.

    Notes on restrict_to_legal_forms:
    - LIST endpoint doesn't provide ICO, so we still must call DETAIL for every unit id.
    - Restriction saves DB writes/storage (ruz_units + ruz_unit_zavierky), but not API calls.
    - Resume is safe: progress_id advances regardless of upsert/skip.
    """
    t0 = time.time()
    total_seen = 0
    total_upserted = 0
    total_skipped = 0

    with get_conn(row_factory=dict_row) as conn:
        ensure_schema(conn)

        st = conn.execute(SQL_GET_STATE).fetchone() or {"zmenene_od": None, "pokracovat_za_id": None}

        if since:
            window_start_dt = _parse_since(since)
            progress_id = None
        elif st["zmenene_od"]:
            window_start_dt = st["zmenene_od"].astimezone(timezone.utc)
            progress_id = st["pokracovat_za_id"]
        else:
            window_start_dt = datetime(2000, 1, 1, tzinfo=timezone.utc)
            progress_id = None

        window_start_str = _dt_to_api(window_start_dt)
        run_started_at = datetime.now(timezone.utc)

        log.info(
            "Listing units from %s (pokracovat-za-id=%s) | restrict=%s | legal_forms=%s",
            window_start_str,
            progress_id,
            restrict_to_legal_forms,
            ",".join(legal_forms),
        )

        while True:
            ids, more = list_unit_ids(window_start_str, progress_id, max_zaznamov=page_size)
            if not ids:
                conn.execute(SQL_SET_STATE, (run_started_at, None))
                conn.commit()
                break

            if limit_ids is not None:
                remain = max(0, limit_ids - total_seen)
                if remain <= 0:
                    break
                if len(ids) > remain:
                    ids = ids[:remain]
                    more = False

            with conn.transaction():
                with conn.cursor() as cur:
                    for uid in ids:
                        total_seen += 1
                        try:
                            d = fetch_unit_detail(uid)
                            row = extract_min_fields(d)

                            # Always advance cursor, even on skip, to avoid refetch loops.
                            progress_id = uid

                            if restrict_to_legal_forms:
                                if not row["ico"]:
                                    total_skipped += 1
                                    continue

                                ok = cur.execute(SQL_ICO_ALLOWED, (row["ico"], list(legal_forms))).fetchone()
                                if not ok:
                                    total_skipped += 1
                                    continue

                            cur.execute(
                                SQL_UPSERT_UNIT,
                                {
                                    "id": row["id"],
                                    "ico": row["ico"],
                                    "id_uctovnych_zavierok": Json(row["id_uctovnych_zavierok"]),
                                },
                            )

                            cur.execute(SQL_DELETE_LINKS_FOR_UNIT, (row["id"],))
                            if row["id_uctovnych_zavierok"]:
                                cur.execute(SQL_INSERT_LINKS_BULK, (row["id"], Json(row["id_uctovnych_zavierok"])))

                            total_upserted += 1

                        except Exception as e:
                            # still move progress_id to avoid being stuck on a problematic id
                            progress_id = uid
                            log.warning("Unit detail failed (id=%s): %s", uid, e)

                conn.execute(SQL_SET_STATE, (window_start_dt, progress_id))

            elapsed = time.time() - t0
            rate = total_seen / elapsed if elapsed > 0 else 0.0
            log.info(
                "Seen=%d | upserted=%d | skipped=%d | page=%d | speed=%.2f/s | last_id=%s",
                total_seen,
                total_upserted,
                total_skipped,
                len(ids),
                rate,
                progress_id,
            )

            if not more:
                conn.execute(SQL_SET_STATE, (run_started_at, None))
                conn.commit()
                break

        log.info(
            "Done. Units seen=%d | upserted=%d | skipped=%d | time=%.1fs",
            total_seen,
            total_upserted,
            total_skipped,
            time.time() - t0,
        )
