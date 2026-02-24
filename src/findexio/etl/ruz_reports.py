from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, Iterable, Optional

from psycopg.rows import dict_row
from psycopg.types.json import Json

from ..config import RUZ_API_BASE
from ..db import ensure_schema, get_conn
from ..http import DEFAULT_TIMEOUT, build_session

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

# statement_id -> unit.ico (canonical)
SQL_GET_UNIT_ICO_FOR_STATEMENT = """
SELECT u.ico
FROM core.ruz_unit_zavierky uz
JOIN core.ruz_units u ON u.id = uz.unit_id
WHERE uz.zavierka_id = %s
LIMIT 1;
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


def extract_ico(detail: Dict[str, Any]) -> Optional[str]:
    t = (detail.get("obsah") or {}).get("titulnaStrana") or {}
    ico = t.get("ico")
    if not ico:
        return None
    s = str(ico).strip().replace(" ", "")
    return s if (s.isdigit() and len(s) == 8) else None


def norm_ico8(x: Optional[str]) -> Optional[str]:
    if not x:
        return None
    s = re.sub(r"\D", "", str(x))
    if not s:
        return None
    return s.zfill(8)[:8]


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
        "ico": extract_ico(detail),
    }


def run_sync(
    *,
    batch_size: int = 1000,
    refresh_all: bool = False,
    hard_limit: Optional[int] = None,          # limit na uložené reporty (Stored)
    candidate_limit: Optional[int] = None,     # limit na spracované kandidáty (rid loop)
    http_limit: Optional[int] = None,          # limit na http_ok v tomto rune
    max_seconds: Optional[int] = None,         # runtime budget
    max_batches: Optional[int] = None,         # limit na počet batchov
    template_id_only: Optional[int | Iterable[int]] = None,
    use_template_cache: bool = True,
    reset_cursor: bool = False,
) -> None:
    t0 = time.time()
    total, skipped_tombstone, skipped_template = 0, 0, 0
    skipped_ico_mismatch = 0
    http_ok, http_fail = 0, 0
    candidates_seen = 0
    batches_done = 0

    # normalize template_id_only -> set[int] or None
    if template_id_only is None:
        template_filter: Optional[set[int]] = None
    elif isinstance(template_id_only, int):
        template_filter = {template_id_only}
    else:
        template_filter = {int(x) for x in template_id_only}

    def time_budget_exceeded() -> bool:
        return max_seconds is not None and (time.time() - t0) >= max_seconds

    with get_conn(row_factory=dict_row) as conn:
        ensure_schema(conn)

        if reset_cursor:
            conn.execute(SQL_SET_STATE, (0,))
            conn.commit()

        last_id = int((conn.execute(SQL_GET_STATE).fetchone() or {"last_report_id": 0})["last_report_id"])
        log.info(
            "Start cursor last_id=%d | refresh_all=%s | template_id_only=%s | batch_size=%d | hard_limit=%s | candidate_limit=%s | http_limit=%s | max_seconds=%s | max_batches=%s",
            last_id, refresh_all, template_filter, batch_size,
            str(hard_limit), str(candidate_limit), str(http_limit), str(max_seconds), str(max_batches),
        )

        while True:
            if time_budget_exceeded():
                log.info("Stop: max_seconds reached (%s)", max_seconds)
                break
            if max_batches is not None and batches_done >= max_batches:
                log.info("Stop: max_batches reached (%s)", max_batches)
                break
            if candidate_limit is not None and candidates_seen >= candidate_limit:
                log.info("Stop: candidate_limit reached (%s)", candidate_limit)
                break
            if http_limit is not None and http_ok >= http_limit:
                log.info("Stop: http_limit reached (%s)", http_limit)
                break
            if hard_limit is not None and total >= hard_limit:
                log.info("Stop: hard_limit reached (%s)", hard_limit)
                break

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
                    candidates_seen += 1

                    if candidate_limit is not None and candidates_seen > candidate_limit:
                        break
                    if http_limit is not None and http_ok >= http_limit:
                        break
                    if hard_limit is not None and total >= hard_limit:
                        break
                    if time_budget_exceeded():
                        break

                    # 1) cache skip
                    if use_template_cache:
                        m = conn.execute(SQL_GET_TEMPLATE_MAP, (rid,)).fetchone()
                        if m:
                            if m["tombstone"]:
                                skipped_tombstone += 1
                                continue
                            if template_filter is not None and m["id_sablony"] not in template_filter:
                                skipped_template += 1
                                continue

                    # 2) HTTP fetch
                    try:
                        with SESSION.get(_url(DETAIL_PATH), params={"id": rid}, timeout=REQUEST_TIMEOUT) as r:
                            r.raise_for_status()
                            d = r.json()
                        http_ok += 1
                    except Exception as e:
                        http_fail += 1
                        log.warning("HTTP detail failed (id=%s): %s", rid, e)
                        continue

                    is_tomb = _is_tombstone(d)
                    sabl = d.get("idSablony")

                    # 3) upsert template map
                    if use_template_cache:
                        conn.execute(SQL_UPSERT_TEMPLATE_MAP, (rid, sabl, is_tomb))

                    if is_tomb:
                        skipped_tombstone += 1
                        continue

                    if template_filter is not None and sabl not in template_filter:
                        skipped_template += 1
                        continue

                    # 3.5) ICO mismatch check: unit.ico vs titulnaStrana.ico -> IGNORE (do not store)
                    try:
                        statement_id = d.get("idUctovnejZavierky")
                        ico_doc = extract_ico(d)

                        ico_unit = None
                        if statement_id:
                            urow = conn.execute(SQL_GET_UNIT_ICO_FOR_STATEMENT, (statement_id,)).fetchone()
                            if urow:
                                ico_unit = urow["ico"]

                        ico_doc_n = norm_ico8(ico_doc)
                        ico_unit_n = norm_ico8(ico_unit)

                        if ico_doc_n and ico_unit_n and ico_doc_n != ico_unit_n:
                            skipped_ico_mismatch += 1
                            log.warning(
                                "Skip report due to ICO mismatch | report_id=%s statement_id=%s ico_unit=%s ico_doc=%s template_id=%s",
                                rid, statement_id, ico_unit_n, ico_doc_n, sabl
                            )
                            continue
                    except Exception as e:
                        # fail-open: if check fails, keep storing (avoid accidental data loss)
                        log.warning("ICO mismatch check failed (report_id=%s): %s", rid, e)

                    # 4) DB write (only ruz_reports)
                    try:
                        params = prep_upsert(d)
                        with conn.cursor() as cur:
                            cur.execute(SQL_UPSERT_REPORT, params)
                        total += 1
                    except Exception as e:
                        log.error("DB write failed (report_id=%s): %s", rid, e)

                conn.execute(SQL_SET_STATE, (batch_max,))
                last_id = batch_max

            conn.commit()
            batches_done += 1

            elapsed = time.time() - t0
            store_rate = total / elapsed if elapsed > 0 else 0.0
            http_rate = http_ok / elapsed if elapsed > 0 else 0.0
            seen_rate = candidates_seen / elapsed if elapsed > 0 else 0.0

            log.info(
                "Stored=%d | cursor=%d | batches=%d | batch=%d | store_speed=%.2f/s | candidates_seen=%d seen_speed=%.2f/s | http_ok=%d http_fail=%d http_speed=%.2f/s | tomb=%d | wrong_template=%d | ico_mismatch=%d",
                total, last_id, batches_done, len(ids),
                store_rate, candidates_seen, seen_rate,
                http_ok, http_fail, http_rate,
                skipped_tombstone, skipped_template,
                skipped_ico_mismatch,
            )

        conn.commit()
        log.info(
            "Done. Stored=%d | candidates_seen=%d | http_ok=%d http_fail=%d | tomb=%d | wrong_template=%d | ico_mismatch=%d | batches=%d | time=%.1fs | last_id=%d",
            total, candidates_seen, http_ok, http_fail,
            skipped_tombstone, skipped_template,
            skipped_ico_mismatch,
            batches_done, time.time() - t0, last_id
        )