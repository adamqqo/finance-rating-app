from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from psycopg.rows import dict_row

from ..config import SD_API_BASE
from ..db import get_conn

log = logging.getLogger("findexio.sd_org")

SYNC_URL = f"{SD_API_BASE}/organizations/sync"
ORG_URL = f"{SD_API_BASE}/organizations/{{id}}"


# -----------------------
# Sync cursor state
# -----------------------

@dataclass
class SdCursor:
    since: str
    last_id: int


def _get_cursor() -> SdCursor:
    """Load sync cursor from core.rpo_bulk_state (id=1)."""
    with get_conn(row_factory=dict_row) as conn:
        row = conn.execute(
            "SELECT sd_since, sd_last_id FROM core.rpo_bulk_state WHERE id = 1"
        ).fetchone()

    if not row:
        return SdCursor(since="1970-01-01T00:00:00Z", last_id=0)

    since = row["sd_since"]
    last_id = row["sd_last_id"]

    if since is None:
        since_iso = "1970-01-01T00:00:00Z"
    else:
        if isinstance(since, datetime):
            since_iso = since.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        else:
            since_iso = str(since)

    return SdCursor(since=since_iso, last_id=int(last_id or 0))


def _set_cursor(cur: SdCursor) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE core.rpo_bulk_state
            SET sd_since = %s::timestamptz,
                sd_last_id = %s,
                last_run_at = now()
            WHERE id = 1
            """,
            (cur.since, cur.last_id),
        )
        conn.commit()


# -----------------------
# HTTP helpers
# -----------------------

class SdHttpError(RuntimeError):
    pass


def _parse_next_link(link_header: Optional[str]) -> Optional[str]:
    """Extract rel='next' from Link header."""
    if not link_header:
        return None

    # Link: <https://.../sync?last_id=...&since=...>; rel='next'
    parts = [p.strip() for p in link_header.split(",")]
    for p in parts:
        if "rel='next'" in p or 'rel="next"' in p:
            start = p.find("<")
            end = p.find(">", start + 1)
            if start != -1 and end != -1:
                return p[start + 1 : end]
    return None


def _throttle_from_headers(resp: requests.Response) -> None:
    """Respect SD rate limit headers when present."""
    try:
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is None or reset is None:
            return
        remaining_i = int(remaining)
        reset_i = int(reset)
        if remaining_i <= 1:
            now = int(time.time())
            sleep_s = max(0, reset_i - now) + 1
            log.info("SD rate limit reached; sleeping %ss", sleep_s)
            time.sleep(sleep_s)
    except Exception:
        return


def _get_list(url: str, *, timeout: int = 30) -> Tuple[List[Dict[str, Any]], requests.Response]:
    r = requests.get(url, timeout=timeout)

    if r.status_code == 429:
        retry_after = r.headers.get("Retry-After")
        sleep_s = int(retry_after) if (retry_after and retry_after.isdigit()) else 5
        log.warning("SD 429 rate-limited; sleeping %ss", sleep_s)
        time.sleep(sleep_s)
        r = requests.get(url, timeout=timeout)

    if not r.ok:
        raise SdHttpError(f"SD request failed: {r.status_code} {r.text[:200]}")

    _throttle_from_headers(r)

    data = r.json()
    if not isinstance(data, list):
        raise SdHttpError(f"Expected list JSON from {url}, got {type(data)}")
    return data, r


def _get_obj(url: str, *, timeout: int = 30) -> Tuple[Dict[str, Any], requests.Response]:
    """GET an endpoint that returns a JSON object (dict)."""
    r = requests.get(url, timeout=timeout)

    if r.status_code == 429:
        retry_after = r.headers.get("Retry-After")
        sleep_s = int(retry_after) if (retry_after and retry_after.isdigit()) else 5
        log.warning("SD 429 rate-limited; sleeping %ss", sleep_s)
        time.sleep(sleep_s)
        r = requests.get(url, timeout=timeout)

    if not r.ok:
        raise SdHttpError(f"SD request failed: {r.status_code} {r.text[:200]}")

    _throttle_from_headers(r)

    data = r.json()
    if not isinstance(data, dict):
        raise SdHttpError(f"Expected object JSON from {url}, got {type(data)}")
    return data, r


# -----------------------
# Normalization
# -----------------------

def _pick_current_entry(entries: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not entries:
        return None

    current = [e for e in entries if e.get("effective_to") in (None, "")]
    if current:
        return max(current, key=lambda e: (e.get("effective_from") or ""))

    return max(entries, key=lambda e: (e.get("effective_to") or "", e.get("effective_from") or ""))


def _norm_org(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    ident = _pick_current_entry(payload.get("identifier_entries") or [])
    if not ident or ident.get("ipo") is None:
        return None

    ico = int(ident["ipo"])
    sd_org_id = int(payload["id"])

    mac = payload.get("main_activity_code")
    mac_id = mac.get("id") if isinstance(mac, dict) else None
    mac_name = mac.get("name") if isinstance(mac, dict) else None

    return {
        "ico": ico,
        "sd_org_id": sd_org_id,
        "established_on": payload.get("established_on"),
        "terminated_on": payload.get("terminated_on"),
        "actualized_at": payload.get("actualized_at"),
        "main_activity_code_id": mac_id,
        "main_activity_code_name": mac_name,
        "main_activity_created_at": mac.get("created_at") if isinstance(mac, dict) else None,
        "main_activity_updated_at": mac.get("updated_at") if isinstance(mac, dict) else None,
    }


def _norm_addresses(payload: Dict[str, Any], ico: int, sd_org_id: int) -> List[Dict[str, Any]]:
    entries = payload.get("address_entries") or []
    if not isinstance(entries, list) or not entries:
        return []

    current = _pick_current_entry(entries)
    current_id = current.get("id") if current else None

    out: List[Dict[str, Any]] = []
    for a in entries:
        if not isinstance(a, dict) or a.get("id") is None:
            continue
        out.append(
            {
                "ico": ico,
                "sd_org_id": sd_org_id,
                "sd_address_entry_id": int(a["id"]),
                "is_current": bool(current_id and int(a["id"]) == int(current_id)),
                "effective_from": a.get("effective_from"),
                "effective_to": a.get("effective_to"),
                "street": a.get("street"),
                "building_number": a.get("building_number"),
                "reg_number": a.get("reg_number"),
                "postal_code": a.get("postal_code"),
                "municipality": a.get("municipality"),
                "district": a.get("district"),
                "country": a.get("country"),
            }
        )
    return out


def _norm_successors(payload: Dict[str, Any], ico: int, sd_org_id: int) -> List[Dict[str, Any]]:
    entries = payload.get("successor_entries") or []
    if not isinstance(entries, list) or not entries:
        return []

    out: List[Dict[str, Any]] = []
    for s in entries:
        if not isinstance(s, dict) or s.get("id") is None:
            continue
        out.append(
            {
                "ico": ico,
                "sd_org_id": sd_org_id,
                "sd_successor_entry_id": int(s["id"]),
                "successor_cin": s.get("cin"),
                "successor_name": s.get("full_name"),
                "successor_established_on": s.get("established_on"),
                "successor_terminated_on": s.get("terminated_on"),
            }
        )
    return out


# -----------------------
# DB batch upserts
# -----------------------

SQL_UPSERT_ACTIVITY = """
INSERT INTO core.sd_activity_code_dim (id, name, created_at, updated_at)
VALUES (%(id)s, %(name)s, %(created_at)s, %(updated_at)s)
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  updated_at = COALESCE(EXCLUDED.updated_at, core.sd_activity_code_dim.updated_at);
"""

SQL_UPSERT_ORG = """
INSERT INTO core.sd_org (
  ico, sd_org_id, established_on, terminated_on, actualized_at,
  main_activity_code_id, main_activity_code_name
)
VALUES (
  %(ico)s, %(sd_org_id)s, %(established_on)s, %(terminated_on)s, %(actualized_at)s,
  %(main_activity_code_id)s, %(main_activity_code_name)s
)
ON CONFLICT (ico) DO UPDATE SET
  sd_org_id = EXCLUDED.sd_org_id,
  established_on = EXCLUDED.established_on,
  terminated_on = EXCLUDED.terminated_on,
  actualized_at = EXCLUDED.actualized_at,
  main_activity_code_id = EXCLUDED.main_activity_code_id,
  main_activity_code_name = EXCLUDED.main_activity_code_name,
  updated_at = now();
"""

SQL_UPSERT_ADDRESS = """
INSERT INTO core.sd_org_address (
  ico, sd_org_id, sd_address_entry_id, is_current,
  effective_from, effective_to,
  street, building_number, reg_number,
  postal_code, municipality, district, country
)
VALUES (
  %(ico)s, %(sd_org_id)s, %(sd_address_entry_id)s, %(is_current)s,
  %(effective_from)s, %(effective_to)s,
  %(street)s, %(building_number)s, %(reg_number)s,
  %(postal_code)s, %(municipality)s, %(district)s, %(country)s
)
ON CONFLICT (sd_address_entry_id) DO UPDATE SET
  ico = EXCLUDED.ico,
  sd_org_id = EXCLUDED.sd_org_id,
  is_current = EXCLUDED.is_current,
  effective_from = EXCLUDED.effective_from,
  effective_to = EXCLUDED.effective_to,
  street = EXCLUDED.street,
  building_number = EXCLUDED.building_number,
  reg_number = EXCLUDED.reg_number,
  postal_code = EXCLUDED.postal_code,
  municipality = EXCLUDED.municipality,
  district = EXCLUDED.district,
  country = EXCLUDED.country;
"""

SQL_UPSERT_SUCCESSOR = """
INSERT INTO core.sd_org_successor (
  ico, sd_org_id, sd_successor_entry_id,
  successor_cin, successor_name,
  successor_established_on, successor_terminated_on
)
VALUES (
  %(ico)s, %(sd_org_id)s, %(sd_successor_entry_id)s,
  %(successor_cin)s, %(successor_name)s,
  %(successor_established_on)s, %(successor_terminated_on)s
)
ON CONFLICT (sd_successor_entry_id) DO UPDATE SET
  ico = EXCLUDED.ico,
  sd_org_id = EXCLUDED.sd_org_id,
  successor_cin = EXCLUDED.successor_cin,
  successor_name = EXCLUDED.successor_name,
  successor_established_on = EXCLUDED.successor_established_on,
  successor_terminated_on = EXCLUDED.successor_terminated_on;
"""


def _db_upsert_batch(
    *,
    org_rows: List[Dict[str, Any]],
    activity_rows: List[Dict[str, Any]],
    address_rows: List[Dict[str, Any]],
    successor_rows: List[Dict[str, Any]],
) -> None:
    if not (org_rows or activity_rows or address_rows or successor_rows):
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            if activity_rows:
                cur.executemany(SQL_UPSERT_ACTIVITY, activity_rows)
            if org_rows:
                cur.executemany(SQL_UPSERT_ORG, org_rows)
            if address_rows:
                ico_set = sorted({r["ico"] for r in address_rows})
                cur.execute(
                    "UPDATE core.sd_org_address SET is_current = FALSE WHERE ico = ANY(%s)",
                    (ico_set,),
                )
                cur.executemany(SQL_UPSERT_ADDRESS, address_rows)
            if successor_rows:
                cur.executemany(SQL_UPSERT_SUCCESSOR, successor_rows)
        conn.commit()


# -----------------------
# Main sync loop
# -----------------------

def _build_sync_url(since: str, last_id: int) -> str:
    return f"{SYNC_URL}?since={since}&last_id={last_id}&only_ids"


def run_sync(*, hard_limit: Optional[int] = None, db_batch_size: int = 200) -> None:
    cursor = _get_cursor()
    log.info("SD sync starting from since=%s last_id=%s", cursor.since, cursor.last_id)

    next_url = _build_sync_url(cursor.since, cursor.last_id)
    processed = 0

    org_rows: List[Dict[str, Any]] = []
    activity_rows: List[Dict[str, Any]] = []
    address_rows: List[Dict[str, Any]] = []
    successor_rows: List[Dict[str, Any]] = []

    last_seen: Optional[SdCursor] = None

    while next_url:
        items, resp = _get_list(next_url, timeout=60)
        if not items:
            next_url = _parse_next_link(resp.headers.get("Link"))
            continue

        for item in items:
            if not isinstance(item, dict) or item.get("id") is None:
                continue

            sd_org_id = int(item["id"])
            detail_url = ORG_URL.format(id=sd_org_id)

            try:
                payload, _ = _get_obj(detail_url, timeout=60)
            except Exception as e:
                log.warning("SD detail fetch failed id=%s: %s", sd_org_id, e)
                continue

            org = _norm_org(payload)
            if org is None:
                continue

            ico = org["ico"]
            sd_org_id = org["sd_org_id"]

            if org.get("main_activity_code_id") is not None:
                activity_rows.append(
                    {
                        "id": org["main_activity_code_id"],
                        "name": org.get("main_activity_code_name"),
                        "created_at": org.get("main_activity_created_at"),
                        "updated_at": org.get("main_activity_updated_at"),
                    }
                )

            org_rows.append(
                {
                    "ico": ico,
                    "sd_org_id": sd_org_id,
                    "established_on": org.get("established_on"),
                    "terminated_on": org.get("terminated_on"),
                    "actualized_at": org.get("actualized_at"),
                    "main_activity_code_id": org.get("main_activity_code_id"),
                    "main_activity_code_name": org.get("main_activity_code_name"),
                }
            )

            address_rows.extend(_norm_addresses(payload, ico, sd_org_id))
            successor_rows.extend(_norm_successors(payload, ico, sd_org_id))

            processed += 1

            upd = item.get("updated_at")
            if upd:
                last_seen = SdCursor(since=str(upd), last_id=int(item["id"]))

            if processed % db_batch_size == 0:
                _db_upsert_batch(
                    org_rows=org_rows,
                    activity_rows=activity_rows,
                    address_rows=address_rows,
                    successor_rows=successor_rows,
                )
                org_rows.clear()
                activity_rows.clear()
                address_rows.clear()
                successor_rows.clear()

                if last_seen:
                    _set_cursor(last_seen)

            if hard_limit is not None and processed >= hard_limit:
                log.info("SD hard_limit reached: %s", hard_limit)
                next_url = None
                break

        if hard_limit is not None and processed >= hard_limit:
            break

        next_url = _parse_next_link(resp.headers.get("Link"))
        if not next_url and last_seen:
            _set_cursor(last_seen)

    _db_upsert_batch(
        org_rows=org_rows,
        activity_rows=activity_rows,
        address_rows=address_rows,
        successor_rows=successor_rows,
    )
    if last_seen:
        _set_cursor(last_seen)

    log.info("SD sync done. processed=%s", processed)
