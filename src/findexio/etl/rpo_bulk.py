from __future__ import annotations

import gzip
import io
import json
import logging
import re
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
import psycopg
from botocore import UNSIGNED
from botocore.config import Config
from psycopg.rows import dict_row

from ..config import RPO_S3_ENDPOINT
from ..db import ensure_schema, get_conn

log = logging.getLogger("findexio.rpo_bulk")

S3_ENDPOINT = (RPO_S3_ENDPOINT or "").rstrip("/")
BUCKET = "susr-rpo"
PREFIX_INIT = "batch-init/"
PREFIX_DAILY = "batch-daily/"

# --------------------------------------------------------------------------------------
# DB STATE
# --------------------------------------------------------------------------------------

SQL_SET_INIT = "UPDATE core.rpo_bulk_state SET last_init_key = %s, last_run_at = now() WHERE id = 1;"
SQL_SET_DAILY = "UPDATE core.rpo_bulk_state SET last_daily_key = %s, last_run_at = now() WHERE id = 1;"
SQL_GET_STATE = "SELECT last_init_key, last_daily_key FROM core.rpo_bulk_state WHERE id = 1;"

# Upsert includes legal_form_code + legal_form_name.
SQL_UPSERT = """
INSERT INTO core.rpo_all_orgs (
  ico, name, legal_form, legal_form_code, legal_form_name,
  status, address, updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, now())
ON CONFLICT (ico) DO UPDATE SET
  name            = EXCLUDED.name,
  legal_form      = EXCLUDED.legal_form,
  legal_form_code = EXCLUDED.legal_form_code,
  legal_form_name = EXCLUDED.legal_form_name,
  status          = EXCLUDED.status,
  address         = EXCLUDED.address,
  updated_at      = now();
"""

# Backfill: fill missing new cols from the legacy `legal_form` string in shape "XXX - Name" (dash variants allowed).
SQL_BACKFILL_FROM_LEGAL_FORM = r"""
UPDATE core.rpo_all_orgs
SET
  legal_form_code = COALESCE(
    legal_form_code,
    (regexp_match(legal_form, '^\s*(\d+)\s*[\-–—]\s*(.+?)\s*$'))[1]
  ),
  legal_form_name = COALESCE(
    legal_form_name,
    (regexp_match(legal_form, '^\s*(\d+)\s*[\-–—]\s*(.+?)\s*$'))[2]
  )
WHERE legal_form IS NOT NULL
  AND (legal_form_code IS NULL OR legal_form_name IS NULL);
"""

# --------------------------------------------------------------------------------------
# S3
# --------------------------------------------------------------------------------------


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        config=Config(signature_version=UNSIGNED),
        region_name="eu-frankfurt-1",
    )


def list_objects(prefix: str) -> List[Dict[str, Any]]:
    cli = s3_client()
    token = None
    out: List[Dict[str, Any]] = []
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = cli.list_objects_v2(**kw)
        out.extend(resp.get("Contents", []))
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    return out


def _export_date_from_daily_key(key: str) -> Optional[datetime]:
    m = re.search(r"actual_(\d{4}-\d{2}-\d{2})\.json\.gz$", key)
    return datetime.strptime(m.group(1), "%Y-%m-%d") if m else None


def daily_keys_sorted(objs: List[Dict[str, Any]]) -> List[str]:
    pat = re.compile(r"^batch-daily/actual_(\d{4}-\d{2}-\d{2})\.json\.gz$")
    parsed: List[Tuple[datetime, str]] = []
    for o in objs:
        k = o["Key"]
        m = pat.match(k)
        if not m:
            continue
        d = datetime.strptime(m.group(1), "%Y-%m-%d")
        parsed.append((d, k))
    parsed.sort()
    return [k for _, k in parsed]


def init_keys_for_latest_date(objs: List[Dict[str, Any]], pinned_date: Optional[str] = None) -> Tuple[str, List[str]]:
    """
    Returns (init_date_str, keys_for_that_date_sorted_by_seq)
    Keys are like: batch-init/init_YYYY-MM-DD_SEQ.json.gz
    """
    pat = re.compile(r"^batch-init/init_(\d{4}-\d{2}-\d{2})_(\d+)\.json\.gz$")
    by_date: Dict[str, List[Tuple[int, str]]] = {}
    for o in objs:
        k = o["Key"]
        m = pat.match(k)
        if not m:
            continue
        d, seq = m.group(1), int(m.group(2))
        by_date.setdefault(d, []).append((seq, k))

    if not by_date:
        raise RuntimeError("Nenašli sa žiadne 'batch-init' súbory.")

    if pinned_date and pinned_date in by_date:
        init_date_str = pinned_date
    else:
        init_date_str = sorted(by_date.keys())[-1]

    keys = [k for _, k in sorted(by_date[init_date_str])]
    return init_date_str, keys


def _init_date_from_key(key: Optional[str]) -> Optional[str]:
    m = re.search(r"batch-init/init_(\d{4}-\d{2}-\d{2})_\d+\.json\.gz$", key or "")
    return m.group(1) if m else None


# --------------------------------------------------------------------------------------
# STREAMING EXPORT PARSING (NO FULL READ INTO RAM)
# --------------------------------------------------------------------------------------


def _iter_jsonl(gz: gzip.GzipFile) -> Iterable[Dict[str, Any]]:
    """
    Stream JSON Lines: each line is a JSON dict.
    """
    for line in gz:
        line = line.strip()
        if not line:
            continue
        obj = json.loads(line)
        if isinstance(obj, dict):
            yield obj


def _iter_big_json_with_ijson(gz: gzip.GzipFile) -> Iterable[Dict[str, Any]]:
    """
    Stream parse either:
      - {"results":[...]}  -> path "results.item"
      - [...]             -> path "item"
    Requires dependency `ijson`.
    """
    try:
        import ijson  # type: ignore
    except Exception as e:
        raise RuntimeError(
            "Export nie je JSONL a vyžaduje streamové parsovanie cez 'ijson'. "
            "Pridaj dependency: pip install ijson (alebo do pyproject/requirements)."
        ) from e

    # Try dict.results first, then array root
    # NOTE: ijson consumes the stream; for the second attempt we need a fresh stream.
    # Therefore, caller must provide a rewindable source, or we reopen S3 object. We do reopen in iter_export_records_s3().
    for path in ("results.item", "item"):
        for rec in ijson.items(gz, path):
            if isinstance(rec, dict):
                yield rec
        return


def iter_export_records_s3(key: str) -> Iterable[Dict[str, Any]]:
    """
    Stream records directly from S3 -> gzip -> records.

    Strategy:
      1) Heuristic attempt JSONL (fast, no extra dependency).
      2) If JSONL fails, reopen stream and parse via ijson.
    """
    cli = s3_client()

    def open_gz() -> gzip.GzipFile:
        resp = cli.get_object(Bucket=BUCKET, Key=key)
        return gzip.GzipFile(fileobj=resp["Body"])

    # Attempt JSONL: read one line, if it parses to dict and next bytes look like newline-delimited objects,
    # proceed with JSONL streaming. If it fails, fallback to ijson on a fresh stream.
    gz1 = open_gz()
    try:
        first_line = gz1.readline()
        if not first_line:
            return
        try:
            first_obj = json.loads(first_line)
        except Exception:
            first_obj = None

        if isinstance(first_obj, dict):
            # Yield first parsed line, then the rest as JSONL
            yield first_obj
            for rec in _iter_jsonl(gz1):
                yield rec
            return
    finally:
        try:
            gz1.close()
        except Exception:
            pass

    # Fallback: reopen and stream parse big JSON via ijson
    gz2 = open_gz()
    try:
        for rec in _iter_big_json_with_ijson(gz2):
            yield rec
    finally:
        try:
            gz2.close()
        except Exception:
            pass


# --------------------------------------------------------------------------------------
# NORMALIZATION HELPERS (LEGAL FORM INCLUDED)
# --------------------------------------------------------------------------------------

_LF_RE = re.compile(r"^\s*(\d+)\s*[\-–—]\s*(.+?)\s*$")


def parse_legal_form_text(legal_form: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Parse 'XXX - Name' (accepts -, – and —)."""
    if not legal_form:
        return None, None
    m = _LF_RE.match(str(legal_form))
    if not m:
        return None, None
    code = (m.group(1) or "").strip() or None
    name = (m.group(2) or "").strip() or None
    return code, name


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except Exception:
        return None


def _pick_current(entries: Optional[List[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    if not entries:
        return None
    current = [e for e in entries if not e.get("validTo")]
    if current:
        current.sort(key=lambda e: _parse_dt(e.get("validFrom")) or datetime.min)
        return current[-1]
    entries.sort(key=lambda e: _parse_dt(e.get("validFrom")) or datetime.min)
    return entries[-1]


def normalize_item(it: Dict[str, Any]) -> Dict[str, Optional[str]]:
    # ICO
    ico: Optional[str] = None
    ids = it.get("identifiers") or []
    pick_id = _pick_current(ids)
    if isinstance(pick_id, dict):
        ico = pick_id.get("value")
    if not ico:
        ico = it.get("identifier") or it.get("ico") or it.get("ipo")
    if ico:
        ico = str(ico).strip().replace(" ", "")

    # Name
    name: Optional[str] = None
    for key in ("fullNames", "businessNames", "names"):
        arr = it.get(key)
        if isinstance(arr, list) and arr:
            cur = _pick_current(arr) or arr[0]
            if isinstance(cur, dict):
                name = cur.get("value") or name
        if name:
            break

    # Legal form fields (new + legacy)
    legal_form: Optional[str] = None
    legal_form_code: Optional[str] = None
    legal_form_name: Optional[str] = None

    lf_arr = it.get("legalForms") or []
    lf = _pick_current(lf_arr)
    if isinstance(lf, dict):
        val = lf.get("value")
        # Typical API shape: {"code": "112", "value": "Obchodná spoločnosť"}
        if isinstance(val, dict):
            if val.get("code") is not None:
                legal_form_code = str(val.get("code")).strip() or None
            if val.get("value") is not None:
                legal_form_name = str(val.get("value")).strip() or None
        elif val is not None:
            # Some exports might provide a string directly (often "XXX - Name")
            legal_form = str(val).strip() or None

    # Keep legacy column consistent
    if not legal_form and (legal_form_code or legal_form_name):
        if legal_form_code and legal_form_name:
            legal_form = f"{legal_form_code} - {legal_form_name}"
        else:
            legal_form = legal_form_name or legal_form_code

    # Derive missing code/name from legacy text
    if (not legal_form_code or not legal_form_name) and legal_form:
        c, n = parse_legal_form_text(legal_form)
        legal_form_code = legal_form_code or c
        legal_form_name = legal_form_name or n

    status = "terminated" if it.get("termination") else "active"

    # Address
    address: Optional[str] = None
    addr_arr = it.get("addresses") or []
    a = _pick_current(addr_arr)
    if isinstance(a, dict):
        address = a.get("formatedAddress")
        if not address:
            muni = (a.get("municipality") or {}).get("value")
            pcs = a.get("postalCodes") or []
            regn = a.get("regNumber")
            country = (a.get("country") or {}).get("value")
            parts = [p for p in [muni, str(regn) if regn else None, (pcs[0] if pcs else None), country] if p]
            address = ", ".join(parts) if parts else None

    return {
        "ico": ico if (ico and ico.isdigit() and len(ico) == 8) else None,
        "name": name,
        "legal_form": legal_form,
        "legal_form_code": legal_form_code,
        "legal_form_name": legal_form_name,
        "status": status,
        "address": address,
    }


# --------------------------------------------------------------------------------------
# BACKFILL (POST-INIT COLUMNS)
# --------------------------------------------------------------------------------------


def backfill_legal_form_fields(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(SQL_BACKFILL_FROM_LEGAL_FORM)
        updated = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    return int(updated) if updated and updated > 0 else 0


# --------------------------------------------------------------------------------------
# APPLY BATCH (STREAMING)
# --------------------------------------------------------------------------------------


def apply_batch_to_db(
    key: str,
    conn: psycopg.Connection,
    *,
    commit_every: int = 10_000,
    log_every: int = 50_000,
) -> int:
    """
    Applies one INIT/DAILY gzip export into DB with streaming S3 read (no full payload in memory).
    """
    t0 = time.time()
    n = 0
    skipped_no_ico = 0
    since_commit = 0

    with conn.cursor() as cur:
        for rec in iter_export_records_s3(key):
            meta = normalize_item(rec)
            ico = meta["ico"]
            if not ico:
                skipped_no_ico += 1
                continue

            cur.execute(
                SQL_UPSERT,
                (
                    ico,
                    meta["name"],
                    meta["legal_form"],
                    meta["legal_form_code"],
                    meta["legal_form_name"],
                    meta["status"],
                    meta["address"],
                ),
            )

            n += 1
            since_commit += 1

            if since_commit >= commit_every:
                conn.commit()
                since_commit = 0

            if log_every and (n % log_every == 0):
                elapsed = time.time() - t0
                rate = n / elapsed if elapsed > 0 else 0.0
                log.info(
                    "RPO %s | progress=%d | skipped_no_ico=%d | speed=%.1f rec/s",
                    key,
                    n,
                    skipped_no_ico,
                    rate,
                )

    conn.commit()
    elapsed = time.time() - t0
    rate = n / elapsed if elapsed > 0 else 0.0
    log.info(
        "Applied %d records from %s (skipped_no_ico=%d) | time=%.1fs | avg_speed=%.1f rec/s",
        n,
        key,
        skipped_no_ico,
        elapsed,
        rate,
    )
    return n


# --------------------------------------------------------------------------------------
# ORCHESTRATION
# --------------------------------------------------------------------------------------


def run_full_sync(
    *,
    apply_daily: bool = True,
    max_daily: Optional[int] = None,
    reset_init: bool = False,
    reset_daily: bool = False,
    reset_all: bool = False,
    backfill_missing_legal_form_fields: bool = True,
) -> None:
    """
    - Applies the latest INIT (or continues from last_init_key).
    - Applies DAILY files newer than INIT date (or continues from last_daily_key).
    - Backfills legal_form_code/legal_form_name from legal_form for DBs where INIT predated the columns.
    """
    with get_conn(row_factory=dict_row) as conn:
        ensure_schema(conn)

        if reset_all:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE core.rpo_all_orgs;")
                cur.execute(
                    "UPDATE core.rpo_bulk_state "
                    "SET last_init_key = NULL, last_daily_key = NULL, last_run_at = now() "
                    "WHERE id = 1;"
                )
            conn.commit()

        if reset_init and not reset_all:
            conn.execute("UPDATE core.rpo_bulk_state SET last_init_key = NULL, last_run_at = now() WHERE id = 1;")
            conn.commit()

        if reset_daily and not reset_all:
            conn.execute("UPDATE core.rpo_bulk_state SET last_daily_key = NULL, last_run_at = now() WHERE id = 1;")
            conn.commit()

        # Backfill new columns from legacy text
        if backfill_missing_legal_form_fields:
            updated = backfill_legal_form_fields(conn)
            if updated:
                log.info("Backfilled legal_form_code/legal_form_name for %d existing org rows.", updated)

        st = conn.execute(SQL_GET_STATE).fetchone() or {"last_init_key": None, "last_daily_key": None}
        last_init_key = st["last_init_key"]
        last_daily_key = st["last_daily_key"]

        # INIT
        objs_init = list_objects(PREFIX_INIT)
        pinned_date = _init_date_from_key(last_init_key) if last_init_key else None
        init_date_str, init_keys = init_keys_for_latest_date(objs_init, pinned_date=pinned_date)
        init_date = datetime.strptime(init_date_str, "%Y-%m-%d")

        start_idx = 0
        if last_init_key and last_init_key in init_keys:
            start_idx = init_keys.index(last_init_key) + 1

        if start_idx < len(init_keys):
            for key in init_keys[start_idx:]:
                log.info("Downloading INIT %s", key)
                apply_batch_to_db(key, conn)
                conn.execute(SQL_SET_INIT, (key,))
                conn.commit()
        else:
            log.info("INIT %s already applied.", init_date_str)

        if not apply_daily:
            return

        # DAILY newer than INIT date
        objs_daily = list_objects(PREFIX_DAILY)
        keys_daily_all = daily_keys_sorted(objs_daily)
        if not keys_daily_all:
            log.info("No batch-daily files.")
            return

        keys_daily = [k for k in keys_daily_all if (_export_date_from_daily_key(k) or datetime.min) > init_date]
        if not keys_daily:
            log.info("No DAILY newer than INIT.")
            return

        start_idx = 0
        if last_daily_key and last_daily_key in keys_daily:
            start_idx = keys_daily.index(last_daily_key) + 1

        applied = 0
        for key in keys_daily[start_idx:]:
            log.info("Downloading DAILY %s", key)
            apply_batch_to_db(key, conn)
            conn.execute(SQL_SET_DAILY, (key,))
            conn.commit()

            applied += 1
            if max_daily is not None and applied >= max_daily:
                break
