from __future__ import annotations

import time
import io
import re
import gzip
import json
import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import boto3
from botocore import UNSIGNED
from botocore.config import Config
import psycopg
from psycopg.rows import dict_row

from ..config import RPO_S3_ENDPOINT
from ..db import get_conn, ensure_schema

S3_ENDPOINT = RPO_S3_ENDPOINT.rstrip("/")
BUCKET = "susr-rpo"
PREFIX_INIT = "batch-init/"
PREFIX_DAILY = "batch-daily/"

log = logging.getLogger("findexio.rpo_bulk")

SQL_SET_INIT = "UPDATE core.rpo_bulk_state SET last_init_key = %s, last_run_at = now() WHERE id = 1;"
SQL_SET_DAILY = "UPDATE core.rpo_bulk_state SET last_daily_key = %s, last_run_at = now() WHERE id = 1;"
SQL_GET_STATE = "SELECT last_init_key, last_daily_key FROM core.rpo_bulk_state WHERE id = 1;"

SQL_UPSERT = """
INSERT INTO core.rpo_all_orgs (ico, name, legal_form, status, address, updated_at)
VALUES (%s, %s, %s, %s, %s, now())
ON CONFLICT (ico) DO UPDATE SET
  name       = EXCLUDED.name,
  legal_form = EXCLUDED.legal_form,
  status     = EXCLUDED.status,
  address    = EXCLUDED.address,
  updated_at = now();
"""


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

def _init_date_from_key(key: str) -> Optional[str]:
    m = re.search(r"batch-init/init_(\d{4}-\d{2}-\d{2})_\d+\.json\.gz$", key or "")
    return m.group(1) if m else None

def init_keys_for_latest_date(objs: List[Dict[str, Any]]) -> Tuple[str, List[str]]:
    pat = re.compile(r"^batch-init/init_(\d{4}-\d{2}-\d{2})_(\d+)\.json\.gz$")
    by_date: Dict[str, List[Tuple[int, str]]] = {}
    for o in objs:
        k = o["Key"]
        m = pat.match(k)
        if not m:
            continue
        date_str, seq = m.group(1), int(m.group(2))
        by_date.setdefault(date_str, []).append((seq, k))
    if not by_date:
        return "", []
    latest_date = sorted(by_date.keys())[-1]
    seq_keys = [k for _, k in sorted(by_date[latest_date])]
    return latest_date, seq_keys


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


def download_object(key: str) -> bytes:
    cli = s3_client()
    resp = cli.get_object(Bucket=BUCKET, Key=key)
    return resp["Body"].read()


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


def _export_date_from_daily_key(key: str) -> Optional[datetime]:
    m = re.search(r"actual_(\d{4}-\d{2}-\d{2})\.json\.gz$", key)
    return datetime.strptime(m.group(1), "%Y-%m-%d") if m else None


def iter_export_records(payload: bytes) -> Iterable[Dict[str, Any]]:
    with gzip.GzipFile(fileobj=io.BytesIO(payload)) as gz:
        raw = gz.read()

    try:
        obj = json.loads(raw)
        if isinstance(obj, dict) and isinstance(obj.get("results"), list):
            for rec in obj["results"]:
                if isinstance(rec, dict):
                    yield rec
            return
        if isinstance(obj, list):
            for rec in obj:
                if isinstance(rec, dict):
                    yield rec
            return
    except Exception:
        pass

    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except Exception:
            continue
        if isinstance(rec, dict):
            yield rec


def normalize_item(it: Dict[str, Any]) -> Dict[str, Optional[str]]:
    ico: Optional[str] = None
    ids = it.get("identifiers") or []
    pick_id = _pick_current(ids)
    if isinstance(pick_id, dict):
        ico = pick_id.get("value")
    if not ico:
        ico = it.get("identifier") or it.get("ico") or it.get("ipo")
    if ico:
        ico = str(ico).strip().replace(" ", "")

    name: Optional[str] = None
    for key in ("fullNames", "businessNames", "names"):
        arr = it.get(key)
        if isinstance(arr, list) and arr:
            cur = _pick_current(arr) or arr[0]
            if isinstance(cur, dict):
                name = cur.get("value") or name
        if name:
            break

    legal_form: Optional[str] = None
    lf_arr = it.get("legalForms") or []
    lf = _pick_current(lf_arr)
    if isinstance(lf, dict):
        val = lf.get("value") or {}
        if isinstance(val, dict):
            code = val.get("code")
            txt = val.get("value")
            legal_form = f"{code} – {txt}" if code and txt else (txt or code)
        else:
            legal_form = str(val) if val else None

    status = "terminated" if it.get("termination") else "active"

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
        "status": status,
        "address": address,
    }


def apply_batch_to_db(
    key: str,
    payload: bytes,
    conn: psycopg.Connection,
    *,
    commit_every: int = 10000,
    log_every: int = 50000,
) -> int:
    """
    Apply one INIT/DAILY gzip export into DB with batching + progress logs.
    - commit_every: commit after N successful upserts (reduces risk on long runs)
    - log_every: log progress each N processed records
    """
    t0 = time.time()
    n = 0
    skipped_no_ico = 0
    since_commit = 0

    with conn.cursor() as cur:
        for rec in iter_export_records(payload):
            meta = normalize_item(rec)
            ico = meta["ico"]
            if not ico:
                skipped_no_ico += 1
                continue

            cur.execute(
                SQL_UPSERT,
                (ico, meta["name"], meta["legal_form"], meta["status"], meta["address"]),
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

    # final commit for remainder
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



def run_full_sync(
    *,
    apply_daily: bool = True,
    max_daily: Optional[int] = None,
    reset_init: bool = False,
    reset_daily: bool = False,
    reset_all: bool = False,
) -> None:
    with get_conn(row_factory=dict_row) as conn:
        ensure_schema(conn)

        if reset_all:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE core.rpo_all_orgs;")
                cur.execute(
                    "UPDATE core.rpo_bulk_state SET last_init_key = NULL, last_daily_key = NULL, last_run_at = now() WHERE id = 1;"
                )
            conn.commit()

        if reset_init and not reset_all:
            conn.execute("UPDATE core.rpo_bulk_state SET last_init_key = NULL, last_run_at = now() WHERE id = 1;")
            conn.commit()

        if reset_daily and not reset_all:
            conn.execute("UPDATE core.rpo_bulk_state SET last_daily_key = NULL, last_run_at = now() WHERE id = 1;")
            conn.commit()

        st = conn.execute(SQL_GET_STATE).fetchone() or {"last_init_key": None, "last_daily_key": None}
        last_init_key = st["last_init_key"]
        last_daily_key = st["last_daily_key"]

        # INIT
        objs_init = list_objects(PREFIX_INIT)

        # Zoskup INIT súbory podľa dátumu
        pat = re.compile(r"^batch-init/init_(\d{4}-\d{2}-\d{2})_(\d+)\.json\.gz$")
        by_date: Dict[str, List[Tuple[int, str]]] = {}
        for o in objs_init:
            k = o["Key"]
            m = pat.match(k)
            if not m:
                continue
            d, seq = m.group(1), int(m.group(2))
            by_date.setdefault(d, []).append((seq, k))

        if not by_date:
            raise RuntimeError("Nenašli sa žiadne 'batch-init' súbory.")

        # Ak máme last_init_key, pokračujeme v tom istom dátume; inak berieme najnovší INIT dátum
        pinned_date = _init_date_from_key(last_init_key) if last_init_key else None
        if pinned_date and pinned_date in by_date:
            init_date_str = pinned_date
        else:
            init_date_str = sorted(by_date.keys())[-1]

        init_keys = [k for _, k in sorted(by_date[init_date_str])]
        init_date = datetime.strptime(init_date_str, "%Y-%m-%d")

        start_idx = 0
        if last_init_key and last_init_key in init_keys:
            start_idx = init_keys.index(last_init_key) + 1

        if start_idx < len(init_keys):
            for key in init_keys[start_idx:]:
                log.info("Downloading INIT %s", key)
                payload = download_object(key)
                apply_batch_to_db(key, payload, conn)
                conn.execute(SQL_SET_INIT, (key,))
                conn.commit()
        else:
            log.info("INIT %s already applied.", init_date_str)

        if not apply_daily:
            return

        # DAILY strictly newer than INIT date
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
            payload = download_object(key)
            apply_batch_to_db(key, payload, conn)
            conn.execute(SQL_SET_DAILY, (key,))
            conn.commit()
            applied += 1
            if max_daily is not None and applied >= max_daily:
                break
