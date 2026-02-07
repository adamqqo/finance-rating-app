from __future__ import annotations

import codecs
import gzip
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

# -----------------------------
# DB state
# -----------------------------
SQL_GET_STATE = "SELECT last_init_key, last_daily_key FROM core.rpo_bulk_state WHERE id = 1;"
SQL_SET_INIT = "UPDATE core.rpo_bulk_state SET last_init_key = %s, last_run_at = now() WHERE id = 1;"
SQL_SET_DAILY = "UPDATE core.rpo_bulk_state SET last_daily_key = %s, last_run_at = now() WHERE id = 1;"

# Upsert includes legal_form_code + legal_form_name
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

# Backfill for DBs where INIT ran before the new cols existed
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


# -----------------------------
# S3 helpers
# -----------------------------
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


def _init_date_from_key(key: Optional[str]) -> Optional[str]:
    m = re.search(r"batch-init/init_(\d{4}-\d{2}-\d{2})_\d+\.json\.gz$", key or "")
    return m.group(1) if m else None


def init_keys_for_date(objs: List[Dict[str, Any]], pinned_date: Optional[str] = None) -> Tuple[str, List[str]]:
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
        date_str = pinned_date
    else:
        date_str = sorted(by_date.keys())[-1]

    keys = [k for _, k in sorted(by_date[date_str])]
    return date_str, keys


# -----------------------------
# Streaming JSON parser (no ijson)
# Supports:
#   1) JSON root array: [ {...}, {...}, ... ]
#   2) Wrapper object: {"results":[ {...}, ... ]}
# Also supports JSONL as a fast path.
# -----------------------------
_JSON_DECODER = json.JSONDecoder()


def _iter_jsonl_from_text_stream(text_iter: Iterable[str]) -> Optional[Iterable[Dict[str, Any]]]:
    """
    Detects JSONL cheaply: tries to parse first non-empty line as dict.
    If it looks like JSONL, returns generator; else returns None.
    """
    buf_lines: List[str] = []
    for s in text_iter:
        buf_lines.append(s)
        # stop once we have at least one full line
        joined = "".join(buf_lines)
        if "\n" not in joined:
            continue

        first_line, rest = joined.split("\n", 1)
        first_line = first_line.strip()
        if not first_line:
            # keep scanning
            buf_lines = [rest]
            continue

        try:
            obj = json.loads(first_line)
        except Exception:
            return None

        if not isinstance(obj, dict):
            return None

        def gen():
            yield obj
            # process remaining buffered content line-by-line
            pending = rest
            # include remaining stream
            for chunk in text_iter:
                pending += chunk
                while "\n" in pending:
                    line, pending = pending.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    if isinstance(rec, dict):
                        yield rec
            # tail
            tail = pending.strip()
            if tail:
                rec = json.loads(tail)
                if isinstance(rec, dict):
                    yield rec

        return gen()

    return None


def _incremental_text_from_gz(gz: gzip.GzipFile, chunk_size: int = 1 << 20) -> Iterable[str]:
    """
    Yields UTF-8 decoded text chunks from gz without splitting multibyte sequences.
    """
    decoder = codecs.getincrementaldecoder("utf-8")()
    while True:
        b = gz.read(chunk_size)
        if not b:
            break
        yield decoder.decode(b)
    tail = decoder.decode(b"", final=True)
    if tail:
        yield tail


def _find_array_start_for_results(buffer: str) -> Optional[int]:
    """
    Try to find the start index of the '[' that begins the results array in a wrapper object.
    This is intentionally conservative; it searches for "results" then the next '['.
    """
    m = re.search(r'"results"\s*:\s*\[', buffer)
    if not m:
        return None
    # find the '[' after the match
    idx = buffer.find("[", m.end() - 1)
    return idx if idx != -1 else None


def _iter_objects_from_json_array(text_iter: Iterable[str], *, wrapper_results: bool) -> Iterable[Dict[str, Any]]:
    """
    Stream parse objects from a JSON array without loading whole file.

    - If wrapper_results=True, expects wrapper object and parses objects inside "results":[...]
    - If wrapper_results=False, expects root array: [...]
    """
    buf = ""
    in_array = False
    done = False
    i = 0

    def lstrip_ws(s: str) -> str:
        return s.lstrip(" \t\r\n")

    for chunk in text_iter:
        buf += chunk

        while True:
            if done:
                return

            if not in_array:
                b2 = lstrip_ws(buf)
                # If wrapper, find "results":[
                if wrapper_results:
                    start = _find_array_start_for_results(buf)
                    if start is None:
                        # keep buffer bounded so it doesn't grow forever while scanning header
                        if len(buf) > 2_000_000:
                            buf = buf[-2_000_000:]
                        break
                    buf = buf[start + 1 :]  # move after '['
                    in_array = True
                else:
                    if not b2:
                        # need more
                        break
                    if b2[0] != "[":
                        raise RuntimeError("Neočakávaný JSON formát: nevidím root '['.")
                    # drop up to and including first '['
                    drop = buf.find("[")
                    buf = buf[drop + 1 :]
                    in_array = True

            # Now we are inside array content, parse comma-separated JSON values until we hit ']'
            buf = lstrip_ws(buf)
            if not buf:
                break

            if buf[0] == "]":
                done = True
                return

            # tolerate leading commas
            if buf[0] == ",":
                buf = lstrip_ws(buf[1:])
                if not buf:
                    break
                if buf[0] == "]":
                    done = True
                    return

            # Parse one JSON value using raw_decode
            try:
                obj, end = _JSON_DECODER.raw_decode(buf)
            except json.JSONDecodeError:
                # Need more data
                # Keep buffer from exploding if something is pathological; but generally object size drives this.
                if len(buf) > 50_000_000:
                    raise RuntimeError(
                        "JSON objekt je príliš veľký pre streaming parser bez ijson. "
                        "Toto typicky znamená extrémne veľké vnorené polia v jednom zázname."
                    )
                break

            buf = buf[end:]
            if isinstance(obj, dict):
                yield obj
            # If it's not a dict, ignore (shouldn't happen for these exports)
            i += 1

    if not done:
        # finalize parse attempt if file ended unexpectedly
        tail = lstrip_ws(buf)
        if tail and tail != "]":
            raise RuntimeError("Neúplný JSON: stream skončil uprostred parsovania.")


def iter_export_records_s3(key: str) -> Iterable[Dict[str, Any]]:
    """
    Main entry: stream records from S3 gzip export.

    Supported:
      - JSONL (each line is a dict)
      - Root array ([{...}, ...])
      - Wrapper object {"results":[{...}, ...]}
    """
    cli = s3_client()
    resp = cli.get_object(Bucket=BUCKET, Key=key)

    with gzip.GzipFile(fileobj=resp["Body"]) as gz:
        # Convert to incremental text stream
        text_stream = _incremental_text_from_gz(gz)

        # Fast path: JSONL detection
        # To do detection, we need a "tee" approach: we buffer a little.
        # We'll read a few chunks into memory to attempt JSONL; if not JSONL, we parse array/wrapper using those chunks too.
        prebuf: List[str] = []
        it = iter(text_stream)
        for _ in range(3):  # a few chunks are enough for detection + header
            try:
                prebuf.append(next(it))
            except StopIteration:
                break

        def combined_iter():
            for s in prebuf:
                yield s
            for s in it:
                yield s

        combined = combined_iter()

        # Try JSONL
        jsonl_gen = _iter_jsonl_from_text_stream(combined)
        if jsonl_gen is not None:
            for rec in jsonl_gen:
                yield rec
            return

    # If not JSONL, reopen stream and parse as array/wrapper (need fresh gz stream)
    resp2 = cli.get_object(Bucket=BUCKET, Key=key)
    with gzip.GzipFile(fileobj=resp2["Body"]) as gz2:
        # Peek first non-ws char to decide root array vs wrapper object
        # We'll stream text and also keep small rolling buffer for the decision.
        text_iter = _incremental_text_from_gz(gz2)
        buf = ""
        it2 = iter(text_iter)

        # read until we can decide
        while True:
            try:
                buf += next(it2)
            except StopIteration:
                break
            trimmed = buf.lstrip(" \t\r\n")
            if not trimmed:
                continue
            first = trimmed[0]
            if first == "[":
                # root array
                def combined2():
                    yield buf
                    for s in it2:
                        yield s

                for rec in _iter_objects_from_json_array(combined2(), wrapper_results=False):
                    yield rec
                return
            if first == "{":
                # wrapper object; parse results array
                def combined3():
                    yield buf
                    for s in it2:
                        yield s

                for rec in _iter_objects_from_json_array(combined3(), wrapper_results=True):
                    yield rec
                return

            raise RuntimeError("Neočakávaný JSON formát (nezačína '[' ani '{').")

        raise RuntimeError("Prázdny export alebo nenašiel som začiatok JSON.")


# -----------------------------
# Normalization (incl. legal form)
# -----------------------------
_LF_RE = re.compile(r"^\s*(\d+)\s*[\-–—]\s*(.+?)\s*$")


def parse_legal_form_text(legal_form: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
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
        ico = it.get("identifier") or it.get("ico")
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

    # Legal form
    legal_form: Optional[str] = None
    legal_form_code: Optional[str] = None
    legal_form_name: Optional[str] = None

    lf_arr = it.get("legalForms") or []
    lf = _pick_current(lf_arr)
    if isinstance(lf, dict):
        val = lf.get("value")
        # expected: {"code": "...", "value": "..."} or sometimes string
        if isinstance(val, dict):
            if val.get("code") is not None:
                legal_form_code = str(val.get("code")).strip() or None
            if val.get("value") is not None:
                legal_form_name = str(val.get("value")).strip() or None
        elif val is not None:
            legal_form = str(val).strip() or None

    # Keep legacy text column consistent
    if not legal_form and (legal_form_code or legal_form_name):
        if legal_form_code and legal_form_name:
            legal_form = f"{legal_form_code} - {legal_form_name}"
        else:
            legal_form = legal_form_name or legal_form_code

    # Derive code/name from legacy "XXX - Name"
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
        address = a.get("formattedAddress") or a.get("formatedAddress")
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


# -----------------------------
# Backfill
# -----------------------------
def backfill_legal_form_fields(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(SQL_BACKFILL_FROM_LEGAL_FORM)
        updated = cur.rowcount if cur.rowcount is not None else 0
    conn.commit()
    return int(updated) if updated and updated > 0 else 0


# -----------------------------
# Apply batch (streaming)
# -----------------------------
def apply_batch_to_db(
    key: str,
    conn: psycopg.Connection,
    *,
    commit_every: int = 10_000,
    log_every: int = 50_000,
) -> int:
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


# -----------------------------
# Orchestration
# -----------------------------
def run_full_sync(
    *,
    apply_daily: bool = True,
    max_daily: Optional[int] = None,
    reset_init: bool = False,
    reset_daily: bool = False,
    reset_all: bool = False,
    backfill_missing_legal_form_fields: bool = True,
) -> None:
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

        if backfill_missing_legal_form_fields:
            updated = backfill_legal_form_fields(conn)
            if updated:
                log.info("Backfilled legal_form_code/legal_form_name for %d existing org rows.", updated)

        st = conn.execute(SQL_GET_STATE).fetchone() or {"last_init_key": None, "last_daily_key": None}
        last_init_key = st["last_init_key"]
        last_daily_key = st["last_daily_key"]

        # INIT selection
        objs_init = list_objects(PREFIX_INIT)
        pinned_date = _init_date_from_key(last_init_key) if last_init_key else None
        init_date_str, init_keys = init_keys_for_date(objs_init, pinned_date=pinned_date)
        init_date = datetime.strptime(init_date_str, "%Y-%m-%d")

        # Continue INIT from last_init_key
        init_start = 0
        if last_init_key and last_init_key in init_keys:
            init_start = init_keys.index(last_init_key) + 1

        if init_start < len(init_keys):
            for key in init_keys[init_start:]:
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

        daily_start = 0
        if last_daily_key and last_daily_key in keys_daily:
            daily_start = keys_daily.index(last_daily_key) + 1

        applied = 0
        for key in keys_daily[daily_start:]:
            log.info("Downloading DAILY %s", key)
            apply_batch_to_db(key, conn)
            conn.execute(SQL_SET_DAILY, (key,))
            conn.commit()

            applied += 1
            if max_daily is not None and applied >= max_daily:
                break
