#!/usr/bin/env python3
import os
import re
import json
import time
import logging
from typing import List, Tuple, Any, Dict, Optional
from collections import defaultdict

import boto3
import psycopg2

# ----------------------------
# Config via environment
# ----------------------------
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
SQS_ADDR   = os.environ["SQS_URL"]      # SQS URL or ARN
DB_DSN     = os.environ["DB_DSN"]       # Target DB DSN (Aurora writer)

MAX_SQS_BATCH      = int(os.getenv("MAX_SQS_BATCH", "10"))
WAIT_TIME_SEC      = int(os.getenv("WAIT_TIME_SEC", "20"))
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "120"))
DB_LINGER_MS       = int(os.getenv("DB_LINGER_MS", "50"))
RECEIVE_TRIPS_MAX  = int(os.getenv("RECEIVE_TRIPS_MAX", "50"))
SYNC_OFF           = os.getenv("SYNC_OFF", "0") == "1"

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("sqs-consumer")

# ----------------------------
# Helpers
# ----------------------------
def to_queue_url(addr: str) -> str:
    """Accept either a full SQS URL or an ARN and return a URL."""
    if addr.startswith("https://sqs."):
        return addr
    m = re.match(r"arn:aws:sqs:([a-z0-9-]+):(\d+):(.+)", addr)
    if not m:
        raise ValueError(f"Invalid SQS address: {addr}")
    region, acct, name = m.groups()
    return f"https://sqs.{region}.amazonaws.com/{acct}/{name}"

def _kvlist_get(items: List[Dict[str, Any]], key: str) -> Optional[Any]:
    for it in items or []:
        if it.get("name") == key:
            return it.get("value")
    return None

def _names_vals_get(names: List[str], vals: List[Any], key: str) -> Optional[Any]:
    if names and vals and key in names:
        try:
            return vals[names.index(key)]
        except Exception:
            return None
    return None

def extract_customer_id(obj: Dict[str, Any]) -> Optional[str]:
    # wal2json v2 style
    act = obj.get("action")
    if act in ("I", "U", "D"):
        v = _kvlist_get(obj.get("columns") or [], "customer_id")
        if v is not None:
            return str(v)
        v = _kvlist_get(obj.get("pk") or [], "customer_id")
        if v is not None:
            return str(v)
    # wal2json v1 style
    for ch in obj.get("change") or []:
        v = _names_vals_get(ch.get("columnnames") or [], ch.get("columnvalues") or [], "customer_id")
        if v is not None:
            return str(v)
        ok = ch.get("oldkeys") or {}
        v = _names_vals_get(ok.get("keynames") or [], ok.get("keyvalues") or [], "customer_id")
        if v is not None:
            return str(v)
    return None

def parse_table_name(body: Dict[str, Any]) -> str:
    schema_table = body.get("schema_table") or body.get("table_name")
    if schema_table and "." in schema_table:
        return schema_table
    schema = body.get("schema")
    table = body.get("table")
    return f"{schema}.{table}" if schema and table else (schema or table or "unknown")

def parse_op(body: Dict[str, Any]) -> str:
    act = body.get("action")
    if act in ("I", "U", "D"):
        return {"I": "insert", "U": "update", "D": "delete"}[act]
    return body.get("kind") or body.get("op") or body.get("operation") or "unknown"

def parse_commit_ts(body: Dict[str, Any]) -> Optional[str]:
    return body.get("timestamp") or body.get("commit_ts") or body.get("created_at")

def is_row_change(body: Dict[str, Any]) -> bool:
    act = body.get("action")
    if act is not None:
        return act in ("I", "U", "D")
    ch = body.get("change") or []
    return len(ch) > 0

def extract_col(body: Dict[str, Any], name: str) -> Optional[Any]:
    # v2 columns/pk
    v = _kvlist_get(body.get("columns") or [], name)
    if v is not None:
        return v
    v = _kvlist_get(body.get("pk") or [], name)
    if v is not None:
        return v
    # v1 change array
    for ch in body.get("change") or []:
        v = _names_vals_get(ch.get("columnnames") or [], ch.get("columnvalues") or [], name)
        if v is not None:
            return v
        ok = ch.get("oldkeys") or {}
        v = _names_vals_get(ok.get("keynames") or [], ok.get("keyvalues") or [], name)
        if v is not None:
            return v
    return None

def extract_row_id(body: Dict[str, Any], table_name: str) -> Optional[str]:
    """
    Extract the primary key value as row_id (string).
    Handles wal2json v2 (pk[]) and v1 (change[].oldkeys).
    Also falls back to looking in columns[] if pk[] absent.
    """
    # Map canonical PK names per table
    pk_name_map = {
        "poc.transaction":  "tx_id",
        "poc.subscription": "sub_id",
        "poc.invoice":      "inv_id",
        "poc.card":         "card_id",
    }
    pk_hint = pk_name_map.get(table_name)

    # v2: pk[]
    pk_list = body.get("pk") or []
    if isinstance(pk_list, list) and pk_list:
        # If we know the name, prefer it
        if pk_hint:
            v = _kvlist_get(pk_list, pk_hint)
            if v is not None:
                return str(v)
        # Otherwise, if single element, take it
        if len(pk_list) == 1:
            v = pk_list[0].get("value")
            if v is not None:
                return str(v)
        # Or take the first non-null value
        for it in pk_list:
            if "value" in it and it["value"] is not None:
                return str(it["value"])

    # Fallback: check columns[] for the hinted PK name (some plugins include PK in columns)
    if pk_hint:
        v = _kvlist_get(body.get("columns") or [], pk_hint)
        if v is not None:
            return str(v)

    # v1: change[].oldkeys
    for ch in body.get("change") or []:
        ok = ch.get("oldkeys") or {}
        kn = ok.get("keynames") or []
        kv = ok.get("keyvalues") or []
        if pk_hint and pk_hint in kn:
            try:
                return str(kv[kn.index(pk_hint)])
            except Exception:
                pass
        # else if single key, return it
        if len(kv) == 1:
            return str(kv[0])

    return None

def unwrap_message(m: Dict[str, Any]) -> Tuple[Optional[int], Dict[str, Any]]:
    """Return (sns_sequence, body_dict). Handles SNS envelope and raw message."""
    body_text = m.get("Body", "")
    try:
        maybe_env = json.loads(body_text)
    except Exception:
        return None, {}
    # SNS envelope (Notification)
    if isinstance(maybe_env, dict) and maybe_env.get("Type") == "Notification" and "Message" in maybe_env:
        sns_seq = None
        try:
            sns_seq = int(maybe_env.get("SequenceNumber")) if "SequenceNumber" in maybe_env else None
        except Exception:
            sns_seq = None
        try:
            body = json.loads(maybe_env["Message"])
        except Exception:
            body = {"raw": maybe_env.get("Message")}
        return sns_seq, body
    # raw body (already JSON)
    return None, maybe_env if isinstance(maybe_env, dict) else {}

# ----------------------------
# Parse SQS → rows
# ----------------------------
def parse_messages(messages: List[dict]) -> Tuple[List[Tuple], List[dict]]:
    """
    Each tuple = (
        customer_id, row_id, sns_seq, lsn, commit_ts,
        table_name, op, row_seq, update_seq,
        msg_group_id, payload_json
    )
    """
    rows: List[Tuple] = []
    handles: List[dict] = []

    for m in messages:
        try:
            sns_seq, body = unwrap_message(m)
            if not is_row_change(body):
                continue

            table_name = parse_table_name(body)
            op = parse_op(body)
            customer_id = extract_customer_id(body)
            if not customer_id:
                log.warning("Skipping message without customer_id (table=%s op=%s)", table_name, op)
                continue

            commit_ts = parse_commit_ts(body)
            lsn = body.get("lsn") or ""

            # row_seq
            row_seq = None
            try:
                rs = extract_col(body, "row_seq")
                row_seq = int(rs) if rs is not None and str(rs).strip() != "" else None
            except Exception:
                row_seq = None

            # update_seq
            update_seq = None
            try:
                us = extract_col(body, "update_seq")
                update_seq = int(us) if us is not None and str(us).strip() != "" else None
            except Exception:
                update_seq = None

            # robust PK
            row_id = extract_row_id(body, table_name)

            # SQS message group id (from SNS FIFO -> SQS FIFO)
            msg_group_id = m.get("Attributes", {}).get("MessageGroupId")

            payload_json = json.dumps(body, separators=(",", ":"))

            rows.append((
                str(customer_id),
                str(row_id) if row_id is not None else None,
                int(sns_seq) if sns_seq is not None else 0,
                str(lsn),
                commit_ts,
                table_name,
                op,
                row_seq,
                update_seq,
                msg_group_id,
                payload_json
            ))
            handles.append(m)

        except Exception as e:
            log.exception("Failed to parse SQS message: %s", e)

    return rows, handles

# ----------------------------
# DB helpers
# ----------------------------
INSERT_ONE_SQL = """
INSERT INTO target.cdc_events
  (customer_id, row_id, sns_sequence, lsn, commit_ts,
   table_name, op, row_seq, update_seq,
   msg_group_id, payload)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

def ensure_target_columns(conn) -> None:
    """Make sure target.cdc_events has the columns we intend to write (idempotent)."""
    with conn.cursor() as cur:
        # row_id is the only new one we *must* ensure; others likely exist already
        cur.execute("ALTER TABLE target.cdc_events ADD COLUMN IF NOT EXISTS row_id text")
    conn.commit()

def insert_one_and_commit(cur, conn, row: Tuple) -> None:
    if SYNC_OFF:
        cur.execute("SET LOCAL synchronous_commit = off")
    cur.execute(INSERT_ONE_SQL, row)
    conn.commit()

# ----------------------------
# Main loop
# ----------------------------
def main():
    log.info("sqs-consumer starting (region=%s)", AWS_REGION)
    queue_url = to_queue_url(SQS_ADDR)
    log.info("Using queue URL: %s", queue_url)

    sqs = boto3.client("sqs", region_name=AWS_REGION)
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    ensure_target_columns(conn)
    cur = conn.cursor()

    rows: List[Tuple] = []
    handles: List[dict] = []

    def flush_one_by_one():
        """Group by customer_id, order by (row_seq, update_seq), insert each row individually."""
        nonlocal rows, handles, cur, conn, sqs, queue_url
        if not rows:
            return

        pairs = [(rows[i], handles[i]) for i in range(len(rows))]
        grouped: Dict[str, List[Tuple[Tuple, dict]]] = defaultdict(list)
        for row, h in pairs:
            grouped[row[0]].append((row, h))  # row[0] = customer_id

        success_handles: List[dict] = []

        for cust, ph in grouped.items():
            # Sort within customer by row_seq then update_seq (missing -> huge)
            ph.sort(
                key=lambda pair: (
                    pair[0][7] if pair[0][7] is not None else 10**18,  # row_seq at index 7 after reordering?
                    pair[0][8] if pair[0][8] is not None else 10**18   # update_seq at index 8
                )
            )
            # NOTE: indexes:
            # 0=customer_id, 1=row_id, 2=sns_seq, 3=lsn, 4=commit_ts,
            # 5=table_name, 6=op, 7=row_seq, 8=update_seq, 9=msg_group_id, 10=payload

            for row, h in ph:
                try:
                    insert_one_and_commit(cur, conn, row)
                    success_handles.append(h)
                except Exception as e:
                    conn.rollback()
                    log.exception(
                        "Insert failed for customer %s row_id=%s (row_seq=%s update_seq=%s): %s",
                        row[0], row[1], row[7], row[8], e
                    )
                    # Stop processing; leave un-inserted messages on queue for redelivery
                    rows.clear()
                    handles.clear()
                    break
            else:
                # inner loop finished cleanly; continue next group
                continue
            # We broke due to failure; stop outer loop as well
            break

        # delete successfully processed messages (in chunks of 10)
        if success_handles:
            entries = [{"Id": str(i), "ReceiptHandle": h["ReceiptHandle"]} for i, h in enumerate(success_handles)]
            for i in range(0, len(entries), 10):
                chunk = entries[i:i+10]
                try:
                    sqs.delete_message_batch(QueueUrl=queue_url, Entries=chunk)
                except Exception as e:
                    log.exception("delete_message_batch failed for some successful messages: %s", e)

        # Clear buffers (any failed/unprocessed will redeliver)
        rows.clear()
        handles.clear()

    try:
        while True:
            start = time.monotonic()
            trips = 0

            # Fast-drain loop (no wait) to build a small batch for ordering
            while True:
                resp = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=MAX_SQS_BATCH,
                    WaitTimeSeconds=0,
                    VisibilityTimeout=VISIBILITY_TIMEOUT,
                    MessageAttributeNames=["All"],
                    AttributeNames=["All"],
                )
                msgs = resp.get("Messages", [])
                trips += 1

                if msgs:
                    new_rows, new_handles = parse_messages(msgs)
                    if new_rows:
                        rows.extend(new_rows)
                        handles.extend(new_handles)

                # Flush conditions: tiny linger or too many trips (we insert one-by-one anyway)
                if (len(rows) >= 1) and ((time.monotonic() - start) * 1000 >= DB_LINGER_MS):
                    flush_one_by_one()
                    break
                if trips >= RECEIVE_TRIPS_MAX:
                    if rows:
                        flush_one_by_one()
                    break
                if len(rows) >= 2000:
                    flush_one_by_one()
                    break

            # Idle: long poll once
            if not rows and not handles:
                resp = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=MAX_SQS_BATCH,
                    WaitTimeSeconds=WAIT_TIME_SEC,
                    VisibilityTimeout=VISIBILITY_TIMEOUT,
                    MessageAttributeNames=["All"],
                    AttributeNames=["All"],
                )
                msgs = resp.get("Messages", [])
                if msgs:
                    new_rows, new_handles = parse_messages(msgs)
                    if new_rows:
                        rows.extend(new_rows)
                        handles.extend(new_handles)
                        flush_one_by_one()

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
