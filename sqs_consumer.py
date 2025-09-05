#!/usr/bin/env python3
import os
import re
import json
import time
import logging
from typing import List, Tuple, Any, Dict, Optional

import boto3
import psycopg2
from psycopg2.extras import execute_values

# ----------------------------
# Config via environment
# ----------------------------
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")

# IMPORTANT: Provide SQS_URL as the *queue URL* (recommended). If you give an ARN,
# this script will convert ARN -> URL automatically.
SQS_ADDR = os.environ["SQS_URL"]

# Target Aurora DSN provided via ECS "secrets" (ValueFrom)
DB_DSN = os.environ["DB_DSN"]

# Tuning knobs
MAX_SQS_BATCH      = int(os.getenv("MAX_SQS_BATCH", "10"))     # SQS receive max per call (<=10)
WAIT_TIME_SEC      = int(os.getenv("WAIT_TIME_SEC", "20"))     # long poll when idle
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "120"))
DB_BATCH_MAX       = int(os.getenv("DB_BATCH_MAX", "5000"))    # rows per DB commit
DB_LINGER_MS       = int(os.getenv("DB_LINGER_MS", "50"))      # flush window
RECEIVE_TRIPS_MAX  = int(os.getenv("RECEIVE_TRIPS_MAX", "50")) # max 0-wait receives before flush
SYNC_OFF           = os.getenv("SYNC_OFF", "0") == "1"         # SET LOCAL synchronous_commit=off

# CloudWatch-friendly logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
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
        raise ValueError(f"Invalid SQS address (expect URL or ARN): {addr}")
    region, acct, name = m.groups()
    return f"https://sqs.{region}.amazonaws.com/{acct}/{name}"

# ---- wal2json helpers (v2 action frames + v1 change arrays) ----
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

def extract_customer_id_v2(obj: Dict[str, Any]) -> Optional[str]:
    act = obj.get("action")
    if act not in ("I", "U", "D"):
        return None
    v = _kvlist_get(obj.get("columns") or [], "customer_id")
    if v is not None:
        return str(v)
    v = _kvlist_get(obj.get("pk") or [], "customer_id")
    if v is not None:
        return str(v)
    return None

def extract_customer_id_v1(obj: Dict[str, Any]) -> Optional[str]:
    for ch in obj.get("change") or []:
        v = _names_vals_get(ch.get("columnnames") or [], ch.get("columnvalues") or [], "customer_id")
        if v is not None:
            return str(v)
        ok = ch.get("oldkeys") or {}
        v = _names_vals_get(ok.get("keynames") or [], ok.get("keyvalues") or [], "customer_id")
        if v is not None:
            return str(v)
    return None

def extract_customer_id(obj: Dict[str, Any]) -> Optional[str]:
    v = extract_customer_id_v2(obj)
    if v:
        return v
    return extract_customer_id_v1(obj)

def parse_table_name(body: Dict[str, Any]) -> str:
    schema_table = body.get("schema_table") or body.get("table_name")
    if schema_table and "." in schema_table:
        return schema_table
    schema = body.get("schema")
    table = body.get("table")
    return f"{schema}.{table}" if schema and table else (schema or table or "unknown")

def parse_op(body: Dict[str, Any]) -> str:
    act = body.get("action")  # v2
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

# Generic column extraction (for row_seq, etc.)
def extract_col_v2(obj: Dict[str, Any], name: str) -> Optional[Any]:
    act = obj.get("action")
    if act in ("I", "U", "D"):
        v = _kvlist_get(obj.get("columns") or [], name)
        if v is not None:
            return v
        v = _kvlist_get(obj.get("pk") or [], name)
        if v is not None:
            return v
    return None

def extract_col_v1(obj: Dict[str, Any], name: str) -> Optional[Any]:
    for ch in obj.get("change") or []:
        v = _names_vals_get(ch.get("columnnames") or [], ch.get("columnvalues") or [], name)
        if v is not None:
            return v
        ok = ch.get("oldkeys") or {}
        v = _names_vals_get(ok.get("keynames") or [], ok.get("keyvalues") or [], name)
        if v is not None:
            return v
    return None

def extract_col(obj: Dict[str, Any], name: str) -> Optional[Any]:
    v = extract_col_v2(obj, name)
    if v is not None:
        return v
    return extract_col_v1(obj, name)

def unwrap_message(m: Dict[str, Any]) -> Tuple[Optional[int], Dict[str, Any]]:
    """
    Returns (sns_sequence, body_dict).
    Supports:
      - SNS envelope (standard for SNS->SQS)
      - Raw delivery (Body is already the published JSON)
    """
    body_text = m.get("Body", "")
    try:
        maybe_env = json.loads(body_text)
    except Exception:
        return None, {}

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

    return None, maybe_env if isinstance(maybe_env, dict) else {}

def parse_messages(messages: List[dict]) -> Tuple[List[Tuple], List[dict]]:
    """
    Convert a batch of SQS messages (potentially SNS envelopes) into DB rows for target.cdc_events.
    Returns:
      rows: list of tuples matching INSERT order
      handles: the original SQS messages (for delete after commit)
    """
    rows: List[Tuple] = []
    handles: List[dict] = []

    for m in messages:
        try:
            sns_seq, body = unwrap_message(m)

            # Only process row-change frames (I/U/D for v2; non-empty change[] for v1)
            if not is_row_change(body):
                continue

            table_name = parse_table_name(body)
            op = parse_op(body)
            customer_id = extract_customer_id(body)
            if not customer_id:
                # We key order by MessageGroupId=customer_id; skip anything we can't attribute
                log.warning("Skipping message without customer_id (table=%s op=%s)", table_name, op)
                continue

            commit_ts = parse_commit_ts(body)
            lsn = body.get("lsn") or ""  # optional

            # row_seq (int) if present
            row_seq_val = extract_col(body, "row_seq")
            try:
                row_seq = int(row_seq_val) if row_seq_val is not None and str(row_seq_val).strip() != "" else None
            except Exception:
                row_seq = None

            # Capture the actual SQS MessageGroupId that delivered this message
            msg_group_id = None
            try:
                msg_group_id = m.get("Attributes", {}).get("MessageGroupId")
            except Exception:
                msg_group_id = None

            payload_json = json.dumps(body, separators=(",", ":"))

            rows.append((
                str(customer_id),
                sns_seq if sns_seq is not None else 0,
                str(lsn),
                commit_ts,
                table_name,
                op,
                row_seq,
                msg_group_id,
                payload_json
            ))
            handles.append(m)

        except Exception as e:
            log.exception("Failed to parse SQS message; leaving it on the queue. Error: %s", e)

    return rows, handles

# ----------------------------
# DB
# ----------------------------
INSERT_SQL = """
INSERT INTO target.cdc_events
  (customer_id, sns_sequence, lsn, commit_ts, table_name, op, row_seq, msg_group_id, payload)
VALUES %s
"""

def db_insert(conn, rows: List[Tuple]):
    """Batch insert rows into target.cdc_events using execute_values."""
    if not rows:
        return
    with conn.cursor() as cur:
        if SYNC_OFF:
            cur.execute("SET LOCAL synchronous_commit = off")
        execute_values(cur, INSERT_SQL, rows, page_size=min(DB_BATCH_MAX, len(rows)))
    conn.commit()

# ----------------------------
# Main loop (batch + short linger, no reordering)
# ----------------------------
def main():
    log.info("sqs-consumer starting (region=%s)", AWS_REGION)

    queue_url = to_queue_url(SQS_ADDR)
    log.info("Using queue URL: %s", queue_url)

    sqs = boto3.client("sqs", region_name=AWS_REGION)

    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False

    rows: List[Tuple] = []
    handles: List[dict] = []

    def flush():
        if not rows:
            return
        n = len(rows)
        t0 = time.monotonic()
        db_insert(conn, rows)
        # delete SQS in chunks of 10
        entries = [{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(handles)]
        for i in range(0, len(entries), 10):
            try:
                sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries[i:i+10])
            except Exception as e:
                log.exception("delete_message_batch failed; will retry on redelivery. Err: %s", e)
        rows.clear()
        handles.clear()
        dt = (time.monotonic() - t0) * 1000.0
        log.info("flushed %d rows in %.1f ms (DB_BATCH_MAX=%d, LINGER=%d ms)", n, dt, DB_BATCH_MAX, DB_LINGER_MS)

    try:
        while True:
            start = time.monotonic()
            trips = 0

            # Fast-drain loop: build a big batch quickly (no wait)
            while True:
                resp = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=MAX_SQS_BATCH,
                    WaitTimeSeconds=0,  # no wait while draining
                    VisibilityTimeout=VISIBILITY_TIMEOUT,
                    MessageAttributeNames=["All"],
                    AttributeNames=["All"],
                )
                msgs = resp.get("Messages", [])
                trips += 1

                if msgs:
                    new_rows, new_handles = parse_messages(msgs)
                    rows.extend(new_rows)      # keep delivery order (per-group FIFO)
                    handles.extend(new_handles)

                # Flush conditions: size, linger, or too many receive trips
                if len(rows) >= DB_BATCH_MAX:
                    flush()
                    break
                if (time.monotonic() - start) * 1000 >= DB_LINGER_MS:
                    if rows:
                        flush()
                    break
                if trips >= RECEIVE_TRIPS_MAX:
                    if rows:
                        flush()
                    break

            # If nothing drained, fall back to long poll when idle
            if not rows and not handles:
                resp = sqs.receive_message(
                    QueueUrl=queue_url,
                    MaxNumberOfMessages=MAX_SQS_BATCH,
                    WaitTimeSeconds=WAIT_TIME_SEC,  # long poll only when idle
                    VisibilityTimeout=VISIBILITY_TIMEOUT,
                    MessageAttributeNames=["All"],
                    AttributeNames=["All"],
                )
                msgs = resp.get("Messages", [])
                if msgs:
                    new_rows, new_handles = parse_messages(msgs)
                    rows.extend(new_rows)
                    handles.extend(new_handles)
                    flush()

    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
