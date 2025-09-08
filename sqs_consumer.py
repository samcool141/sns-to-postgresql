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
from psycopg2.extras import execute_values

# ----------------------------
# Config via environment
# ----------------------------
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
SQS_ADDR   = os.environ["SQS_URL"]          # SQS URL or ARN
DB_DSN     = os.environ["DB_DSN"]           # Aurora DSN

MAX_SQS_BATCH      = int(os.getenv("MAX_SQS_BATCH", "10"))     # max per receive (<=10)
WAIT_TIME_SEC      = int(os.getenv("WAIT_TIME_SEC", "20"))     # long poll
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "120"))
DB_BATCH_MAX       = int(os.getenv("DB_BATCH_MAX", "5000"))    # rows per commit
DB_LINGER_MS       = int(os.getenv("DB_LINGER_MS", "50"))      # flush window
RECEIVE_TRIPS_MAX  = int(os.getenv("RECEIVE_TRIPS_MAX", "50")) # max empty receives
SYNC_OFF           = os.getenv("SYNC_OFF", "0") == "1"

# Logging
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
    # v2
    act = obj.get("action")
    if act in ("I", "U", "D"):
        v = _kvlist_get(obj.get("columns") or [], "customer_id")
        if v is not None: return str(v)
        v = _kvlist_get(obj.get("pk") or [], "customer_id")
        if v is not None: return str(v)
    # v1
    for ch in obj.get("change") or []:
        v = _names_vals_get(ch.get("columnnames") or [], ch.get("columnvalues") or [], "customer_id")
        if v is not None: return str(v)
        ok = ch.get("oldkeys") or {}
        v = _names_vals_get(ok.get("keynames") or [], ok.get("keyvalues") or [], "customer_id")
        if v is not None: return str(v)
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
    return len(body.get("change") or []) > 0

def extract_col(body: Dict[str, Any], name: str) -> Optional[Any]:
    v = _kvlist_get(body.get("columns") or [], name)
    if v is not None: return v
    v = _kvlist_get(body.get("pk") or [], name)
    if v is not None: return v
    for ch in body.get("change") or []:
        v = _names_vals_get(ch.get("columnnames") or [], ch.get("columnvalues") or [], name)
        if v is not None: return v
        ok = ch.get("oldkeys") or {}
        v = _names_vals_get(ok.get("keynames") or [], ok.get("keyvalues") or [], name)
        if v is not None: return v
    return None

def unwrap_message(m: Dict[str, Any]) -> Tuple[Optional[int], Dict[str, Any]]:
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
    rows, handles = [], []
    for m in messages:
        try:
            sns_seq, body = unwrap_message(m)
            if not is_row_change(body):
                continue
            table_name  = parse_table_name(body)
            op          = parse_op(body)
            customer_id = extract_customer_id(body)
            if not customer_id:
                continue
            commit_ts = parse_commit_ts(body)
            lsn       = body.get("lsn") or ""
            row_seq   = extract_col(body, "row_seq")
            try:
                row_seq = int(row_seq) if row_seq is not None else None
            except Exception:
                row_seq = None
            msg_group_id = m.get("Attributes", {}).get("MessageGroupId")
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
            log.exception("parse_messages failed: %s", e)
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
    """Group rows by customer_id and sort by row_seq before insert."""
    if not rows:
        return
    grouped = defaultdict(list)
    for r in rows:
        grouped[r[0]].append(r)
    sorted_rows = []
    for cust, rs in grouped.items():
        rs.sort(key=lambda x: (x[6] if x[6] is not None else 0))  # index 6 = row_seq
        sorted_rows.extend(rs)
    with conn.cursor() as cur:
        if SYNC_OFF:
            cur.execute("SET LOCAL synchronous_commit = off")
        execute_values(cur, INSERT_SQL, sorted_rows, page_size=min(DB_BATCH_MAX, len(sorted_rows)))
    conn.commit()

# ----------------------------
# Main loop
# ----------------------------
def main():
    log.info("sqs-consumer starting (region=%s)", AWS_REGION)
    queue_url = to_queue_url(SQS_ADDR)
    log.info("Using queue URL: %s", queue_url)

    sqs  = boto3.client("sqs", region_name=AWS_REGION)
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False

    rows, handles = [], []

    def flush():
        if not rows:
            return
        n = len(rows)
        t0 = time.monotonic()
        db_insert(conn, rows)
        entries = [{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(handles)]
        for i in range(0, len(entries), 10):
            try:
                sqs.delete_message_batch(QueueUrl=queue_url, Entries=entries[i:i+10])
            except Exception as e:
                log.exception("delete_message_batch failed: %s", e)
        rows.clear()
        handles.clear()
        log.info("flushed %d rows in %.1f ms", n, (time.monotonic() - t0) * 1000)

    try:
        while True:
            start = time.monotonic()
            trips = 0
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
                    rows.extend(new_rows)
                    handles.extend(new_handles)
                if len(rows) >= DB_BATCH_MAX or (time.monotonic() - start) * 1000 >= DB_LINGER_MS or trips >= RECEIVE_TRIPS_MAX:
                    if rows:
                        flush()
                    break
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
