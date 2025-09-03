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

# Tuning knobs (sensible defaults)
MAX_SQS_BATCH = int(os.getenv("MAX_SQS_BATCH", "10"))           # SQS receive max per call (<=10)
WAIT_TIME_SEC = int(os.getenv("WAIT_TIME_SEC", "20"))           # SQS long poll
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "60")) # keep > DB insert time
DB_BATCH_MAX = int(os.getenv("DB_BATCH_MAX", "500"))            # how many rows per execute_values commit

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
            i = names.index(key)
            return vals[i]
        except Exception:
            return None
    return None

def extract_customer_id_v2(obj: Dict[str, Any]) -> Optional[str]:
    """wal2json v2 'action' frames (I/U/D) with columns[] / pk[]."""
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
    """wal2json v1 'change' array with columnnames/columnvalues or oldkeys."""
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
    # Normalize operation for storage
    act = body.get("action")  # v2
    if act in ("I", "U", "D"):
        return {"I": "insert", "U": "update", "D": "delete"}[act]
    # fallback to v1-ish hints
    return body.get("kind") or body.get("op") or body.get("operation") or "unknown"

def parse_commit_ts(body: Dict[str, Any]) -> Optional[str]:
    return body.get("timestamp") or body.get("commit_ts") or body.get("created_at")

def is_row_change(body: Dict[str, Any]) -> bool:
    """True only for I/U/D (v2) or when v1 change[] exists and non-empty."""
    act = body.get("action")
    if act is not None:
        return act in ("I", "U", "D")
    ch = body.get("change") or []
    return len(ch) > 0

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
        # Not JSON? Treat as empty
        return None, {}

    # Standard SNS envelope?
    if isinstance(maybe_env, dict) and maybe_env.get("Type") == "Notification" and "Message" in maybe_env:
        sns_seq = None
        # FIFO SNS adds SequenceNumber
        try:
            sns_seq = int(maybe_env.get("SequenceNumber")) if "SequenceNumber" in maybe_env else None
        except Exception:
            sns_seq = None
        try:
            body = json.loads(maybe_env["Message"])
        except Exception:
            body = {"raw": maybe_env.get("Message")}
        return sns_seq, body

    # Raw delivery case: Body is the actual message
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
                # Skip quietly but keep slot/queue flowing
                continue

            # Extract essentials
            table_name = parse_table_name(body)
            op = parse_op(body)
            customer_id = extract_customer_id(body)
            if not customer_id:
                # We key order by MessageGroupId=customer_id; skip anything we can't attribute
                log.warning("Skipping message without customer_id (table=%s op=%s)", table_name, op)
                continue

            commit_ts = parse_commit_ts(body)
            lsn = body.get("lsn") or ""  # optional
            payload_json = json.dumps(body, separators=(",", ":"))

            rows.append((
                str(customer_id),
                sns_seq if sns_seq is not None else 0,  # keep schema unchanged
                str(lsn),
                commit_ts,
                table_name,
                op,
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
  (customer_id, sns_sequence, lsn, commit_ts, table_name, op, payload)
VALUES %s
"""

def db_insert(conn, rows: List[Tuple]):
    """Batch insert rows into target.cdc_events using execute_values."""
    if not rows:
        return
    with conn.cursor() as cur:
        execute_values(cur, INSERT_SQL, rows, page_size=min(DB_BATCH_MAX, len(rows)))
    conn.commit()

# ----------------------------
# Main loop
# ----------------------------
def main():
    log.info("sqs-consumer starting (region=%s)", AWS_REGION)

    # Resolve queue URL (handles ARN or URL)
    queue_url = to_queue_url(SQS_ADDR)
    log.info("Using queue URL: %s", queue_url)

    # boto3 clients
    sqs = boto3.client("sqs", region_name=AWS_REGION)

    # DB connection (persistent)
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False

    try:
        while True:
            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=MAX_SQS_BATCH,
                WaitTimeSeconds=WAIT_TIME_SEC,
                VisibilityTimeout=VISIBILITY_TIMEOUT,
                MessageAttributeNames=["All"],
                AttributeNames=["All"],
            )

            messages = resp.get("Messages", [])
            if not messages:
                continue

            rows, handles = parse_messages(messages)
            if not rows:
                # nothing we could parse/use; don't delete
                log.debug("Polled %d messages, 0 usable row-change frames", len(messages))
                continue

            # Insert into target; only delete SQS if commit succeeds
            try:
                db_insert(conn, rows)
            except Exception as e:
                log.exception("DB insert failed; NOT deleting SQS messages. Error: %s", e)
                # Let visibility timeout expire → messages will be retried / land in DLQ
                conn.rollback()
                time.sleep(1)
                continue

            # Delete successfully processed messages (in chunks of 10)
            entries = [{"Id": str(i), "ReceiptHandle": m["ReceiptHandle"]} for i, m in enumerate(handles)]
            for i in range(0, len(entries), 10):
                chunk = entries[i:i+10]
                try:
                    sqs.delete_message_batch(QueueUrl=queue_url, Entries=chunk)
                except Exception as e:
                    log.exception("delete_message_batch failed (will retry on next delivery). Error: %s", e)

    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
