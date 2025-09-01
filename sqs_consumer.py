#!/usr/bin/env python3
import os
import re
import json
import time
import logging
from typing import List, Tuple

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
DB_BATCH_MAX = int(os.getenv("DB_BATCH_MAX", "500"))            # how many rows per insert_values commit

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

def parse_messages(messages: List[dict]) -> Tuple[List[Tuple], List[dict]]:
    """
    Convert a batch of SQS messages (SNS envelopes) into DB rows for target.cdc_events.
    Returns:
      rows: list of tuples matching INSERT order
      handles: the original SQS messages (for delete after commit)
    """
    rows = []
    handles = []

    for m in messages:
        try:
            env = json.loads(m["Body"])  # SNS envelope (RawMessageDelivery=false)
            # SQS FIFO preserves per-group order; we still keep SNS SequenceNumber for verification.
            seq = int(env["SequenceNumber"])
            body = json.loads(env["Message"])  # actual event body (outbox or wal2json-like)

            # Accept both outbox and wal2json shapes -------------------------
            # table name
            schema_table = body.get("schema_table") or body.get("table_name")
            if schema_table and "." in schema_table:
                table_name = schema_table
            else:
                # wal2json style
                schema = body.get("schema")
                table = body.get("table")
                table_name = f"{schema}.{table}" if schema and table else (schema or table or "unknown")

            # operation
            op = body.get("kind") or body.get("op") or body.get("operation") or "unknown"

            # payload
            payload = body.get("payload") or body.get("row") or body

            # customer id (must exist for our MessageGroupId contract)
            customer_id = body.get("customer_id")
            if not customer_id and isinstance(payload, dict):
                customer_id = payload.get("customer_id")
            if not customer_id:
                # Skip messages we can't attribute to a customer stream
                log.warning("Skipping message without customer_id (table=%s op=%s)", table_name, op)
                continue

            # commit timestamp & lsn (optional with outbox)
            commit_ts = body.get("created_at") or body.get("commit_ts")
            lsn = body.get("lsn") or ""

            rows.append((
                str(customer_id),
                seq,
                str(lsn),
                commit_ts,             # can be None; target column should allow NULL
                table_name,
                op,
                json.dumps(payload, separators=(",", ":"))
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
            # Receive a batch
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
                # no messages in this poll window
                continue

            rows, handles = parse_messages(messages)
            if not rows:
                # Nothing parseable; do not delete (let them retry / go to DLQ)
                log.warning("Received %d messages but 0 parseable rows; check payload format.", len(messages))
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

            # Delete successfully processed messages
            entries = []
            for i, m in enumerate(handles):
                entries.append({
                    "Id": str(i),
                    "ReceiptHandle": m["ReceiptHandle"]
                })

            # Delete in chunks of 10 (SQS limit)
            for i in range(0, len(entries), 10):
                chunk = entries[i:i+10]
                try:
                    sqs.delete_message_batch(QueueUrl=queue_url, Entries=chunk)
                except Exception as e:
                    # Log and continue; worst case those messages reappear after visibility timeout
                    log.exception("delete_message_batch failed (will retry on next delivery). Error: %s", e)

    finally:
        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
