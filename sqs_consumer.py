#!/usr/bin/env python3
import os
import re
import json
import time
import logging
from typing import List, Tuple, Any, Dict, Optional, DefaultDict
from collections import defaultdict

import boto3
import psycopg2
from psycopg2.extras import execute_values

# ----------------------------
# Config via environment
# ----------------------------
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
SQS_ADDR = os.environ["SQS_URL"]
DB_DSN = os.environ["DB_DSN"]

MAX_SQS_BATCH      = int(os.getenv("MAX_SQS_BATCH", "10"))
WAIT_TIME_SEC      = int(os.getenv("WAIT_TIME_SEC", "20"))
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "120"))
DB_BATCH_MAX       = int(os.getenv("DB_BATCH_MAX", "5000"))
DB_LINGER_MS       = int(os.getenv("DB_LINGER_MS", "50"))
RECEIVE_TRIPS_MAX  = int(os.getenv("RECEIVE_TRIPS_MAX", "50"))
SYNC_OFF           = os.getenv("SYNC_OFF", "0") == "1"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("sqs-consumer")

# ----------------------------
# Helpers (same as before) ...
# ----------------------------
# ... keep your existing helper functions (unwrap_message, extract_customer_id, etc.) ...

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
# Main loop (with per-customer ordering)
# ----------------------------
def main():
    log.info("sqs-consumer starting (region=%s)", AWS_REGION)
    queue_url = to_queue_url(SQS_ADDR)
    sqs = boto3.client("sqs", region_name=AWS_REGION)

    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False

    rows: List[Tuple] = []
    handles: List[dict] = []

    def flush():
        if not rows:
            return
        ### MOD: group by customer_id and sort by row_seq
        grouped: DefaultDict[str, List[Tuple]] = defaultdict(list)
        for r in rows:
            customer_id = r[0]
            grouped[customer_id].append(r)

        ordered_rows: List[Tuple] = []
        for cust, cust_rows in grouped.items():
            # Sort by row_seq if present, else fallback to sns_sequence
            cust_rows.sort(key=lambda x: (x[6] if x[6] is not None else 0))
            ordered_rows.extend(cust_rows)

        n = len(ordered_rows)
        t0 = time.monotonic()
        db_insert(conn, ordered_rows)

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
        log.info("flushed %d rows (grouped+sorted) in %.1f ms", n, dt)

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
