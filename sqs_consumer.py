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
SQS_ADDR   = os.environ["SQS_URL"]   # SQS URL or ARN
DB_DSN     = os.environ["DB_DSN"]    # Target DB DSN (Aurora writer)

# Tuning knobs (these are less critical now since we do single-row inserts)
MAX_SQS_BATCH      = int(os.getenv("MAX_SQS_BATCH", "10"))     # SQS receive max (<=10)
WAIT_TIME_SEC      = int(os.getenv("WAIT_TIME_SEC", "20"))     # long poll when idle
VISIBILITY_TIMEOUT = int(os.getenv("VISIBILITY_TIMEOUT", "120"))
DB_LINGER_MS       = int(os.getenv("DB_LINGER_MS", "50"))      # how long to drain before flushing
RECEIVE_TRIPS_MAX  = int(os.getenv("RECEIVE_TRIPS_MAX", "50"))

SYNC_OFF = os.getenv("SYNC_OFF", "0") == "1"  # if true, set local synchronous_commit = off per insert

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sqs-consumer")

# ----------------------------
# Helper functions
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
    # try v2 columns / pk
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

def parse_messages(messages: List[dict]) -> Tuple[List[Tuple], List[dict]]:
    """
    Parse SQS messages; return:
      - rows: list of tuples in same order as handles
      - handles: list of original message dicts (ReceiptHandle required for delete)
    Each tuple = (customer_id, sns_seq, lsn, commit_ts, table_name, op, row_seq, msg_group_id, payload_json)
    """
    rows: List[Tuple] = []
    handles: List[dict]   = []
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
            rs = extract_col(body, "row_seq")
            try:
                row_seq = int(rs) if rs is not None else None
            except Exception:
                row_seq = None
            msg_group_id = m.get("Attributes", {}).get("MessageGroupId")
            payload_json = json.dumps(body, separators=(",", ":"))
            rows.append((
                str(customer_id),
                int(sns_seq) if sns_seq is not None else 0,
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
            log.exception("Failed to parse SQS message: %s", e)
    return rows, handles

# ----------------------------
# DB insertion helpers
# ----------------------------
INSERT_ONE_SQL = """
INSERT INTO target.cdc_events
  (customer_id, sns_sequence, lsn, commit_ts, table_name, op, row_seq, msg_group_id, payload)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

def insert_one_and_commit(cur, conn, row: Tuple) -> None:
    """Insert single row using existing cursor and commit. Raises on failure."""
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
    cur = conn.cursor()

    # temporary buffers
    rows: List[Tuple] = []
    handles: List[dict] = []

    def flush_one_by_one():
        """Group rows+handles by customer_id, sort by row_seq, insert sequentially and delete only successful messages."""
        nonlocal rows, handles, cur, conn, sqs, queue_url
        if not rows:
            return
        # build list of (row, handle) pairs
        pairs = [(rows[i], handles[i]) for i in range(len(rows))]
        # group by customer_id (row[0])
        grouped: Dict[str, List[Tuple[Tuple, dict]]] = defaultdict(list)
        for row, h in pairs:
            grouped[row[0]].append((row, h))

        success_handles: List[dict] = []

        # process each customer group independently (order enforced inside group)
        for cust, ph in grouped.items():
            # sort by row_seq (index 6). Missing row_seq -> push to end via large key
            ph.sort(key=lambda pair: (pair[0][6] if pair[0][6] is not None else 10**18, pair[0][1] if pair[0][1] is not None else 0))
            for row, h in ph:
                try:
                    insert_one_and_commit(cur, conn, row)
                    success_handles.append(h)
                except Exception as e:
                    conn.rollback()
                    log.exception("Insert failed for customer %s row_seq=%s; stopping group processing. Error: %s", row[0], row[6], e)
                    # stop processing further rows in this flush; leave remaining messages for retry
                    # We continue to next group only after deciding to break/continue; here we break out to outer cleanup
                    # break out of both loops: set flag and return cleanup
                    # We'll return after deleting already-successful messages
                    # (Do not delete messages corresponding to failed inserts)
                    # NOTE: we intentionally stop processing here to let the failed message redeliver.
                    # Return to cleanup stage now
                    # First delete successfully inserted handles
                    # We'll do deletion after loops
                    # So use goto-like semantics by emptying remaining work and going to cleanup
                    rows.clear()
                    handles.clear()
                    # delete success_handles below and return
                    break
            else:
                # inner loop finished normally; continue to next group
                continue
            # if we hit a failure and broke inner loop, break outer loop as well
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

        # clear buffers (unprocessed messages remain on queue)
        rows.clear()
        handles.clear()

    try:
        while True:
            start = time.monotonic()
            trips = 0

            # Drain fast (no wait) to gather a batch of messages
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

                # flush conditions
                if (len(rows) >= 1) and ((time.monotonic() - start) * 1000 >= DB_LINGER_MS):
                    # We flush whatever we have (even 1 row). This keeps latency low.
                    flush_one_by_one()
                    break
                if trips >= RECEIVE_TRIPS_MAX:
                    if rows:
                        flush_one_by_one()
                    break
                # Also if we accumulated a "reasonable" number; keep it small because we do one-by-one
                if len(rows) >= 2000:
                    flush_one_by_one()
                    break

            # If idle (no rows), use a long poll
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
