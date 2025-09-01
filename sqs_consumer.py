import os, json, time, boto3, psycopg2
from psycopg2.extras import execute_values

QURL   = os.getenv("SQS_URL")                 # https://sqs.eu-north-1.amazonaws.com/ACCT/customer-queue.fifo
DB_DSN = os.getenv("DB_DSN")                  # secret-injected plain DSN

sqs = boto3.client("sqs")

def to_rows(msgs):
    rows = []
    for m in msgs:
        # m["Body"] is SNS envelope JSON (because RawMessageDelivery = false)
        env = json.loads(m["Body"])
        seq = int(env["SequenceNumber"])      # monotonic per MessageGroupId
        body = json.loads(env["Message"])     # our CDC payload from publisher
        rows.append((
            body["customer_id"],
            seq,
            body["lsn"],
            body["commit_ts"],
            f"{body['schema']}.{body['table']}",
            body["kind"],
            json.dumps(body)
        ))
    return rows

def main():
    conn = psycopg2.connect(DB_DSN)
    conn.autocommit = False
    try:
        while True:
            resp = sqs.receive_message(
                QueueUrl=QURL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,       # long poll
                VisibilityTimeout=60      # tune to your processing time
            )
            msgs = resp.get("Messages", [])
            if not msgs:
                continue

            rows = to_rows(msgs)
            with conn.cursor() as cur:
                execute_values(cur, """
                  INSERT INTO target.cdc_events
                  (customer_id, sns_sequence, lsn, commit_ts, table_name, op, payload)
                  VALUES %s
                """, rows)
            conn.commit()

            # delete in batch
            entries = [{"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]} for m in msgs]
            sqs.delete_message_batch(QueueUrl=QURL, Entries=entries)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
