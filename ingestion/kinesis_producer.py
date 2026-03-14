"""
kinesis_producer.py
Streams log events to AWS Kinesis Data Streams in real time.
"""

import os
import json
import time
import argparse
import boto3
import uuid
from data_simulator.generate_logs import generate_log_event, generate_user_pool


def get_kinesis_client():
    return boto3.client(
        "kinesis",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )


def stream_logs(stream_name: str, events_per_second: int = 10, duration_seconds: int = 60):
    """
    Continuously streams log events to Kinesis.
    Args:
        stream_name:       Kinesis stream name
        events_per_second: Throughput rate
        duration_seconds:  How long to stream (0 = infinite)
    """
    client = get_kinesis_client()
    user_pool = generate_user_pool(200)
    total_sent = 0
    start_time = time.time()

    print(f"🚀 Streaming to Kinesis stream: {stream_name} at {events_per_second} events/sec")

    while True:
        batch = []
        for _ in range(events_per_second):
            event = generate_log_event(user_pool)
            batch.append({
                "Data": json.dumps(event).encode("utf-8"),
                "PartitionKey": event["user_id"]    # partition by user for ordering
            })

        # Kinesis put_records supports up to 500 records per call
        response = client.put_records(StreamName=stream_name, Records=batch)

        failed = response.get("FailedRecordCount", 0)
        total_sent += len(batch) - failed

        if failed > 0:
            print(f"⚠️  {failed} records failed to send")

        print(f"✅ Sent {len(batch) - failed} events | Total: {total_sent}")

        elapsed = time.time() - start_time
        if duration_seconds > 0 and elapsed >= duration_seconds:
            print(f"\n🏁 Streaming complete. Total events sent: {total_sent}")
            break

        time.sleep(1)


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kinesis Log Producer")
    parser.add_argument("--stream",   type=str, required=True, help="Kinesis stream name")
    parser.add_argument("--rate",     type=int, default=10,    help="Events per second")
    parser.add_argument("--duration", type=int, default=60,    help="Duration in seconds (0=infinite)")
    args = parser.parse_args()

    stream_logs(
        stream_name=args.stream,
        events_per_second=args.rate,
        duration_seconds=args.duration
    )
