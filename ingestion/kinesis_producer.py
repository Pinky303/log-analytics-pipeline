"""
kinesis_producer.py
Streams log events to AWS Kinesis Data Streams in real time.

Usage:
    python ingestion/kinesis_producer.py --stream log-stream --rate 10 --duration 60
"""

import os
import json
import time
import argparse
import boto3
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
sys.path.append(str(Path(__file__).parent.parent))
from data_simulator.generate_logs import generate_log_event, generate_user_pool


def stream_logs(stream_name: str, events_per_second: int = 10, duration_seconds: int = 60):
    client    = boto3.client(
        "kinesis",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name           = os.getenv("AWS_REGION", "ap-south-1"),
    )
    user_pool  = generate_user_pool(200)
    total_sent = 0
    start_time = time.time()
    print(f"🚀 Streaming to: {stream_name} @ {events_per_second} events/sec")

    while True:
        batch = [
            {"Data": json.dumps(generate_log_event(user_pool)).encode("utf-8"),
             "PartitionKey": "logs"}
            for _ in range(events_per_second)
        ]
        response   = client.put_records(StreamName=stream_name, Records=batch)
        failed     = response.get("FailedRecordCount", 0)
        total_sent += len(batch) - failed
        print(f"✅ Sent {len(batch) - failed} | Total: {total_sent}")
        if duration_seconds > 0 and time.time() - start_time >= duration_seconds:
            print(f"\n🏁 Done. Total sent: {total_sent}")
            break
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream",   required=True)
    parser.add_argument("--rate",     type=int, default=10)
    parser.add_argument("--duration", type=int, default=60)
    args = parser.parse_args()
    stream_logs(args.stream, args.rate, args.duration)
