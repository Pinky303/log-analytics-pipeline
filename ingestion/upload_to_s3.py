"""
upload_to_s3.py
Uploads generated log files to S3 raw zone with Hive-style date partitioning.

Usage:
    python ingestion/upload_to_s3.py --file logs.json --bucket pinky-log-pipeline-raw
"""

import os
import argparse
import boto3
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def upload_logs_to_s3(local_file: str, bucket: str, prefix: str = "raw/logs") -> None:
    s3  = boto3.client(
        "s3",
        aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name           = os.getenv("AWS_REGION", "ap-south-1"),
    )
    now       = datetime.utcnow()
    s3_key    = f"{prefix}/year={now.year}/month={now.month:02d}/day={now.day:02d}/{Path(local_file).name}"
    print(f"⬆️  Uploading {local_file} → s3://{bucket}/{s3_key}")
    s3.upload_file(local_file, bucket, s3_key)
    print(f"✅ Upload complete: s3://{bucket}/{s3_key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file",   required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="raw/logs")
    args = parser.parse_args()
    upload_logs_to_s3(args.file, args.bucket, args.prefix)
