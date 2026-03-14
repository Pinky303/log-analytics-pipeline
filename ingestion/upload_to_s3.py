"""
upload_to_s3.py
Uploads generated log files to the S3 raw zone with date partitioning.
"""

import os
import json
import argparse
import boto3
from datetime import datetime
from pathlib import Path


def upload_logs_to_s3(
    local_file: str,
    bucket: str,
    prefix: str = "raw/logs"
) -> None:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )

    now = datetime.utcnow()
    # Hive-style partitioning: year=YYYY/month=MM/day=DD/
    partition_path = f"{prefix}/year={now.year}/month={now.month:02d}/day={now.day:02d}"
    filename = Path(local_file).name
    s3_key = f"{partition_path}/{filename}"

    print(f"⬆️  Uploading {local_file} → s3://{bucket}/{s3_key}")
    s3.upload_file(local_file, bucket, s3_key)
    print(f"✅ Upload complete: s3://{bucket}/{s3_key}")


# ── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload logs to S3")
    parser.add_argument("--file",   type=str, required=True,  help="Local log file path")
    parser.add_argument("--bucket", type=str, required=True,  help="Target S3 bucket name")
    parser.add_argument("--prefix", type=str, default="raw/logs", help="S3 key prefix")
    args = parser.parse_args()

    upload_logs_to_s3(
        local_file=args.file,
        bucket=args.bucket,
        prefix=args.prefix
    )
