"""
generate_logs.py
Simulates realistic application logs: user events, API calls, errors, latency.

Usage:
    python data_simulator/generate_logs.py --num-events 5000 --output logs.json
"""

import json
import random
import uuid
import argparse
from datetime import datetime, timedelta

ENDPOINTS = [
    "/api/v1/users", "/api/v1/products", "/api/v1/orders",
    "/api/v1/search", "/api/v1/cart", "/api/v1/checkout",
    "/api/v1/auth/login", "/api/v1/auth/logout", "/health"
]
METHODS      = ["GET", "POST", "PUT", "DELETE"]
STATUS_CODES = [200, 200, 200, 200, 201, 400, 401, 403, 404, 500, 503]
USER_AGENTS  = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (Linux; Android 11)",
    "PostmanRuntime/7.32.0"
]
REGIONS  = ["us-east-1", "eu-west-1", "ap-south-1", "us-west-2"]
SERVICES = ["auth-service", "product-service", "order-service", "gateway"]


def random_timestamp(start_hours_ago: int = 24) -> str:
    delta = random.uniform(0, start_hours_ago * 3600)
    ts    = datetime.utcnow() - timedelta(seconds=delta)
    return ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def generate_user_pool(n: int = 200) -> list:
    return [
        {"user_id": f"user_{str(uuid.uuid4())[:8]}", "session_id": str(uuid.uuid4())}
        for _ in range(n)
    ]


def generate_log_event(user_pool: list) -> dict:
    status  = random.choice(STATUS_CODES)
    latency = round(random.lognormvariate(4.5, 0.8), 2)
    if status >= 500:
        latency = round(latency * random.uniform(3, 10), 2)
    return {
        "event_id":            str(uuid.uuid4()),
        "session_id":          random.choice(user_pool)["session_id"],
        "user_id":             random.choice(user_pool)["user_id"],
        "timestamp":           random_timestamp(),
        "service":             random.choice(SERVICES),
        "endpoint":            random.choice(ENDPOINTS),
        "method":              random.choice(METHODS),
        "status_code":         status,
        "latency_ms":          latency,
        "region":              random.choice(REGIONS),
        "user_agent":          random.choice(USER_AGENTS),
        "request_size_bytes":  random.randint(100, 5000),
        "response_size_bytes": random.randint(200, 50000),
        "is_error":            status >= 400,
        "error_message":       f"HTTPError {status}" if status >= 400 else None,
        "trace_id":            str(uuid.uuid4()),
    }


def generate_logs(num_events: int = 1000, output_file: str = "logs.json") -> None:
    user_pool = generate_user_pool(200)
    logs      = [generate_log_event(user_pool) for _ in range(num_events)]
    with open(output_file, "w") as f:
        for log in logs:
            f.write(json.dumps(log) + "\n")
    print(f"✅ Generated {num_events} log events → {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-events", type=int, default=1000)
    parser.add_argument("--output",     type=str, default="logs.json")
    args = parser.parse_args()
    generate_logs(num_events=args.num_events, output_file=args.output)
