"""Scheduled alert check: exit nonzero with a diagnostic when health thresholds are exceeded.

Reads the same persisted state as GET /api/v1/ingestions/metrics, directly from
MongoDB, so it needs no API session. It never writes. Use cron, a CI schedule or
an orchestrator's health task; no paid alerting service is involved.

Run: MONGO_URL=... python scripts/check_health.py [--fail-on degraded|failed] [--json]
Exit codes: 0 within thresholds, 1 threshold exceeded, 2 database unreachable.
Thresholds: HEALTH_MAX_CONSECUTIVE_FAILURES, HEALTH_MAX_SUCCESS_AGE_HOURS,
HEALTH_MAX_QUEUE_WAIT_SECONDS and HEALTH_EXPECT_AIRFLOW (see docs/operations.md).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from pymongo.errors import PyMongoError

from bharatpay_mcp.services.nav_metrics import SEVERITY, ingestion_metrics
from bharatpay_mcp.storage import Store


async def collect() -> dict:
    store = Store(os.environ["MONGO_URL"])
    try:
        return await ingestion_metrics(store)
    finally:
        await store.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fail-on", choices=["degraded", "failed"], default="degraded")
    parser.add_argument("--json", action="store_true", help="Print the full metrics document")
    args = parser.parse_args()
    try:
        metrics = asyncio.run(collect())
    except PyMongoError:
        print("FAILED database: MongoDB is unreachable; no other component can be assessed")
        return 2
    health = metrics["health"]
    if args.json:
        print(json.dumps(metrics, default=str, indent=2))
    for name, component in health["components"].items():
        print(f"{component['status'].upper():<15} {name}: {component['detail']}")
    threshold = SEVERITY.index(args.fail_on)
    breached = SEVERITY.index(health["status"]) >= threshold
    print(f"overall={health['status']} fail_on={args.fail_on} result={'ALERT' if breached else 'OK'}")
    return 1 if breached else 0


if __name__ == "__main__":
    sys.exit(main())
