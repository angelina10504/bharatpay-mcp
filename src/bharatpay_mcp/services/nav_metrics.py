"""Read-only operational metrics from persisted ingestion, job and worker state."""
from __future__ import annotations

from datetime import date, timedelta, timezone

from bharatpay_mcp.services.nav_coordination import LEASE_ID
from bharatpay_mcp.storage import Store, now


def run_metrics(run: dict | None):
    if run is None:
        return None
    keys = ("status", "trigger", "started_at", "finished_at", "duration_seconds", "accepted",
            "rejected_count", "parsed_count", "matched_count", "upserted_count", "modified_count",
            "source_sha256", "source_bytes", "source_date_min", "source_date_max", "error_category",
            "diagnostic", "write_outcome")
    return {"run_id": run["_id"], **{key: run.get(key) for key in keys}}


def observed_time(hello: dict):
    # Command replies may use a different codec from collection documents.
    stamp = hello.get("localTime", now())
    return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)


async def ingestion_metrics(store: Store) -> dict:
    hello = await store.db.command("hello")
    stamp = observed_time(hello)
    latest = await store.db.ingestion_runs.find_one({}, sort=[("created_at", -1), ("_id", -1)])
    completed = await store.db.ingestion_runs.find_one({"status": "completed"}, sort=[("finished_at", -1), ("_id", -1)])
    failed = await store.db.ingestion_runs.find_one({"status": {"$in": ["failed", "interrupted"]}}, sort=[("finished_at", -1)])
    failure_query = {"status": {"$in": ["failed", "interrupted"]}}
    if completed:
        failure_query["finished_at"] = {"$gt": completed["finished_at"]}
    consecutive = await store.db.ingestion_runs.count_documents(failure_query)
    groups = await store.db.ingestion_runs.aggregate([
        {"$group": {"_id": "$status", "count": {"$sum": 1},
                    "accepted": {"$sum": "$accepted"}, "rejected": {"$sum": "$rejected_count"},
                    "matched": {"$sum": "$matched_count"}, "inserted": {"$sum": "$upserted_count"},
                    "modified": {"$sum": "$modified_count"}}}])
    totals = {row.pop("_id"): row async for row in groups}
    jobs = await store.db.batch_jobs.aggregate([
        {"$group": {"_id": "$status", "count": {"$sum": 1}, "reclaims": {"$sum": "$lease_reclaims"}}}])
    job_rows = [row async for row in jobs]
    lease = await store.db.nav_coordination.find_one({"_id": LEASE_ID})
    heartbeat = await store.db.worker_states.find_one({}, sort=[("heartbeat_at", -1)])
    source_date = completed.get("source_date_max") if completed else None
    return {
        "observed_at": stamp,
        "transaction_capable": bool(hello.get("setName") and hello.get("logicalSessionTimeoutMinutes") is not None
                                    and hello.get("maxWireVersion", 0) >= 7),
        "latest_attempt": run_metrics(latest), "last_completed": run_metrics(completed),
        "last_failure": run_metrics(failed), "consecutive_failures": consecutive,
        "last_completed_age_seconds": max(0, (stamp - completed["finished_at"]).total_seconds()) if completed else None,
        "source_freshness_days": (stamp.date() - date.fromisoformat(source_date)).days if source_date else None,
        "run_totals": totals,
        "legacy_runs_without_write_counts": await store.db.ingestion_runs.count_documents({"upserted_count": {"$exists": False}}),
        "jobs": {status: next((row["count"] for row in job_rows if row["_id"] == status), 0)
                 for status in ("queued", "running", "failed", "completed", "completed_with_errors")},
        "job_lease_expired": await store.db.batch_jobs.count_documents({"status": "running", "$expr": {"$lte": ["$lease_until", "$$NOW"]}}),
        "job_lease_reclaims": sum(row["reclaims"] for row in job_rows),
        "workers": {
            "active": await store.db.worker_states.count_documents({"heartbeat_at": {"$gte": stamp - timedelta(seconds=30)}}),
            "last_heartbeat_at": heartbeat["heartbeat_at"] if heartbeat else None},
        "lease": {"owner_run_id": lease.get("owner") if lease else None,
                  "version": lease.get("version") if lease else None,
                  "expires_at": lease.get("lease_until") if lease else None,
                  "expired": bool(lease and lease.get("owner") and lease["lease_until"] <= stamp),
                  "reclaims": lease.get("reclaims", 0) if lease else 0},
    }
