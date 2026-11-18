"""Read-only operational metrics from persisted ingestion, job and worker state.

Health is derived from what the database recorded, never from a process merely
existing. Five states are kept distinct: `healthy`, `degraded` (working, needs
attention), `failed`, `not_configured` (optional component never seen and not
expected) and `never_executed` (expected, but no evidence it has ever run).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import math
import os

from bharatpay_mcp.services.nav_coordination import LEASE_ID
from bharatpay_mcp.storage import Store, now

TRIGGERS = ("airflow", "cli", "worker")
HEARTBEAT_SECONDS = 30  # presence renews every ten seconds
POLL_SECONDS = 60  # an idle worker polls for claims every two seconds
SEVERITY = ("healthy", "not_configured", "never_executed", "degraded", "failed")


@dataclass(frozen=True)
class HealthLimits:
    # One scheduled Airflow run is three attempts: the first plus two retries.
    max_consecutive_failures: int = 3
    # The weekday 19:00 UTC schedule leaves 72 hours from Friday to Monday;
    # 96 allows one market holiday before the gap is reported.
    max_success_age_hours: float = 96
    # Idle workers poll every two seconds; contention defers for ten.
    max_queue_wait_seconds: float = 300
    expect_airflow: bool = False

    def __post_init__(self):
        values = (self.max_consecutive_failures, self.max_success_age_hours, self.max_queue_wait_seconds)
        if any(not math.isfinite(value) or value <= 0 for value in values):
            raise ValueError("Health limits must be positive")

    @classmethod
    def from_env(cls):
        return cls(int(os.getenv("HEALTH_MAX_CONSECUTIVE_FAILURES", "3")),
                   float(os.getenv("HEALTH_MAX_SUCCESS_AGE_HOURS", "96")),
                   float(os.getenv("HEALTH_MAX_QUEUE_WAIT_SECONDS", "300")),
                   os.getenv("HEALTH_EXPECT_AIRFLOW", "").lower() in {"1", "true", "yes"})


def run_metrics(run: dict | None):
    if run is None:
        return None
    keys = ("status", "trigger", "source_kind", "started_at", "finished_at", "duration_seconds", "accepted",
            "rejected_count", "parsed_count", "matched_count", "upserted_count", "modified_count",
            "source_sha256", "source_bytes", "source_date_min", "source_date_max", "error_category",
            "diagnostic", "write_outcome")
    return {"run_id": run["_id"], **{key: run.get(key) for key in keys}}


def observed_time(hello: dict):
    # Command replies may use a different codec from collection documents.
    stamp = hello.get("localTime", now())
    return stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)


def age_seconds(stamp: datetime, earlier: datetime | None):
    return max(0, (stamp - earlier).total_seconds()) if earlier else None


def worker_state(doc: dict, stamp: datetime) -> str:
    """Classify one presence document without I/O.

    A heartbeat proves the process and its database link. Only a recent claim
    poll or an in-flight job proves the work loop itself is still advancing.
    """
    if doc["heartbeat_at"] < stamp - timedelta(seconds=HEARTBEAT_SECONDS):
        return "stale"
    if doc.get("current_job_id"):
        return "busy"
    polled = doc.get("polled_at")
    return "polling" if polled and polled >= stamp - timedelta(seconds=POLL_SECONDS) else "stalled"


def assess(metrics: dict, limits: HealthLimits) -> dict:
    """Turn collected metrics into per-component states, without I/O."""
    def item(status: str, detail: str):
        return {"status": status, "detail": detail}

    def ingestion(latest, completed, age, failures, missing):
        if latest is None:
            return missing
        category = latest.get("error_category") or "unknown"
        if completed is None:
            return (item("failed", f"No completed run; latest attempt ended with {category}") if failures
                    else item("degraded", f"No completed run yet; latest attempt is {latest['status']}"))
        if failures >= limits.max_consecutive_failures:
            return item("failed", f"{failures} failed attempts since the last success; latest: {category}")
        if failures:
            return item("degraded", f"{failures} failed attempt(s) since the last success; latest: {category}")
        if age > limits.max_success_age_hours * 3600:
            return item("degraded", f"Last success was {age / 3600:.1f} hours ago; limit is {limits.max_success_age_hours:g}")
        return item("healthy", f"Last success {age / 3600:.1f} hours ago")

    workers, queue = metrics["workers"], metrics["queue"]
    components = {"database": item("healthy", "Replica set; transactions available") if metrics["transaction_capable"]
                  else item("degraded", "Connected, but not a replica set: NAV ingestion fails closed")}
    # Jobs cannot run without a worker, and presence documents expire after a
    # day, so a missing heartbeat is a failure rather than "never executed".
    if workers["last_heartbeat_at"] is None:
        components["worker"] = item("failed", "No worker heartbeat on record within the last day")
    elif not workers["active"]:
        components["worker"] = item("failed", f"No worker heartbeat within {HEARTBEAT_SECONDS} seconds")
    elif not workers["polling"] + workers["busy"]:
        components["worker"] = item("degraded", "Workers hold a heartbeat, but none is polling or processing")
    else:
        components["worker"] = item("healthy", f"{workers['polling']} polling, {workers['busy']} busy")
    components["nav_ingestion"] = ingestion(
        metrics["latest_attempt"], metrics["last_completed"], metrics["last_completed_age_seconds"],
        metrics["consecutive_failures"], item("never_executed", "No NAV ingestion has been attempted"))
    airflow = metrics["by_trigger"]["airflow"]
    components["airflow_schedule"] = ingestion(
        airflow["latest_attempt"], airflow["last_completed"], airflow["last_completed_age_seconds"],
        airflow["failures_since_success"],
        item("never_executed", "Airflow is expected, but no Airflow-triggered run is recorded") if limits.expect_airflow
        else item("not_configured", "No Airflow-triggered run is recorded; set HEALTH_EXPECT_AIRFLOW=1 if it is deployed"))
    wait = queue["oldest_queued_age_seconds"]
    if wait is None or wait <= limits.max_queue_wait_seconds:
        components["job_queue"] = item("healthy", "No job is waiting" if wait is None else f"Oldest queued job has waited {wait:.0f} seconds")
    else:
        components["job_queue"] = item("degraded" if workers["active"] else "failed",
                                       f"Oldest queued job has waited {wait:.0f} seconds; limit is {limits.max_queue_wait_seconds:g}")
    problems = []
    if metrics["job_lease_expired"]:
        problems.append(f"{metrics['job_lease_expired']} running job(s) hold an expired lease")
    if metrics["lease"]["expired"]:
        problems.append("the NAV source lease expired without being released")
    if queue["lease_expired_failures_24h"]:
        problems.append(f"{queue['lease_expired_failures_24h']} job(s) failed after repeated lease expiry in 24 hours")
    if queue["max_active_lease_reclaims"] >= 2:
        problems.append(f"an unfinished job has been reclaimed {queue['max_active_lease_reclaims']} times")
    components["leases"] = item("degraded", "; ".join(problems)) if problems else item("healthy", "No expired or repeatedly reclaimed lease")
    return {"status": max((part["status"] for part in components.values()), key=SEVERITY.index),
            "components": components}


async def ingestion_metrics(store: Store, limits: HealthLimits | None = None) -> dict:
    limits = limits or HealthLimits.from_env()
    hello = await store.db.command("hello")
    stamp = observed_time(hello)
    latest = await store.db.ingestion_runs.find_one({}, sort=[("created_at", -1), ("_id", -1)])
    completed = await store.db.ingestion_runs.find_one({"status": "completed"}, sort=[("finished_at", -1), ("_id", -1)])
    failed = await store.db.ingestion_runs.find_one({"status": {"$in": ["failed", "interrupted"]}}, sort=[("finished_at", -1)])

    async def failures_after(run: dict | None, **query):
        query["status"] = {"$in": ["failed", "interrupted"]}
        if run:
            query["finished_at"] = {"$gt": run["finished_at"]}
        return await store.db.ingestion_runs.count_documents(query)

    by_trigger = {}
    for trigger in TRIGGERS:
        attempt = await store.db.ingestion_runs.find_one({"trigger": trigger}, sort=[("created_at", -1), ("_id", -1)])
        success = await store.db.ingestion_runs.find_one({"trigger": trigger, "status": "completed"},
                                                         sort=[("finished_at", -1), ("_id", -1)])
        by_trigger[trigger] = {
            "latest_attempt": run_metrics(attempt), "last_completed": run_metrics(success),
            "last_completed_age_seconds": age_seconds(stamp, success["finished_at"] if success else None),
            "failures_since_success": await failures_after(success, trigger=trigger)}
    groups = await store.db.ingestion_runs.aggregate([
        {"$group": {"_id": "$status", "count": {"$sum": 1},
                    "accepted": {"$sum": "$accepted"}, "rejected": {"$sum": "$rejected_count"},
                    "matched": {"$sum": "$matched_count"}, "inserted": {"$sum": "$upserted_count"},
                    "modified": {"$sum": "$modified_count"}}}])
    totals = {row.pop("_id"): row async for row in groups}
    jobs = await store.db.batch_jobs.aggregate([
        {"$group": {"_id": "$status", "count": {"$sum": 1}, "reclaims": {"$sum": "$lease_reclaims"}}}])
    job_rows = [row async for row in jobs]
    # A retried job keeps its creation time, so measure from its last change.
    waiting = await store.db.batch_jobs.aggregate([
        {"$match": {"status": {"$in": ["queued", "running"]}}},
        {"$group": {"_id": "$status", "since": {"$min": {"$ifNull": ["$updated_at", "$created_at"]}},
                    "reclaims": {"$max": {"$ifNull": ["$lease_reclaims", 0]}}}}])
    unfinished = {row["_id"]: row async for row in waiting}
    lease = await store.db.nav_coordination.find_one({"_id": LEASE_ID})
    heartbeat = await store.db.worker_states.find_one({}, sort=[("heartbeat_at", -1)])
    presence = store.db.worker_states.find({"heartbeat_at": {"$gte": stamp - timedelta(seconds=HEARTBEAT_SECONDS)}})
    states = [worker_state(doc, stamp) async for doc in presence]
    source_date = completed.get("source_date_max") if completed else None
    metrics = {
        "observed_at": stamp,
        "transaction_capable": bool(hello.get("setName") and hello.get("logicalSessionTimeoutMinutes") is not None
                                    and hello.get("maxWireVersion", 0) >= 7),
        "latest_attempt": run_metrics(latest), "last_completed": run_metrics(completed),
        "last_failure": run_metrics(failed), "consecutive_failures": await failures_after(completed),
        "last_completed_age_seconds": age_seconds(stamp, completed["finished_at"] if completed else None),
        "source_freshness_days": (stamp.date() - date.fromisoformat(source_date)).days if source_date else None,
        "by_trigger": by_trigger,
        "run_totals": totals,
        "legacy_runs_without_write_counts": await store.db.ingestion_runs.count_documents({"upserted_count": {"$exists": False}}),
        "jobs": {status: next((row["count"] for row in job_rows if row["_id"] == status), 0)
                 for status in ("queued", "running", "failed", "completed", "completed_with_errors")},
        "queue": {
            "oldest_queued_age_seconds": age_seconds(stamp, unfinished["queued"]["since"]) if "queued" in unfinished else None,
            "max_active_lease_reclaims": max((row["reclaims"] for row in unfinished.values()), default=0),
            "lease_expired_failures_24h": await store.db.batch_jobs.count_documents({
                "error_category": "lease_expired", "finished_at": {"$gte": stamp - timedelta(days=1)}})},
        "job_lease_expired": await store.db.batch_jobs.count_documents({"status": "running", "$expr": {"$lte": ["$lease_until", "$$NOW"]}}),
        "job_lease_reclaims": sum(row["reclaims"] for row in job_rows),
        "workers": {
            "active": len(states), **{state: states.count(state) for state in ("polling", "busy", "stalled")},
            "last_heartbeat_at": heartbeat["heartbeat_at"] if heartbeat else None},
        "lease": {"owner_run_id": lease.get("owner") if lease else None,
                  "version": lease.get("version") if lease else None,
                  "expires_at": lease.get("lease_until") if lease else None,
                  "expired": bool(lease and lease.get("owner") and lease["lease_until"] <= stamp),
                  "reclaims": lease.get("reclaims", 0) if lease else 0},
    }
    metrics["health"] = assess(metrics, limits)
    return metrics
