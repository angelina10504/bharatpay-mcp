"""Auditable, per-batch atomic NAV ETL shared by CLI, Airflow and worker."""
from __future__ import annotations

import asyncio
from contextlib import suppress
import logging
import os
from pathlib import Path
from uuid import uuid4

import httpx
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, PyMongoError

from bharatpay_mcp.services.nav import AMFI_URL, parse_nav
from bharatpay_mcp.services.nav_coordination import NavLease, LeaseLost
from bharatpay_mcp.services.nav_source import (
    IngestionError, QualityRules, byte_limit, download_source, read_fixture, retain_artifact,
)
from bharatpay_mcp.storage import Store, now, public

logger = logging.getLogger(__name__)


def terminal_fields(status: str, started_at, **extra):
    finished = now()
    return {"status": status, "finished_at": finished,
            "duration_seconds": max(0, (finished - started_at).total_seconds()), **extra}


async def ingest_nav(store: Store, client: httpx.AsyncClient, job_id: str | None = None, *,
                     trigger: str | None = None, job: dict | None = None,
                     fixture: Path | None = None, rules: QualityRules | None = None,
                     lease_seconds: int = 120, batch_size: int = 1000) -> dict:
    """Accepted counts unique valid source rows, not inserted database documents.

    Earlier committed batches survive later failure. Snapshot results and run
    counters commit together, allowing ambiguous outcomes to be read back.
    """
    trigger = trigger or ("worker" if job_id or job else "cli")
    if trigger not in {"cli", "worker", "airflow"} or not 1 <= batch_size <= 1000:
        raise ValueError("Invalid ingestion trigger or batch size")
    if job is not None:
        job_id = job["_id"]
    elif job_id:
        job = await store.get("batch_jobs", {"_id": job_id, "status": "running"})
        if not job or not job.get("lease_token"):
            raise LeaseLost()
    run_id, started = str(uuid4()), now()
    await store.insert("ingestion_runs", {
        "_id": run_id, "run_id": run_id, "trigger": trigger, "job_id": job_id,
        "status": "starting", "source": AMFI_URL, "source_kind": "fixture" if fixture else "http",
        "started_at": started, "finished_at": None, "duration_seconds": None,
        "source_sha256": None, "source_bytes": None, "source_complete": False,
        "parsed_count": 0, "accepted": 0, "rejected_count": 0, "rejected_rows": [],
        "matched_count": 0, "upserted_count": 0, "modified_count": 0, "batches_committed": 0,
        "source_date_min": None, "source_date_max": None, "error_category": None, "diagnostic": None,
        "write_outcome": "not_started"})
    lease = NavLease(store, run_id, job=job, seconds=lease_seconds)

    async def work():
        quality = rules or QualityRules.from_env()
        limit = byte_limit()
        artifact = (await asyncio.to_thread(read_fixture, fixture, limit) if fixture else
                    await download_source(client, limit))
        metadata = artifact.metadata()
        await store.update("ingestion_runs", {"_id": run_id, "status": "running"}, metadata)
        if directory := os.getenv("NAV_RAW_DIR"):
            name = await asyncio.to_thread(retain_artifact, artifact, Path(directory))
            await store.update("ingestion_runs", {"_id": run_id, "status": "running"}, {"artifact_name": name})
        rows, rejected = await asyncio.to_thread(parse_nav, artifact.text(), allow_empty=True)
        summary = quality.summarize(rows, rejected)
        await store.update("ingestion_runs", {"_id": run_id, "status": "running"}, summary)
        if not rows:
            raise IngestionError("parse", "Source contains no usable NAV records")
        quality.validate(summary, artifact.fetched_at.date())
        for offset in range(0, len(rows), batch_size):
            batch = rows[offset:offset + batch_size]
            async def write(session):
                stamp = now()
                operations = [UpdateOne({"scheme_code": row["scheme_code"], "date": row["date"]},
                    {"$set": {**row, "fetched_at": artifact.fetched_at, "ingested_at": stamp,
                              "source": AMFI_URL, "source_sha256": artifact.sha256,
                              "run_id": run_id, "coordination_version": lease.version}}, upsert=True) for row in batch]
                result = await store.db.nav_snapshots.bulk_write(operations, ordered=False, session=session)
                counted = await store.db.ingestion_runs.update_one(
                    {"_id": run_id, "status": "running"},
                    {"$inc": {"matched_count": result.matched_count, "upserted_count": result.upserted_count,
                              "modified_count": result.modified_count, "batches_committed": 1},
                     "$set": {"updated_at": stamp, "write_outcome": "acknowledged_partial"}}, session=session)
                if counted.matched_count != 1:
                    raise LeaseLost()
            await lease.fenced(write)

        async def complete(session):
            fields = terminal_fields("completed", started, write_outcome="acknowledged")
            result = await store.db.ingestion_runs.update_one(
                {"_id": run_id, "status": "running"}, {"$set": fields}, session=session)
            if result.matched_count != 1:
                raise LeaseLost()
            document = await store.db.ingestion_runs.find_one({"_id": run_id}, session=session)
            result = public(document)
            if job:
                await store.db.batch_jobs.update_one(
                    {"_id": job_id, "lease_token": job["lease_token"], "status": "running"},
                    {"$set": {"status": "completed", "summary": result, "finished_at": fields["finished_at"],
                              "error": None, "error_category": None}}, session=session)
            return result
        return await lease.fenced(complete)

    try:
        await lease.acquire()
        return await lease.run(work)
    except (Exception, asyncio.CancelledError) as exc:
        if isinstance(exc, asyncio.CancelledError):
            error = IngestionError("interrupted", "NAV ingestion was interrupted")
        elif isinstance(exc, IngestionError):
            error = exc
        elif isinstance(exc, BulkWriteError):
            error = IngestionError("write", "NAV batch write failed; current transaction was aborted")
        elif isinstance(exc, PyMongoError):
            error = IngestionError("database", "Database operation failed; reconcile persisted run counters before retry")
        elif isinstance(exc, (ValueError, TypeError)):
            error = IngestionError("configuration", "NAV ingestion configuration is invalid")
        else:
            error = IngestionError("internal", "Unexpected NAV ingestion failure")
        status = {"interrupted": "interrupted", "coordination_busy": "skipped"}.get(error.category, "failed")
        fields = terminal_fields(status, started, error_category=error.category, diagnostic=str(error), error=str(error))
        if isinstance(exc, PyMongoError) and not isinstance(exc, BulkWriteError):
            fields["write_outcome"] = "unknown"
        try:
            updated = await store.update("ingestion_runs", {"_id": run_id, "status": {"$in": ["starting", "running"]}}, fields)
            if updated is None:
                persisted = await store.get("ingestion_runs", {"_id": run_id})
                if persisted and persisted["status"] == "completed" and not isinstance(exc, asyncio.CancelledError):
                    return public(persisted)
        except PyMongoError:
            logger.warning("NAV run %s needs status reconciliation after database recovery", run_id)
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise error from None
    finally:
        with suppress(asyncio.CancelledError):
            await lease.release()
