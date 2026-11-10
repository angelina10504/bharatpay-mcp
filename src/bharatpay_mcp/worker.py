"""Mongo-backed worker: leased claims, heartbeats, bounded I/O and resumable rows."""
from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta
import logging
import os
from uuid import uuid4

import httpx
from pydantic import ValidationError

from bharatpay_mcp.api.workbench import VendorInput
from bharatpay_mcp.services.ingestion import ingest_nav
from bharatpay_mcp.services.nav_source import IngestionError
from bharatpay_mcp.services.nav_coordination import reconcile_nav_runs
from bharatpay_mcp.services.vendors import validate_vendor
from bharatpay_mcp.storage import Store, now

logger = logging.getLogger(__name__)


async def process_job(store: Store, client: httpx.AsyncClient, job: dict):
    query = {"_id": job["_id"], "lease_token": job["lease_token"], "status": "running",
             "$expr": {"$gt": ["$lease_until", "$$NOW"]}}

    if job["kind"] == "ingest_nav":
        try:
            await ingest_nav(store, client, job=job, trigger="worker")
        except IngestionError as error:
            retry = error.retryable and job["attempts"] < 3
            values = {"status": "queued" if retry else "failed", "error": str(error),
                      "error_category": error.category, "available_at": now() + timedelta(seconds=10)}
            if not retry:
                values["finished_at"] = now()
            # Busy is contention, not a failed source attempt; defer without
            # exhausting retries while Airflow or CLI owns the source lease.
            update = {"$set": values}
            if error.category == "coordination_busy":
                update["$set"]["status"] = "queued"
                update["$inc"] = {"attempts": -1}
            await store.db.batch_jobs.update_one(query, update)
            logger.warning("NAV job %s: %s", job["_id"], error.category)
        return

    async def heartbeat():
        while True:
            await asyncio.sleep(25)
            if not await store.update("batch_jobs", query, {"lease_until": now() + timedelta(seconds=120)}):
                raise RuntimeError("Job lease lost")

    async def work():
        slots = asyncio.Semaphore(5)

        async def row(number: int, values: dict):
            async with slots:
                row_id = f"{job['_id']}:{number}"
                previous = await store.get("batch_rows", {"_id": row_id})
                if previous and previous["status"] not in {"provider_error"}:
                    return
                try:
                    data = VendorInput(**{k: v or None for k, v in values.items()}).model_dump()
                    result = await validate_vendor(data, client)
                except ValidationError:
                    result = {"status": "invalid", "error": "Invalid row: name is required; fields must fit input limits"}
                if not await store.get("batch_jobs", query):
                    raise RuntimeError("Job lease lost")
                await store.db.batch_rows.update_one(
                    {"_id": row_id}, {"$set": {"owner_id": job["owner_id"], "job_id": job["_id"],
                                               "row_number": number, "name": values.get("name", ""),
                                               **result, "updated_at": now()}}, upsert=True)
                processed = await store.db.batch_rows.count_documents({"job_id": job["_id"]})
                await store.update("batch_jobs", query, {"processed": processed})

        outcomes = await asyncio.gather(*(row(i, data) for i, data in enumerate(job["rows"], 1)), return_exceptions=True)
        for outcome in outcomes:
            if isinstance(outcome, Exception):
                raise outcome
        errors = await store.db.batch_rows.count_documents({"job_id": job["_id"],
                                                           "status": {"$in": ["invalid", "provider_error"]}})
        await store.update("batch_jobs", query, {"status": "completed_with_errors" if errors else "completed",
                                                 "processed": len(job["rows"]), "error_count": errors, "finished_at": now()})

    task = asyncio.create_task(work())
    pulse = asyncio.create_task(heartbeat())
    try:
        done, _ = await asyncio.wait([task, pulse], return_when=asyncio.FIRST_COMPLETED)
        for finished in done:
            finished.result()
    except Exception:
        logger.warning("Job processing failed: %s", job["_id"])
        await store.update("batch_jobs", query, {"status": "queued" if job["attempts"] < 3 else "failed",
                                                 "error": "Processing failed; automatic retry limited to 3 attempts"})
        await asyncio.sleep(min(job["attempts"] * 2, 6))
    finally:
        for pending in [task, pulse]:
            pending.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await pending


async def main():
    store = Store(os.environ["MONGO_URL"])
    await store.start()
    worker_id = str(uuid4())
    async def presence():
        while True:
            stamp = now()
            await store.db.worker_states.update_one({"_id": worker_id}, {"$set": {
                "heartbeat_at": stamp, "expires_at": stamp + timedelta(days=1)}}, upsert=True)
            await reconcile_nav_runs(store)
            await asyncio.sleep(10)
    async def monitor():
        while True:
            try:
                await presence()
            except Exception:
                logger.warning("Worker heartbeat unavailable")
                await asyncio.sleep(5)
    heartbeat_task = asyncio.create_task(monitor())
    try:
        async with httpx.AsyncClient(limits=httpx.Limits(max_connections=12)) as client:
            while True:
                try:
                    job = await store.claim()
                    if job:
                        await process_job(store, client, job)
                    else:
                        await asyncio.sleep(2)
                except Exception:
                    logger.warning("Worker iteration failed; database or provider may be unavailable")
                    await asyncio.sleep(5)
    finally:
        heartbeat_task.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat_task
        await store.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
