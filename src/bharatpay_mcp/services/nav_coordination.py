"""Replica-set fencing: every snapshot transaction also writes the lease document.

A token comparison outside the transaction is insufficient. Updating the shared
document makes takeover and old batches conflict, including transactions opened
before takeover. Expiry permits acquisition; it cannot revoke an already ordered
transaction retroactively. Each batch is atomic, not the entire source file.
"""
from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import datetime, timezone
from typing import Awaitable, Callable

import pymongo
from pymongo import ReturnDocument
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from bharatpay_mcp.services.nav_source import IngestionError
from bharatpay_mcp.storage import Store

LEASE_ID = "amfi_nav"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class LeaseLost(IngestionError):
    def __init__(self):
        super().__init__("lease_lost", "NAV or worker lease is no longer owned by this run")


class IngestionBusy(IngestionError):
    def __init__(self):
        super().__init__("coordination_busy", "Another NAV ingestion owns the source lease")


async def require_transactions(store: Store):
    hello = await store.db.command("hello")
    if not hello.get("setName") or hello.get("logicalSessionTimeoutMinutes") is None or hello.get("maxWireVersion", 0) < 7:
        raise IngestionError("topology", "NAV ingestion requires a transaction-capable MongoDB replica set")


class NavLease:
    def __init__(self, store: Store, run_id: str, *, job: dict | None = None, seconds: int = 120):
        if seconds < 1:
            raise ValueError("Lease duration must be positive")
        self.store, self.run_id, self.job, self.seconds = store, run_id, job, seconds
        self.version: int | None = None
        self.mutex = asyncio.Lock()

    def renewal(self):
        return [{"$set": {"lease_until": {"$dateAdd": {"startDate": "$$NOW", "unit": "second", "amount": self.seconds}},
                          "heartbeat_at": "$$NOW"}}]

    async def transaction(self, callback: Callable[..., Awaitable]):
        # Callbacks can retry: no Python counters or HTTP/file side effects here.
        async with self.mutex:
            with pymongo.timeout(20):
                async with self.store.client.start_session() as session:
                    return await session.with_transaction(callback, read_concern=ReadConcern("snapshot"),
                                                          write_concern=WriteConcern("majority"), max_commit_time_ms=10000)

    async def acquire(self):
        await require_transactions(self.store)
        await self.store.db.nav_coordination.with_options(write_concern=WriteConcern("majority")).update_one(
            {"_id": LEASE_ID}, {"$setOnInsert": {"version": 0, "owner": None, "lease_until": EPOCH}}, upsert=True)

        async def claim(session):
            lock = await self.store.db.nav_coordination.find_one_and_update(
                {"_id": LEASE_ID, "$or": [{"owner": None}, {"$expr": {"$lte": ["$lease_until", "$$NOW"]}}]},
                [{"$set": {"owner": self.run_id,
                           "version": {"$add": ["$version", 1]},
                           "reclaims": {"$add": [{"$ifNull": ["$reclaims", 0]}, {"$cond": [{"$ne": ["$owner", None]}, 1, 0]}]},
                           "lease_until": {"$dateAdd": {"startDate": "$$NOW", "unit": "second", "amount": self.seconds}},
                           "heartbeat_at": "$$NOW"}}], session=session, return_document=ReturnDocument.AFTER)
            if lock is None:
                raise IngestionBusy()
            await self.store.db.ingestion_runs.update_many(
                {"_id": {"$ne": self.run_id}, "status": "running"},
                [{"$set": {"status": "interrupted", "finished_at": "$$NOW", "updated_at": "$$NOW",
                           "duration_seconds": {"$divide": [{"$subtract": ["$$NOW", {"$ifNull": ["$started_at", "$created_at"]}]}, 1000]},
                           "error_category": "interrupted", "error": "Previous ingestion lost its lease",
                           "diagnostic": "Previous ingestion lost its lease", "write_outcome": "partial_or_unknown"}}], session=session)
            result = await self.store.db.ingestion_runs.update_one(
                {"_id": self.run_id, "status": "starting"},
                {"$set": {"status": "running", "coordination_version": lock["version"]}}, session=session)
            if result.matched_count != 1:
                raise LeaseLost()
            return lock["version"]
        self.version = await self.transaction(claim)

    async def guard(self, session):
        result = await self.store.db.nav_coordination.update_one(
            {"_id": LEASE_ID, "owner": self.run_id, "version": self.version,
             "$expr": {"$gt": ["$lease_until", "$$NOW"]}}, self.renewal(), session=session)
        if result.matched_count != 1:
            raise LeaseLost()
        if self.job:
            result = await self.store.db.batch_jobs.update_one(
                {"_id": self.job["_id"], "lease_token": self.job["lease_token"], "status": "running",
                 "$expr": {"$gt": ["$lease_until", "$$NOW"]}}, self.renewal(), session=session)
            if result.matched_count != 1:
                raise LeaseLost()

    async def fenced(self, callback):
        async def guarded(session):
            await self.guard(session)
            return await callback(session)
        return await self.transaction(guarded)

    async def run(self, work):
        async def heartbeat():
            while True:
                await asyncio.sleep(self.seconds / 3)
                async def pulse(session):
                    await self.guard(session)
                await self.transaction(pulse)
        task, pulse = asyncio.create_task(work()), asyncio.create_task(heartbeat())
        try:
            done, _ = await asyncio.wait([task, pulse], return_when=asyncio.FIRST_COMPLETED)
            if pulse in done:
                pulse.result()
            return await task
        finally:
            for pending in (task, pulse):
                pending.cancel()
            await asyncio.gather(task, pulse, return_exceptions=True)

    async def release(self):
        if self.version is not None:
            with suppress(pymongo.errors.PyMongoError):
                with pymongo.timeout(5):
                    await self.store.db.nav_coordination.update_one(
                        {"_id": LEASE_ID, "owner": self.run_id, "version": self.version},
                        {"$set": {"owner": None, "lease_until": EPOCH}})


async def reconcile_nav_runs(store: Store) -> int:
    """Fence expired owners before reconciling; never just relabel a live writer.

    Called periodically by workers and explicitly by tests. Unsupported standalone
    deployments cannot have a new fenced NAV writer, so return without mutation.
    """
    try:
        await require_transactions(store)
    except IngestionError:
        return 0
    await store.db.nav_coordination.with_options(write_concern=WriteConcern("majority")).update_one(
        {"_id": LEASE_ID}, {"$setOnInsert": {"version": 0, "owner": None, "lease_until": EPOCH}}, upsert=True)
    coordinator = NavLease(store, "reconciler")
    async def reconcile(session):
        lock = await store.db.nav_coordination.find_one_and_update(
            {"_id": LEASE_ID, "$or": [{"owner": None}, {"$expr": {"$lte": ["$lease_until", "$$NOW"]}}]},
            [{"$set": {"owner": None, "lease_until": EPOCH,
                       "version": {"$add": ["$version", 1]},
                       "reclaims": {"$add": [{"$ifNull": ["$reclaims", 0]},
                                               {"$cond": [{"$ne": ["$owner", None]}, 1, 0]}]}}}],
            session=session, return_document=ReturnDocument.AFTER)
        if lock is None:
            return 0
        result = await store.db.ingestion_runs.update_many(
            {"$or": [{"status": "running"}, {"status": "starting", "$expr": {"$lt": [
                "$created_at", {"$dateSubtract": {"startDate": "$$NOW", "unit": "second", "amount": 120}}]}}]},
            [{"$set": {"status": "interrupted", "finished_at": "$$NOW", "updated_at": "$$NOW",
                       "duration_seconds": {"$divide": [{"$subtract": ["$$NOW", {"$ifNull": ["$started_at", "$created_at"]}]}, 1000]},
                       "error_category": "interrupted", "error": "Ingestion reconciled after lease expiry",
                       "diagnostic": "Ingestion reconciled after lease expiry", "write_outcome": "partial_or_unknown"}}], session=session)
        return result.modified_count
    return await coordinator.transaction(reconcile)
