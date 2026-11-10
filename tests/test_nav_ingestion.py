"""Offline run-lifecycle contracts; real transaction races live in integration/."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from bharatpay_mcp.services import ingestion
from bharatpay_mcp.services.nav_coordination import LeaseLost, NavLease, require_transactions
from bharatpay_mcp.services.nav_source import IngestionError


def test_standalone_is_rejected_before_snapshot_writes():
    async def exercise():
        store = MagicMock()
        store.db.command = AsyncMock(return_value={"maxWireVersion": 21})
        store.insert, store.update = AsyncMock(), AsyncMock()
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: pytest.fail("Standalone must not download"))) as client:
            with pytest.raises(IngestionError) as error:
                await ingestion.ingest_nav(store, client)
            assert error.value.category == "topology"
        fields = store.update.await_args.args[2]
        assert fields["status"] == "failed" and fields["error_category"] == "topology"
        store.db.nav_snapshots.bulk_write.assert_not_called()
        store.client.start_session.assert_not_called()
    asyncio.run(exercise())


@pytest.mark.parametrize("hello", [{"setName": "rs", "maxWireVersion": 21},
                                     {"logicalSessionTimeoutMinutes": 30, "maxWireVersion": 21}])
def test_incomplete_topology_capabilities_fail_closed(hello):
    async def exercise():
        store = MagicMock()
        store.db.command = AsyncMock(return_value=hello)
        with pytest.raises(IngestionError):
            await require_transactions(store)
    asyncio.run(exercise())


def test_lost_fence_never_calls_write_callback():
    async def exercise():
        store = MagicMock()
        store.db.nav_coordination.update_one = AsyncMock(return_value=MagicMock(matched_count=0))
        lease = NavLease(store, "old-run")
        lease.version = 1
        async def transaction(callback):
            return await callback("synthetic-session")
        lease.transaction = transaction
        write = AsyncMock()
        with pytest.raises(LeaseLost):
            await lease.fenced(write)
        write.assert_not_awaited()
        query = store.db.nav_coordination.update_one.await_args.args[0]
        assert query["owner"] == "old-run" and query["version"] == 1
        assert query["$expr"] == {"$gt": ["$lease_until", "$$NOW"]}
    asyncio.run(exercise())


@pytest.mark.parametrize("content,category,parsed,rejected", [
    (b"<html>error</html>", "parse", 0, 0),
    (b"990001;-;-;Synthetic;NaN;03-Nov-2026\n", "parse", 1, 1),
    (b"\xff", "parse", 0, 0),
])
def test_failed_source_retains_metadata_but_never_writes(monkeypatch, content, category, parsed, rejected):
    class Lease:
        version = 1
        def __init__(self, *args, **kwargs):
            pass
        async def acquire(self):
            pass
        async def release(self):
            pass
        async def run(self, work):
            return await work()
        async def fenced(self, callback):
            pytest.fail("Invalid sources must never reach writes")
    monkeypatch.setattr(ingestion, "NavLease", Lease)
    async def exercise():
        document = {}
        store = MagicMock()
        async def insert(collection, values):
            document.update(values)
            return document.copy()
        async def update(collection, query, values):
            document.update(values)
            return document.copy()
        store.insert, store.update = insert, update
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=content))) as client:
            with pytest.raises(IngestionError) as error:
                await ingestion.ingest_nav(store, client)
            assert error.value.category == category
        assert document["status"] == "failed" and document["source_bytes"] == len(content)
        assert document["source_complete"] is True and len(document["source_sha256"]) == 64
        assert document["parsed_count"] == parsed and document["rejected_count"] == rejected
        assert document["upserted_count"] == document["matched_count"] == 0
        assert document["duration_seconds"] >= 0
    asyncio.run(exercise())
