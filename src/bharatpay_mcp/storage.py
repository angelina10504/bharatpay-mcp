"""MongoDB persistence with explicit indexes and atomic job leases."""
from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from uuid import uuid4

from pymongo import AsyncMongoClient, ReturnDocument
from bson import ObjectId


def now() -> datetime:
    return datetime.now(timezone.utc)


def public(doc: dict | None) -> dict | None:
    if doc is None:
        return None
    return {("id" if k == "_id" else k): (str(v) if isinstance(v, ObjectId) else v) for k, v in doc.items()
            if k not in {"password_hash", "token_hash", "rows", "lease_token"}}


class Store:
    def __init__(self, url: str):
        self.client = AsyncMongoClient(url, serverSelectionTimeoutMS=5000, tz_aware=True)
        self.db = self.client[os.getenv("MONGO_DATABASE", "bharatpay")]

    async def start(self):
        await self.db.command("ping")
        await self.db.users.create_index("username", unique=True)
        await self.db.sessions.create_index("expires_at", expireAfterSeconds=0)
        for name in ["vendors", "validation_runs", "batch_jobs"]:
            await self.db[name].create_index([("owner_id", 1), ("created_at", -1), ("_id", -1)])
        await self.db.batch_jobs.create_index([("status", 1), ("lease_until", 1), ("created_at", 1)])
        await self.db.batch_jobs.create_index("kind", unique=True,
            partialFilterExpression={"kind": "ingest_nav", "status": {"$in": ["queued", "running"]}},
            name="one_active_nav_ingestion")
        await self.db.batch_rows.create_index([("job_id", 1), ("row_number", 1)], unique=True)
        await self.db.nav_snapshots.create_index([("scheme_code", 1), ("date", 1)], unique=True)
        await self.db.ingestion_runs.create_index([("created_at", -1)])

    async def close(self):
        await self.client.close()

    async def get(self, collection: str, query: dict) -> dict | None:
        return await self.db[collection].find_one(query)

    async def insert(self, collection: str, doc: dict) -> dict:
        doc = {"_id": str(uuid4()), "created_at": now(), **doc}
        await self.db[collection].insert_one(doc)
        return doc

    async def page(self, collection: str, query: dict, page: int, size: int, sort=None) -> dict:
        cursor = self.db[collection].find(query).sort(sort or [("created_at", -1), ("_id", -1)])
        items = await cursor.skip((page - 1) * size).limit(size).to_list(length=size)
        return {"items": [public(d) for d in items], "page": page, "page_size": size,
                "total": await self.db[collection].count_documents(query)}

    async def update(self, collection: str, query: dict, values: dict) -> dict | None:
        return await self.db[collection].find_one_and_update(
            query, {"$set": {**values, "updated_at": now()}}, return_document=ReturnDocument.AFTER)

    async def claim(self) -> dict | None:
        await self.db.batch_jobs.update_many(
            {"status": "running", "lease_until": {"$lt": now()}, "attempts": {"$gte": 3}},
            {"$set": {"status": "failed", "error": "Worker lease expired after three attempts"}})
        return await self.db.batch_jobs.find_one_and_update(
            {"attempts": {"$lt": 3}, "$or": [
                {"status": "queued"}, {"status": "running", "lease_until": {"$lt": now()}}]},
            {"$set": {"status": "running", "lease_until": now() + timedelta(seconds=120),
                      "lease_token": str(uuid4()), "updated_at": now()}, "$inc": {"attempts": 1}},
            sort=[("created_at", 1)], return_document=ReturnDocument.AFTER)
