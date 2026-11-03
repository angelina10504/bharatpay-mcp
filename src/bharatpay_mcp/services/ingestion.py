"""Idempotent NAV ETL, callable from the CLI, durable worker or Airflow."""
from __future__ import annotations

import asyncio

import httpx
from pymongo import UpdateOne

from bharatpay_mcp.services.nav import AMFI_URL, parse_nav
from bharatpay_mcp.storage import Store, now
from bharatpay_mcp.tools._http import fetch


async def ingest_nav(store: Store, client: httpx.AsyncClient, job_id: str | None = None) -> dict:
    if job_id:
        await store.db.ingestion_runs.update_many({"job_id": job_id, "status": "running"},
            {"$set": {"status": "interrupted", "finished_at": now()}})
    run = await store.insert("ingestion_runs", {"status": "running", "source": AMFI_URL, "job_id": job_id})
    try:
        response = await fetch(AMFI_URL, client)
        if len(response.content) > 15_000_000:
            raise ValueError("AMFI response exceeds size limit")
        rows, rejected = await asyncio.to_thread(parse_nav, response.text)
        operations = [UpdateOne({"scheme_code": r["scheme_code"], "date": r["date"]},
                                {"$set": {**r, "fetched_at": now(), "source": AMFI_URL}}, upsert=True)
                      for r in rows]
        for offset in range(0, len(operations), 1000):
            await store.db.nav_snapshots.bulk_write(operations[offset:offset + 1000], ordered=False)
        summary = {"status": "completed", "accepted": len(rows), "rejected_count": len(rejected),
                   "rejected_rows": rejected[:100], "finished_at": now()}
        await store.update("ingestion_runs", {"_id": run["_id"]}, summary)
        return {"run_id": run["_id"], **summary}
    except Exception:
        await store.update("ingestion_runs", {"_id": run["_id"]},
                           {"status": "failed", "error": "NAV ingestion failed; safe to retry", "finished_at": now()})
        raise
