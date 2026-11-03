"""Run one ingestion without Airflow: python -m bharatpay_mcp.ingest."""
from __future__ import annotations

import asyncio
import json
import os

import httpx

from bharatpay_mcp.services.ingestion import ingest_nav
from bharatpay_mcp.storage import Store


async def main():
    store = Store(os.environ["MONGO_URL"])
    await store.start()
    try:
        async with httpx.AsyncClient() as client:
            result = await ingest_nav(store, client)
            print(json.dumps(result, default=str))
    finally:
        await store.close()


if __name__ == "__main__":
    asyncio.run(main())
