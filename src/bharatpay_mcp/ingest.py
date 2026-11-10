"""Run one ingestion without Airflow: python -m bharatpay_mcp.ingest."""
from __future__ import annotations

import asyncio
import argparse
import json
import os
from pathlib import Path
from pymongo.errors import PyMongoError

import httpx

from bharatpay_mcp.services.ingestion import ingest_nav
from bharatpay_mcp.services.nav_source import IngestionError
from bharatpay_mcp.storage import Store


async def main(*, trigger: str = "cli", fixture: Path | None = None):
    store = Store(os.environ["MONGO_URL"])
    try:
        await store.start()
        async with httpx.AsyncClient() as client:
            result = await ingest_nav(store, client, trigger=trigger, fixture=fixture)
            print(json.dumps(result, default=str))
            return result
    except PyMongoError:
        raise IngestionError("database", "NAV database is unavailable") from None
    finally:
        await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, help="Replay a retained AMFI text file without HTTP")
    args = parser.parse_args()
    asyncio.run(main(fixture=args.fixture))
