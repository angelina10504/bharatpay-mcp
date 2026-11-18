"""Explicit integration check: needs running Compose services; uses synthetic data.

Run: docker compose exec -T api python < scripts/check_backend.py
This is deliberately separate from the offline pytest suite.
"""
from __future__ import annotations

import asyncio
from datetime import timedelta
import os
import secrets
from uuid import uuid4

import httpx

from bharatpay_mcp.services.ingestion import ingest_nav
from bharatpay_mcp.storage import Store, now
from bharatpay_mcp.worker import process_job
from check_nav import run_nav_checks


async def main():
    store = Store(os.environ["MONGO_URL"])
    await store.start()
    owners = []
    isolated = Store(os.environ["MONGO_URL"])
    isolated.db = isolated.client["bharatpay_check_" + uuid4().hex]
    await isolated.start()
    checks = 0
    async with httpx.AsyncClient(base_url=os.getenv("API_BASE_URL", "http://api:8000"), timeout=40) as client:
        async def call(method, path, token=None, status=200, **kwargs):
            nonlocal checks
            headers = kwargs.pop("headers", {})
            if token:
                headers["Authorization"] = "Bearer " + token
            response = await client.request(method, path, headers=headers, **kwargs)
            assert response.status_code == status, (path, response.status_code, response.text)
            checks += 1
            return response
        try:
            tokens = []
            for i in range(2):
                creds = {"username": "check_" + uuid4().hex[:14], "password": secrets.token_urlsafe(18)}
                response = await call("POST", "/api/v1/auth/register", status=201, json=creds)
                owners.append(response.json()["id"])
                token = (await call("POST", "/api/v1/auth/login", json=creds)).json()["access_token"]
                tokens.append(token)
            a, b = tokens
            await call("GET", "/api/v1/ingestions/metrics", status=401)
            metrics = (await call("GET", "/api/v1/ingestions/metrics", a)).json()
            assert metrics["transaction_capable"] and metrics["workers"]["active"] >= 1
            expected_workers = int(os.getenv("EXPECTED_WORKERS", "1"))
            for _ in range(15):
                if metrics["workers"]["polling"] + metrics["workers"]["busy"] >= expected_workers:
                    break
                await asyncio.sleep(1)
                metrics = (await call("GET", "/api/v1/ingestions/metrics", a)).json()
            assert metrics["health"]["components"]["worker"]["status"] == "healthy", metrics["workers"]
            assert metrics["health"]["components"]["database"]["status"] == "healthy"
            await call("GET", "/api/v1/vendors", status=401)
            vendor = (await call("POST", "/api/v1/vendors", a, status=201,
                                 json={"name": "Synthetic demo", "pan": "ABCPE1234F"})).json()
            id = vendor["id"]
            await call("GET", f"/api/v1/vendors/{id}", b, status=404)
            await call("PUT", f"/api/v1/vendors/{id}", b, status=404, json={"name": "Unauthorized"})
            await call("DELETE", f"/api/v1/vendors/{id}", b, status=404)
            await call("POST", f"/api/v1/vendors/{id}/validate", b, status=404)
            result = await call("POST", f"/api/v1/vendors/{id}/validate", a)
            assert result.json()["status"] == "valid"
            assert (await call("GET", "/api/v1/validation-runs", b)).json()["total"] == 0
            assert (await call("GET", "/api/v1/stats", a)).json()["validation_counts"]["valid"] == 1
            assert (await call("GET", "/api/v1/vendors?page=1&page_size=1", a)).json()["total"] == 1
            csv_data = "name,pan\n=SUM(1),ABCPE1234F\nInvalid,BAD\n"
            upload = {"files": {"file": ("demo.csv", csv_data, "text/csv")}, "headers": {"Idempotency-Key": uuid4().hex}}
            job = (await call("POST", "/api/v1/batch-jobs", a, status=202, **upload)).json()
            repeated = (await call("POST", "/api/v1/batch-jobs", a, status=202, **upload)).json()
            assert job["id"] == repeated["id"]
            await call("GET", f"/api/v1/batch-jobs/{job['id']}/results", b, status=404)
            for _ in range(30):
                current = (await call("GET", f"/api/v1/batch-jobs/{job['id']}", a)).json()
                if current["status"] in {"completed", "completed_with_errors", "failed"}:
                    break
                await asyncio.sleep(1)
            assert current["status"] == "completed_with_errors", current
            assert current["processed"] == 2
            rows = (await call("GET", f"/api/v1/batch-jobs/{job['id']}/results", a)).json()["items"]
            assert {r["status"] for r in rows} == {"valid", "invalid"}
            report = await call("GET", f"/api/v1/batch-jobs/{job['id']}/report.csv", a)
            assert "'=SUM" in report.text
            # Several queued jobs at once: with multiple workers each job is claimed by
            # one lease holder and every row has exactly one stable result document.
            many = [(await call("POST", "/api/v1/batch-jobs", b, status=202,
                                files={"file": ("bulk.csv", "name,pan\n" + "".join(f"Row {n},ABCPE1234F\n" for n in range(20)),
                                                "text/csv")})).json()["id"] for _ in range(6)]
            for _ in range(60):
                states = [(await call("GET", f"/api/v1/batch-jobs/{job_id}", b)).json() for job_id in many]
                if all(state["status"] == "completed" for state in states):
                    break
                await asyncio.sleep(1)
            assert all(state["status"] == "completed" and state["processed"] == 20 and state["attempts"] == 1
                       for state in states), states
            for job_id in many:
                assert await store.db.batch_rows.count_documents({"job_id": job_id}) == 20
            retried = await call("POST", f"/api/v1/batch-jobs/{job['id']}/retry", a)
            assert retried.json()["status"] == "queued"
            await call("POST", f"/api/v1/batch-jobs/{job['id']}/retry", b, status=409)
            for _ in range(30):
                current = (await call("GET", f"/api/v1/batch-jobs/{job['id']}", a)).json()
                if current["status"] in {"completed", "completed_with_errors", "failed"}:
                    break
                await asyncio.sleep(1)
            assert current["status"] == "completed_with_errors"
            assert await store.db.batch_rows.count_documents({"job_id": job["id"]}) == 2  # stable row ids on retry
            if os.getenv("CHECK_WORKER_NAV"):
                # API -> queued job -> worker -> fenced ETL. Requires the workers to
                # run with NAV_SOURCE_FILE set to the synthetic fixture (no AMFI call).
                queued = (await call("POST", "/api/v1/ingestions/nav", a, status=202)).json()
                await call("POST", "/api/v1/ingestions/nav", b, status=409)  # one active refresh across users
                for _ in range(30):
                    nav_job = (await call("GET", f"/api/v1/batch-jobs/{queued['id']}", a)).json()
                    if nav_job["status"] in {"completed", "failed"}:
                        break
                    await asyncio.sleep(1)
                assert nav_job["status"] == "completed", nav_job
                summary = nav_job["summary"]
                assert summary["trigger"] == "worker" and summary["source_kind"] == "fixture"
                assert (summary["accepted"], summary["rejected_count"]) == (2, 1)
                assert summary["upserted_count"] + summary["matched_count"] == 2
                metrics = (await call("GET", "/api/v1/ingestions/metrics", a)).json()
                assert metrics["by_trigger"]["worker"]["last_completed"]["run_id"] == summary["run_id"]
                history = (await call("GET", "/api/v1/funds/990001/nav-history", a)).json()
                assert history["items"][0]["nav"] == "10.25"
            await call("POST", "/api/v1/auth/logout", a)
            await call("GET", "/api/v1/auth/me", a, status=401)
            await store.db.sessions.update_one({"owner_id": owners[1]}, {"$set": {"expires_at": now() - timedelta(seconds=1)}})
            await call("GET", "/api/v1/auth/me", b, status=401)

            # Actual MongoDB indexes, atomic claim, restart recovery and ETL replay.
            recovered = await isolated.insert("batch_jobs", {
                "owner_id": "test", "kind": "validate_csv", "status": "running", "attempts": 1,
                "lease_until": now() - timedelta(minutes=5), "rows": [{"name": "Recovered", "pan": "BAD"}]})
            claimed = await isolated.claim()
            assert claimed["_id"] == recovered["_id"] and claimed["attempts"] == 2
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))) as offline:
                await process_job(isolated, offline, claimed)
            assert (await isolated.get("batch_jobs", {"_id": recovered["_id"]}))["status"] == "completed_with_errors"
            assert await isolated.db.batch_rows.count_documents({"job_id": recovered["_id"]}) == 1
            exhausted = await isolated.insert("batch_jobs", {
                "owner_id": "test", "kind": "validate_csv", "status": "running", "attempts": 3,
                "lease_until": now() - timedelta(minutes=5), "rows": []})
            assert await isolated.claim() is None
            assert (await isolated.get("batch_jobs", {"_id": exhausted["_id"]}))["status"] == "failed"
            nav = "123456;-;-;Synthetic Fund;10.25;02-Nov-2026\n123457;-;-;Rejected Fund;N.A.;02-Nov-2026\n"
            async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, text=nav))) as source:
                await ingest_nav(isolated, source)
                await ingest_nav(isolated, source)
            assert await isolated.db.nav_snapshots.count_documents({}) == 1
            assert await isolated.db.ingestion_runs.count_documents({"status": "completed", "rejected_count": 1}) == 2
            await run_nav_checks(os.environ["MONGO_URL"])
            print(f"PASS: {checks} HTTP checks; ownership, durable CSV worker, report safety, logout, Mongo lease recovery, idempotent ETL.")
        finally:
            for collection in ["users", "sessions", "vendors", "validation_runs", "batch_jobs", "batch_rows"]:
                query = {"_id": {"$in": owners}} if collection == "users" else {"owner_id": {"$in": owners}}
                await store.db[collection].delete_many(query)
            await isolated.client.drop_database(isolated.db.name)
            await isolated.close()
            await store.close()


if __name__ == "__main__":
    asyncio.run(main())
