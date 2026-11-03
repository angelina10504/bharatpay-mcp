"""Authenticated saved records, batch jobs and ingestion history."""
from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from datetime import timedelta
import hashlib
import io
import csv
import secrets
import time
from typing import Annotated
from uuid import uuid5, NAMESPACE_URL

from fastapi import APIRouter, Depends, File, Header, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, ConfigDict, Field
from pymongo.errors import DuplicateKeyError

from bharatpay_mcp.security import hash_password, verify_password, token_hash
from bharatpay_mcp.services.batches import parse_csv
from bharatpay_mcp.services.vendors import validate_vendor
from bharatpay_mcp.storage import Store, now, public

router = APIRouter(prefix="/api/v1")
bearer = HTTPBearer(auto_error=False)


def get_store(request: Request) -> Store:
    store = request.app.state.store
    if store is None:
        raise HTTPException(503, "Start MongoDB and configure MONGO_URL to use saved workflows")
    return store


async def owner(credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
                store: Annotated[Store, Depends(get_store)]) -> str:
    if credentials and len(credentials.credentials) <= 128:
        session = await store.get("sessions", {"_id": token_hash(credentials.credentials), "expires_at": {"$gt": now()}})
        if session:
            return session["owner_id"]
    raise HTTPException(401, "A valid bearer token is required", headers={"WWW-Authenticate": "Bearer"})


StoreDep = Annotated[Store, Depends(get_store)]
OwnerDep = Annotated[str, Depends(owner)]
Page = Annotated[int, Query(ge=1, le=10000)]
Size = Annotated[int, Query(ge=1, le=100)]


class Credentials(BaseModel):
    model_config = ConfigDict(extra="forbid")
    username: str = Field(min_length=3, max_length=40, pattern=r"^[A-Za-z0-9_.-]+$")
    password: str = Field(min_length=10, max_length=128)


class VendorInput(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    name: str = Field(min_length=1, max_length=120)
    pan: str | None = Field(default=None, min_length=1, max_length=40)
    gstin: str | None = Field(default=None, min_length=1, max_length=40)
    vpa: str | None = Field(default=None, min_length=1, max_length=256)
    ifsc: str | None = Field(default=None, min_length=1, max_length=40)
    pincode: str | None = Field(default=None, min_length=1, max_length=40)


def auth_limit(request: Request):
    attempts = request.app.state.auth_attempts
    key = request.client.host if request.client else "unknown"
    stamp = time.monotonic()
    if key not in attempts and len(attempts) >= 4096:
        attempts.pop(next(iter(attempts)))
    window = attempts.setdefault(key, deque())
    while window and window[0] < stamp - 60:
        window.popleft()
    if len(window) >= 15:
        raise HTTPException(429, "Too many authentication attempts; try again in one minute")
    window.append(stamp)


@router.post("/auth/register", status_code=201, tags=["Accounts"], dependencies=[Depends(auth_limit)])
async def register(body: Credentials, store: StoreDep):
    password_hash = await asyncio.to_thread(hash_password, body.password)
    try:
        user = await store.insert("users", {"username": body.username.lower(), "password_hash": password_hash})
    except DuplicateKeyError:
        raise HTTPException(409, "Username is already registered")
    return public(user)


@router.post("/auth/login", tags=["Accounts"], dependencies=[Depends(auth_limit)])
async def login(body: Credentials, store: StoreDep):
    user = await store.get("users", {"username": body.username.lower()})
    dummy = "scrypt$" + "00" * 16 + "$" + "00" * 64
    valid = await asyncio.to_thread(verify_password, body.password, user["password_hash"] if user else dummy)
    if not valid or not user:
        raise HTTPException(401, "Invalid username or password")
    token = secrets.token_urlsafe(32)
    expiry = now() + timedelta(hours=2)
    await store.insert("sessions", {"_id": token_hash(token), "owner_id": user["_id"], "expires_at": expiry})
    return {"access_token": token, "token_type": "bearer", "expires_at": expiry}


@router.post("/auth/logout", tags=["Accounts"])
async def logout(user: OwnerDep, credentials: Annotated[HTTPAuthorizationCredentials, Depends(bearer)], store: StoreDep):
    await store.db.sessions.delete_one({"_id": token_hash(credentials.credentials), "owner_id": user})
    return {"logged_out": True}


@router.get("/auth/me", tags=["Accounts"])
async def me(user: OwnerDep, store: StoreDep):
    return public(await store.get("users", {"_id": user}))


@router.post("/vendors", status_code=201, tags=["Vendors"])
async def create_vendor(body: VendorInput, user: OwnerDep, store: StoreDep):
    return public(await store.insert("vendors", {"owner_id": user, **body.model_dump()}))


@router.get("/vendors", tags=["Vendors"])
async def vendors(user: OwnerDep, store: StoreDep, page: Page = 1, page_size: Size = 20):
    return await store.page("vendors", {"owner_id": user}, page, page_size)


async def owned_vendor(id: str, user: str, store: Store) -> dict:
    vendor = await store.get("vendors", {"_id": id, "owner_id": user})
    if not vendor:
        raise HTTPException(404, "Vendor not found")
    return vendor


@router.get("/vendors/{id}", tags=["Vendors"])
async def vendor(id: str, user: OwnerDep, store: StoreDep):
    return public(await owned_vendor(id, user, store))


@router.put("/vendors/{id}", tags=["Vendors"])
async def update_vendor(id: str, body: VendorInput, user: OwnerDep, store: StoreDep):
    await owned_vendor(id, user, store)
    return public(await store.update("vendors", {"_id": id, "owner_id": user}, body.model_dump()))


@router.delete("/vendors/{id}", tags=["Vendors"])
async def delete_vendor(id: str, user: OwnerDep, store: StoreDep):
    result = await store.db.vendors.delete_one({"_id": id, "owner_id": user})
    if not result.deleted_count:
        raise HTTPException(404, "Vendor not found")
    return {"deleted": True}


@router.post("/vendors/{id}/validate", tags=["Vendors"])
async def run_validation(id: str, request: Request, user: OwnerDep, store: StoreDep):
    data = await owned_vendor(id, user, store)
    async with request.app.state.validation_slots:
        result = await validate_vendor(data, request.app.state.http)
    return public(await store.insert("validation_runs", {"owner_id": user, "vendor_id": id, **result}))


@router.get("/validation-runs", tags=["Vendors"])
async def runs(user: OwnerDep, store: StoreDep, page: Page = 1, page_size: Size = 20):
    return await store.page("validation_runs", {"owner_id": user}, page, page_size)


@router.get("/stats", tags=["Vendors"])
async def stats(user: OwnerDep, store: StoreDep):
    rows = await store.db.validation_runs.aggregate([
        {"$match": {"owner_id": user}}, {"$group": {"_id": "$status", "count": {"$sum": 1}}}])
    return {"validation_counts": {r["_id"]: r["count"] async for r in rows}}


@router.post("/batch-jobs", status_code=202, tags=["Batch jobs"])
async def upload_csv(user: OwnerDep, store: StoreDep, file: UploadFile = File(...),
                     idempotency_key: Annotated[str | None, Header(max_length=128)] = None):
    content = await file.read(1_000_001)
    try:
        rows = parse_csv(content)
    except ValueError as exc:
        raise HTTPException(422, str(exc))
    doc = {"owner_id": user, "kind": "validate_csv", "rows": rows, "total": len(rows),
           "status": "queued", "processed": 0, "attempts": 0, "content_hash": hashlib.sha256(content).hexdigest()}
    if idempotency_key:
        doc["_id"] = str(uuid5(NAMESPACE_URL, user + ":" + idempotency_key))
    try:
        return public(await store.insert("batch_jobs", doc))
    except DuplicateKeyError:
        previous = await store.get("batch_jobs", {"_id": doc["_id"], "owner_id": user})
        if not previous or previous.get("content_hash") != doc["content_hash"]:
            raise HTTPException(409, "Idempotency key was used with different content")
        return public(previous)


@router.get("/batch-jobs", tags=["Batch jobs"])
async def jobs(user: OwnerDep, store: StoreDep, page: Page = 1, page_size: Size = 20):
    return await store.page("batch_jobs", {"owner_id": user}, page, page_size)


async def owned_job(id: str, user: str, store: Store) -> dict:
    job = await store.get("batch_jobs", {"_id": id, "owner_id": user})
    if not job:
        raise HTTPException(404, "Job not found")
    return job


@router.get("/batch-jobs/{id}", tags=["Batch jobs"])
async def job(id: str, user: OwnerDep, store: StoreDep):
    return public(await owned_job(id, user, store))


@router.get("/batch-jobs/{id}/results", tags=["Batch jobs"])
async def results(id: str, user: OwnerDep, store: StoreDep, page: Page = 1, page_size: Size = 20):
    await owned_job(id, user, store)
    return await store.page("batch_rows", {"job_id": id, "owner_id": user}, page, page_size, [("row_number", 1)])


@router.get("/batch-jobs/{id}/report.csv", tags=["Batch jobs"])
async def report(id: str, user: OwnerDep, store: StoreDep):
    await owned_job(id, user, store)
    rows = await store.db.batch_rows.find({"job_id": id, "owner_id": user}).sort("row_number", 1).to_list(length=500)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["row_number", "name", "status", "error"])
    for row in rows:
        # Prevent spreadsheet formulas in user-supplied cells.
        def safe(value):
            value = str(value or "")
            return "'" + value if value.lstrip().startswith(("=", "+", "-", "@")) else value
        writer.writerow([row["row_number"], safe(row.get("name")), row["status"], safe(row.get("error"))])
    return StreamingResponse(iter([output.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": 'attachment; filename="batch-report.csv"'})


@router.post("/batch-jobs/{id}/retry", tags=["Batch jobs"])
async def retry_job(id: str, user: OwnerDep, store: StoreDep):
    result = await store.update("batch_jobs", {"_id": id, "owner_id": user,
                               "status": {"$in": ["failed", "completed_with_errors"]}},
                               {"status": "queued", "attempts": 0, "error": None})
    if not result:
        raise HTTPException(409, "Job must be failed or completed with errors to retry")
    return public(result)


@router.post("/ingestions/nav", status_code=202, tags=["NAV data"])
async def queue_nav(user: OwnerDep, store: StoreDep):
    # One global source: deduplicate active refreshes across users.
    existing = await store.get("batch_jobs", {"kind": "ingest_nav", "$or": [
        {"status": {"$in": ["queued", "running"]}}, {"created_at": {"$gt": now() - timedelta(minutes=1)}}]})
    if existing:
        raise HTTPException(409, "A NAV ingestion is already pending")
    try:
        return public(await store.insert("batch_jobs", {"owner_id": user, "kind": "ingest_nav",
                                                      "status": "queued", "attempts": 0, "processed": 0}))
    except DuplicateKeyError:
        raise HTTPException(409, "A NAV ingestion is already pending")


@router.get("/ingestion-runs", tags=["NAV data"])
async def ingestions(user: OwnerDep, store: StoreDep, page: Page = 1, page_size: Size = 20):
    return await store.page("ingestion_runs", {}, page, page_size)


@router.get("/funds/{scheme_code}/nav-history", tags=["NAV data"])
async def history(scheme_code: str, user: OwnerDep, store: StoreDep, page: Page = 1, page_size: Size = 30):
    return await store.page("nav_snapshots", {"scheme_code": scheme_code}, page, page_size, [("date", -1)])
