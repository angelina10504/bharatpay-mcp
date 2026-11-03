"""FastAPI adapter over the same offline functions used by MCP.

Start with: uvicorn bharatpay_mcp.api.app:app --reload
"""
from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio
import os
from pathlib import Path

import httpx
from fastapi import APIRouter, FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pymongo.errors import PyMongoError

from bharatpay_mcp.api.workbench import router as workbench_router
from bharatpay_mcp.services.nav import NavCache
from bharatpay_mcp.storage import Store
from bharatpay_mcp.tools.ifsc import lookup_ifsc
from bharatpay_mcp.tools.pincode import lookup_pincode

from bharatpay_mcp import __version__
from bharatpay_mcp.api.schemas import GSTINInput, INRInput, PANInput, UPIInput
from bharatpay_mcp.tools.gstin import validate_gstin
from bharatpay_mcp.tools.inr import format_inr
from bharatpay_mcp.tools.pan import validate_pan
from bharatpay_mcp.tools.upi import validate_upi_vpa


def create_app() -> FastAPI:
    """Create an independent app without importing or starting the MCP server."""
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.store = None
        app.state.nav = NavCache()
        app.state.auth_attempts = {}
        app.state.validation_slots = asyncio.Semaphore(5)
        async with httpx.AsyncClient(limits=httpx.Limits(max_connections=20, max_keepalive_connections=10)) as client:
            app.state.http = client
            if os.getenv("MONGO_URL"):
                app.state.store = Store(os.environ["MONGO_URL"])
            try:
                if app.state.store:
                    await app.state.store.start()
                yield
            finally:
                if app.state.store:
                    await app.state.store.close()

    app = FastAPI(
        lifespan=lifespan,
        title="BharatPay Workbench API",
        version=__version__,
        description=(
            "Fintech validation, enrichment, private vendor records and durable batches. "
            "Format/checksum checks do not verify identity, registration, "
            "or whether a UPI address is active. Use synthetic data in this local demo."
        ),
    )

    @app.get("/health", tags=["Health"])
    def health() -> dict[str, str]:
        return {"status": "ok", "version": __version__}

    router = APIRouter(prefix="/api/v1", tags=["Offline utilities"])

    @router.post("/validations/pan")
    def pan(body: PANInput) -> dict:
        """Check format and extract entity metadata; not holder verification."""
        return validate_pan(body.pan)

    @router.post("/validations/gstin")
    def gstin(body: GSTINInput) -> dict:
        """Check format and checksum; not registration verification."""
        return validate_gstin(body.gstin)

    @router.post("/validations/upi")
    def upi(body: UPIInput) -> dict:
        """Check format and identify a known handle; not active-account verification."""
        return validate_upi_vpa(body.vpa)

    @router.post("/format/inr")
    def inr(body: INRInput) -> dict:
        """Round half up to integer paise before formatting."""
        result = format_inr(body.amount, paise=body.paise)
        if "error" in result:
            raise HTTPException(status_code=422, detail=result["error"])
        return result

    def lookup_result(result: dict) -> dict:
        if "error" in result:
            code = result.get("error_code")
            status = {"invalid_input": 422, "not_found": 404, "provider_timeout": 504}.get(code, 502)
            raise HTTPException(status, detail=result)
        return result

    @router.get("/lookups/ifsc/{code}", tags=["Lookups"])
    async def ifsc_lookup(code: str) -> dict:
        return lookup_result(await lookup_ifsc(code, app.state.http))

    @router.get("/lookups/pincode/{code}", tags=["Lookups"])
    async def pincode_lookup(code: str) -> dict:
        return lookup_result(await lookup_pincode(code, app.state.http))

    @router.get("/funds/search", tags=["Lookups"])
    async def fund_search(q: str = Query(min_length=1, max_length=120)) -> dict:
        return lookup_result(await app.state.nav.lookup(q, app.state.http))

    @app.get("/ready", tags=["Health"])
    async def ready():
        if app.state.store:
            await app.state.store.db.command("ping")
        return {"status": "ok", "persistence": app.state.store is not None}

    @app.exception_handler(PyMongoError)
    async def database_error(request, exc):
        return JSONResponse(status_code=503, content={"detail": "Database unavailable; retry later"})

    static = Path(__file__).parent / "static"
    app.mount("/assets", StaticFiles(directory=static), name="assets")

    @app.get("/", include_in_schema=False)
    def dashboard():
        return FileResponse(static / "index.html")

    app.include_router(workbench_router)
    app.include_router(router)
    return app


app = create_app()
