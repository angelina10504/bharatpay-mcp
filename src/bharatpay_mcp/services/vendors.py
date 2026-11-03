"""Validate and enrich vendor inputs with bounded external I/O."""
from __future__ import annotations

import asyncio

import httpx

from bharatpay_mcp.tools.pan import validate_pan
from bharatpay_mcp.tools.gstin import validate_gstin
from bharatpay_mcp.tools.upi import validate_upi_vpa
from bharatpay_mcp.tools.ifsc import lookup_ifsc
from bharatpay_mcp.tools.pincode import lookup_pincode


async def validate_vendor(data: dict, client: httpx.AsyncClient) -> dict:
    checks = {}
    for field, validator in [("pan", validate_pan), ("gstin", validate_gstin), ("vpa", validate_upi_vpa)]:
        if data.get(field):
            checks[field] = validator(data[field])
    fields, calls = [], []
    for field, lookup in [("ifsc", lookup_ifsc), ("pincode", lookup_pincode)]:
        if data.get(field):
            fields.append(field)
            calls.append(lookup(data[field], client))
    checks.update(zip(fields, await asyncio.gather(*calls)))
    if any(r.get("error_code", "").startswith("provider_") for r in checks.values()):
        status = "provider_error"
    elif any(r.get("valid") is False or "error" in r for r in checks.values()):
        status = "invalid"
    else:
        status = "valid" if checks else "not_checked"
    return {"status": status, "checks": checks,
            "note": "Format and lookup checks only; not identity or account verification."}
