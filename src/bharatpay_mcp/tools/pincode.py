"""Pincode (PIN) lookup for India.

Uses the free India Post API at https://api.postalpincode.in.
Returns district, state, and the list of post offices for a 6-digit PIN.
"""
from __future__ import annotations

import re

import httpx
from bharatpay_mcp.tools._http import fetch, provider_error

PIN_PATTERN = re.compile(r"^[1-9][0-9]{5}$")
API_ROOT = "https://api.postalpincode.in"
TIMEOUT = 10.0


async def lookup_pincode(pin: str, client: httpx.AsyncClient | None = None) -> dict:
    """Look up postal data for an Indian PIN code.

    Args:
        pin: 6-digit PIN (e.g., '302001' for Jaipur).

    Returns:
        {
          "pin": "302001",
          "state": "Rajasthan",
          "district": "Jaipur",
          "post_offices": [{name, branch_type, delivery_status, ...}, ...],
          "count": N
        }
        On failure: {"error": "...", "pin": "..."}
    """
    if not pin:
        return {"error": "PIN is required.", "error_code": "invalid_input"}

    code = pin.strip()
    if not PIN_PATTERN.match(code):
        return {
            "pin": code,
            "error_code": "invalid_input",
            "error": (
                "Invalid PIN format. Expected: 6 digits, first digit "
                "must be 1-9 (e.g., '302001')."
            ),
        }

    url = f"{API_ROOT}/pincode/{code}"
    try:
        r = await fetch(url, client)
        payload = r.json()
        if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
            raise ValueError("Unexpected pincode payload")
        entry = payload[0]
        if entry.get("Status") != "Success":
            return {"pin": code, "error": "PIN not found.", "error_code": "not_found"}
        offices = entry.get("PostOffice")
        if not isinstance(offices, list) or not offices or not all(isinstance(p, dict) for p in offices):
            raise ValueError("Unexpected office records")
    except (httpx.HTTPError, ValueError) as exc:
        return {"pin": code, **provider_error(exc)}

    first = offices[0]
    return {
        "pin": code,
        "state": first.get("State"),
        "district": first.get("District"),
        "country": first.get("Country", "India"),
        "post_offices": [
            {
                "name": po.get("Name"),
                "branch_type": po.get("BranchType"),
                "delivery_status": po.get("DeliveryStatus"),
                "circle": po.get("Circle"),
                "region": po.get("Region"),
                "division": po.get("Division"),
                "block": po.get("Block"),
            }
            for po in offices
        ],
        "count": len(offices),
        "source": "India Post API (api.postalpincode.in)",
    }
