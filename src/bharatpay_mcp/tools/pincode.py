"""Pincode (PIN) lookup for India.

Uses the free India Post API at https://api.postalpincode.in.
Returns district, state, and the list of post offices for a 6-digit PIN.
"""
from __future__ import annotations

import re

import httpx

PIN_PATTERN = re.compile(r"^[1-9][0-9]{5}$")
API_ROOT = "https://api.postalpincode.in"
TIMEOUT = 10.0


async def lookup_pincode(pin: str) -> dict:
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
        return {"error": "PIN is required."}

    code = pin.strip()
    if not PIN_PATTERN.match(code):
        return {
            "pin": code,
            "error": (
                "Invalid PIN format. Expected: 6 digits, first digit "
                "must be 1-9 (e.g., '302001')."
            ),
        }

    url = f"{API_ROOT}/pincode/{code}"
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            r = await client.get(url)
    except httpx.RequestError as e:
        return {"pin": code, "error": f"Network error: {e}"}

    if r.status_code != 200:
        return {"pin": code, "error": f"API returned HTTP {r.status_code}."}

    payload = r.json()
    if not payload or not isinstance(payload, list):
        return {"pin": code, "error": "Unexpected response shape."}
    entry = payload[0]
    if entry.get("Status") != "Success":
        return {
            "pin": code,
            "error": entry.get("Message") or "PIN not found.",
        }

    offices = entry.get("PostOffice") or []
    if not offices:
        return {"pin": code, "error": "PIN found but no post offices listed."}

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
