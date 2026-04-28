"""IFSC code lookup — uses Razorpay's free public IFSC API.

Source: https://github.com/razorpay/ifsc (MIT-licensed).
Endpoint: https://ifsc.razorpay.com/{IFSC}
"""
from __future__ import annotations

import re

import httpx

IFSC_PATTERN = re.compile(r"^[A-Z]{4}0[A-Z0-9]{6}$")
API_ROOT = "https://ifsc.razorpay.com"
TIMEOUT = 10.0


def _validate_format(ifsc: str) -> str | None:
    """Return a friendly error message if `ifsc` is malformed, else None."""
    if not ifsc:
        return "IFSC code is required."
    ifsc = ifsc.strip().upper()
    if len(ifsc) != 11:
        return f"IFSC code must be 11 characters (got {len(ifsc)})."
    if not IFSC_PATTERN.match(ifsc):
        return (
            "Invalid IFSC format. Expected: 4 letters (bank) + '0' + "
            "6 alphanumeric characters (branch). Example: HDFC0000123."
        )
    return None


async def lookup_ifsc(ifsc: str) -> dict:
    """Look up bank and branch details for an Indian IFSC code.

    Args:
        ifsc: 11-character IFSC code (e.g., 'KKBK0000261').

    Returns:
        Dict with bank, branch, address, city, district, state, contact,
        MICR/SWIFT codes, and supported payment rails (NEFT/RTGS/IMPS/UPI).
        On failure: {"error": "...", "ifsc": "..."}.
    """
    err = _validate_format(ifsc)
    if err:
        return {"error": err, "ifsc": ifsc}

    code = ifsc.strip().upper()
    url = f"{API_ROOT}/{code}"

    try:
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            r = await client.get(url)
    except httpx.RequestError as e:
        return {"error": f"Network error reaching IFSC API: {e}", "ifsc": code}

    if r.status_code == 404:
        return {
            "error": (
                "IFSC code not found. The format is valid but this code does "
                "not exist in the RBI dataset."
            ),
            "ifsc": code,
        }
    if r.status_code != 200:
        return {
            "error": f"IFSC API returned HTTP {r.status_code}.",
            "ifsc": code,
        }

    data = r.json()
    return {
        "ifsc": data.get("IFSC"),
        "bank": data.get("BANK"),
        "bank_code": data.get("BANKCODE"),
        "branch": data.get("BRANCH"),
        "address": data.get("ADDRESS"),
        "city": data.get("CITY"),
        "district": data.get("DISTRICT"),
        "state": data.get("STATE"),
        "centre": data.get("CENTRE"),
        "contact": data.get("CONTACT"),
        "micr": data.get("MICR") or None,
        "swift": data.get("SWIFT") or None,
        "iso3166": data.get("ISO3166"),
        "supports": {
            "neft": bool(data.get("NEFT")),
            "rtgs": bool(data.get("RTGS")),
            "imps": bool(data.get("IMPS")),
            "upi": bool(data.get("UPI")),
        },
        "source": "Razorpay IFSC API (https://ifsc.razorpay.com)",
    }
