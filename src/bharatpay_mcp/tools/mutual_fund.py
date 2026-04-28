"""Indian mutual fund NAV lookup using AMFI's free daily data file.

AMFI publishes daily NAVs for all Indian mutual fund schemes at
https://www.amfiindia.com/spages/NAVAll.txt as a semicolon-delimited file.
We cache it for the duration of the server process (refreshed every 6h).
"""
from __future__ import annotations

import time
from typing import Any

import httpx

AMFI_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"
CACHE_TTL_SECONDS = 6 * 60 * 60  # 6 hours
TIMEOUT = 30.0  # the file is ~6 MB

_cache: dict[str, Any] = {"fetched_at": 0.0, "schemes": {}, "as_of_date": None}


async def _refresh_cache() -> None:
    """Download the AMFI NAV file and rebuild the in-memory index."""
    # follow_redirects=True future-proofs against further URL moves
    async with httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True) as client:
        r = await client.get(AMFI_URL)
    r.raise_for_status()

    schemes: dict[str, dict] = {}
    last_date = None
    for raw_line in r.text.splitlines():
        line = raw_line.strip()
        if not line or ";" not in line:
            continue
        # Skip the header and category lines (those don't have numeric scheme codes)
        parts = line.split(";")
        if len(parts) < 6:
            continue
        scheme_code, isin_payout, isin_reinvest, name, nav, date = parts[:6]
        if not scheme_code.isdigit():
            continue
        try:
            nav_value = float(nav)
        except ValueError:
            nav_value = None
        schemes[scheme_code] = {
            "scheme_code": scheme_code,
            "scheme_name": name,
            "isin_payout": isin_payout if isin_payout != "-" else None,
            "isin_reinvest": isin_reinvest if isin_reinvest != "-" else None,
            "nav": nav_value,
            "date": date,
        }
        last_date = date

    _cache["schemes"] = schemes
    _cache["fetched_at"] = time.time()
    _cache["as_of_date"] = last_date


async def _ensure_cache() -> None:
    if (
        not _cache["schemes"]
        or time.time() - _cache["fetched_at"] > CACHE_TTL_SECONDS
    ):
        await _refresh_cache()


async def get_mutual_fund_nav(query: str) -> dict:
    """Get the latest NAV for an Indian mutual fund scheme.

    Args:
        query: Either an AMFI scheme code (e.g., '118989') or a name fragment
               to search by (e.g., 'parag parikh flexi cap').

    Returns:
        Single match: full scheme details (code, name, ISIN, NAV, date).
        Multiple matches: top 10 candidates so the LLM can disambiguate.
        No match: helpful error.
    """
    if not query or not query.strip():
        return {"error": "Provide a scheme code or name fragment."}

    try:
        await _ensure_cache()
    except httpx.HTTPError as e:
        return {"error": f"Failed to fetch AMFI data: {e}"}

    schemes = _cache["schemes"]
    q = query.strip()

    # Exact scheme-code match first
    if q.isdigit() and q in schemes:
        return {**schemes[q], "as_of_date": _cache["as_of_date"]}

    # Otherwise fuzzy substring match on the name
    needle = q.lower()
    matches = [s for s in schemes.values() if needle in s["scheme_name"].lower()]

    if not matches:
        return {
            "error": f"No mutual fund found matching '{query}'.",
            "as_of_date": _cache["as_of_date"],
        }

    if len(matches) == 1:
        return {**matches[0], "as_of_date": _cache["as_of_date"]}

    # Multiple matches — return top 10
    return {
        "query": query,
        "match_count": len(matches),
        "as_of_date": _cache["as_of_date"],
        "matches": [
            {
                "scheme_code": m["scheme_code"],
                "scheme_name": m["scheme_name"],
                "nav": m["nav"],
                "date": m["date"],
            }
            for m in matches[:10]
        ],
        "note": (
            "Multiple schemes matched. Use the scheme_code of the desired "
            "match for an exact lookup."
        ),
        "source": "AMFI India daily NAV (amfiindia.com)",
    }
