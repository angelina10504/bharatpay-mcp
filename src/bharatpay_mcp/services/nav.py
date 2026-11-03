"""AMFI parsing and concurrency-safe process-local caching."""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import httpx

from bharatpay_mcp.tools._http import fetch

AMFI_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"


def parse_nav(text: str) -> tuple[list[dict], list[dict]]:
    """Return normalized records and rejected numeric rows, without I/O."""
    records, rejected = [], []
    for number, raw in enumerate(text.splitlines(), 1):
        parts = [part.strip() for part in raw.split(";")]
        if not parts[0].isdigit():
            continue  # source header and category headings
        try:
            if len(parts) not in {6, 8} or not parts[3]:
                raise ValueError("Expected six or eight fields and a scheme name")
            nav = Decimal(parts[-2])
            if not nav.is_finite() or nav < 0:
                raise ValueError("NAV must be finite and nonnegative")
            date = datetime.strptime(parts[-1], "%d-%b-%Y").date().isoformat()
            records.append({
                "scheme_code": parts[0], "isin_payout": None if parts[1] == "-" else parts[1],
                "isin_reinvest": None if parts[2] == "-" else parts[2],
                "scheme_name": parts[3], "nav": str(nav), "date": date,
                "plan": parts[4] if len(parts) == 8 else None,
                "option": parts[5] if len(parts) == 8 else None,
            })
        except (ValueError, InvalidOperation) as exc:
            rejected.append({"line": number, "reason": str(exc)})
    if not records:
        raise ValueError("AMFI returned no usable NAV records")
    return records, rejected


def validate_query(query: str) -> str | None:
    if not isinstance(query, str) or not query.strip():
        return "Provide a numeric scheme code or a name fragment of at least 3 characters."
    q = query.strip()
    if len(q) > 120 or (not q.isdigit() and len(q) < 3):
        return "Use a numeric scheme code or a 3–120 character name fragment."
    return None


class NavCache:
    def __init__(self, ttl: int = 21600):
        self.ttl = ttl
        self.records: dict[str, dict] = {}
        self.fetched_at: str | None = None
        self.refreshed = 0.0
        self.retry_after = 0.0
        self.lock = asyncio.Lock()

    async def lookup(self, query: str, client: httpx.AsyncClient | None = None) -> dict:
        error = validate_query(query)
        if error:
            return {"error": error, "error_code": "invalid_input"}
        stale = False
        try:
            if time.monotonic() - self.refreshed >= self.ttl or not self.records:
                async with self.lock:
                    if time.monotonic() < self.retry_after and self.records:
                        stale = True
                    elif time.monotonic() - self.refreshed >= self.ttl or not self.records:
                        response = await fetch(AMFI_URL, client)
                        if len(response.content) > 15_000_000:
                            raise ValueError("AMFI response exceeds size limit")
                        records, _ = await asyncio.to_thread(parse_nav, response.text)
                        self.records = {row["scheme_code"]: row for row in records}
                        self.fetched_at = datetime.now(timezone.utc).isoformat()
                        self.refreshed = time.monotonic()
        except (httpx.HTTPError, ValueError):
            if not self.records:
                return {"error": "AMFI data is unavailable.", "error_code": "provider_failure"}
            self.retry_after = time.monotonic() + 60
            stale = True
        q = query.strip()
        matches = ([self.records[q]] if q in self.records else
                   [r for r in self.records.values() if q.lower() in r["scheme_name"].lower()])
        meta = {"source": "AMFI daily NAV", "fetched_at": self.fetched_at, "stale": stale}
        if not matches:
            return {"error": "No scheme matched the query.", "error_code": "not_found", **meta}
        if len(matches) == 1:
            return {**matches[0], **meta}
        return {"query": q, "match_count": len(matches), "matches": matches[:10], **meta}
