"""MCP adapter for shared AMFI parsing and cache service."""
from __future__ import annotations

import httpx

from bharatpay_mcp.services.nav import AMFI_URL, NavCache

CACHE_TTL_SECONDS = 6 * 60 * 60
_cache = NavCache(CACHE_TTL_SECONDS)


async def get_mutual_fund_nav(query: str, client: httpx.AsyncClient | None = None) -> dict:
    """Find a scheme by code or name; stale results are explicitly marked."""
    return await _cache.lookup(query, client)
