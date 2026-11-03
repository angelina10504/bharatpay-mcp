"""Shared, injectable HTTP access; MCP callers can still omit the client."""
from __future__ import annotations

import httpx


async def fetch(url: str, client: httpx.AsyncClient | None = None) -> httpx.Response:
    if client is not None:
        response = await client.get(url, timeout=30, follow_redirects=True)
    else:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as owned:
            response = await owned.get(url)
    response.raise_for_status()
    return response


def provider_error(exc: Exception) -> dict:
    if isinstance(exc, httpx.TimeoutException):
        return {"error": "The data provider timed out. Try again later.", "error_code": "provider_timeout"}
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code == 404:
        return {"error": "No matching record found by the data provider.", "error_code": "not_found"}
    return {"error": "The data provider is unavailable or returned invalid data.", "error_code": "provider_failure"}
