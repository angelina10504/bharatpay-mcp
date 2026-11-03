"""Provider contracts, cache failure behavior and async concurrency, offline."""
from __future__ import annotations

import asyncio
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from bharatpay_mcp.api.app import create_app
from bharatpay_mcp.services.nav import NavCache, parse_nav
from bharatpay_mcp.tools.ifsc import lookup_ifsc
from bharatpay_mcp.tools.pincode import lookup_pincode

NAV = "Scheme Code;ISIN;ISIN;Scheme Name;Net Asset Value;Date\n123456;-;-;Demo Fund;10.25;02-Nov-2026\n"


def test_parse_nav_dates_and_rejections():
    rows, rejected = parse_nav(NAV + "123457;-;-;Bad Fund;NaN;02-Nov-2026\n")
    assert rows[0]["date"] == "2026-11-02"
    assert rows[0]["nav"] == "10.25"
    assert len(rejected) == 1


def test_empty_parse_cannot_replace_cache():
    with pytest.raises(ValueError):
        parse_nav("<html>Provider error</html>")


def test_current_eight_column_amfi_format():
    rows, rejected = parse_nav("123456;-;-;Demo Fund;Direct;Growth;12.345;03-Nov-2026\n")
    assert not rejected
    assert rows[0]["plan"] == "Direct"
    assert rows[0]["option"] == "Growth"
    assert rows[0]["nav"] == "12.345"
    assert rows[0]["date"] == "2026-11-03"


def test_concurrent_cache_miss_fetches_once_and_serves_stale_on_failure():
    async def exercise():
        calls = 0
        async def handler(request):
            nonlocal calls
            calls += 1
            await asyncio.sleep(0)
            return httpx.Response(200 if calls == 1 else 503, text=NAV)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            cache = NavCache()
            results = await asyncio.gather(*(cache.lookup("123456", client) for _ in range(10)))
            assert calls == 1
            assert all(not r["stale"] for r in results)
            cache.refreshed = time.monotonic() - cache.ttl - 1
            stale = await cache.lookup("123456", client)
            assert stale["stale"] and stale["nav"] == "10.25"
            await cache.lookup("123456", client)
            assert calls == 2
    asyncio.run(exercise())


def test_invalid_query_never_fetches():
    async def exercise():
        def forbidden(request):
            raise AssertionError("Invalid input must not fetch")
        async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
            assert (await NavCache().lookup("ab", client))["error_code"] == "invalid_input"
            assert (await lookup_ifsc("bad", client))["error_code"] == "invalid_input"
            assert (await lookup_pincode("bad", client))["error_code"] == "invalid_input"
    asyncio.run(exercise())


@pytest.mark.parametrize("payload", [None, [], [None], [{"Status": "Success", "PostOffice": [None]}]])
def test_pincode_bad_payload_is_safe(payload):
    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json=payload))) as client:
            assert (await lookup_pincode("302001", client))["error_code"] == "provider_failure"
    asyncio.run(exercise())


@pytest.mark.parametrize("status,expected", [(404, 404), (429, 502), (503, 502)])
def test_rest_provider_status_mapping(status, expected):
    with TestClient(create_app()) as client:
        old = client.app.state.http
        client.app.state.http = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(status)))
        assert client.get("/api/v1/lookups/ifsc/HDFC0000123").status_code == expected
        asyncio.run(client.app.state.http.aclose())
        client.app.state.http = old


def test_timeout_is_504_and_non_json_is_502():
    for handler, status in [
        (lambda r: (_ for _ in ()).throw(httpx.ReadTimeout("timeout", request=r)), 504),
        (lambda r: httpx.Response(200, text="not json"), 502),
    ]:
        with TestClient(create_app()) as client:
            old = client.app.state.http
            client.app.state.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
            assert client.get("/api/v1/lookups/ifsc/HDFC0000123").status_code == status
            asyncio.run(client.app.state.http.aclose())
            client.app.state.http = old
