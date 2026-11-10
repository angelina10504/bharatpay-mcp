"""Source tests are offline; fixture identifiers represent no real fund."""
from __future__ import annotations

import asyncio
from datetime import date
import hashlib
from pathlib import Path

import httpx
import pytest

from bharatpay_mcp.services.nav import parse_nav
from bharatpay_mcp.services.nav_source import (
    IngestionError, QualityRules, download_source, read_fixture, retain_artifact,
)

FIXTURE = Path(__file__).parent / "fixtures/nav_synthetic.txt"


class Chunks(httpx.AsyncByteStream):
    def __init__(self):
        self.reads = 0
        self.closed = False

    async def __aiter__(self):
        for _ in range(100):
            self.reads += 1
            yield b"x" * 32768

    async def aclose(self):
        self.closed = True


def test_stream_limit_without_content_length_stops_early_and_closes():
    stream = Chunks()
    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=stream))) as client:
            with pytest.raises(IngestionError) as error:
                await download_source(client, 40000)
            assert error.value.category == "source_size"
    asyncio.run(exercise())
    assert stream.reads == 2 and stream.closed


def test_hash_byte_count_and_retention_match_exact_fixture(tmp_path):
    content = FIXTURE.read_bytes()
    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=content))) as client:
            return await download_source(client, len(content))
    artifact = asyncio.run(exercise())
    assert artifact.content == read_fixture(FIXTURE, len(content)).content == content
    assert artifact.sha256 == hashlib.sha256(content).hexdigest()
    assert artifact.metadata()["source_bytes"] == len(content)
    assert artifact.metadata()["source_complete"] is True
    saved = tmp_path / retain_artifact(artifact, tmp_path)
    assert saved.read_bytes() == content
    assert read_fixture(saved, len(content)).sha256 == artifact.sha256
    assert list(tmp_path.iterdir()) == [saved]
    with pytest.raises(IngestionError, match="byte limit"):
        read_fixture(FIXTURE, len(content) - 1)


@pytest.mark.parametrize("response,category", [
    (httpx.Response(200, headers={"Content-Length": "9999"}), "source_size"),
    (httpx.Response(503), "source_http"),
    (httpx.Response(200, headers={"Content-Encoding": "unexpected"}), "source_encoding"),
])
def test_source_errors_are_categorized_without_echoing_source(response, category):
    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response)) as client:
            with pytest.raises(IngestionError) as error:
                await download_source(client, 100)
            assert error.value.category == category
            assert "https://" not in str(error.value)
    asyncio.run(exercise())


@pytest.mark.parametrize("exception,category", [(httpx.ReadTimeout, "source_timeout"), (httpx.ConnectError, "source_network")])
def test_request_errors_are_sanitized(exception, category):
    def fail(request):
        raise exception("secret-must-not-appear", request=request)
    async def exercise():
        async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
            with pytest.raises(IngestionError) as error:
                await download_source(client, 100)
            assert error.value.category == category
            assert "secret" not in str(error.value)
    asyncio.run(exercise())


def test_quality_metadata_and_configurable_rules():
    artifact = read_fixture(FIXTURE, 1000)
    rows, rejected = parse_nav(artifact.text())
    rules = QualityRules(max_rejected_fraction=0.5)
    summary = rules.summarize(rows, rejected)
    rules.validate(summary, date(2026, 11, 3))
    assert (summary["parsed_count"], summary["accepted"], summary["rejected_count"]) == (3, 2, 1)
    assert (summary["source_date_min"], summary["source_date_max"]) == ("2026-11-02", "2026-11-03")
    for strict in (QualityRules(min_records=3), QualityRules(max_rejected_fraction=0.2),
                   QualityRules(max_source_age_days=0), QualityRules(max_future_days=0)):
        with pytest.raises(IngestionError):
            strict.validate(summary, date(2026, 11, 2) if strict.max_future_days == 0 else date(2026, 11, 3))


@pytest.mark.parametrize("feed", ["<html>unavailable</html>", "990001;-;-;Bad;NaN;03-Nov-2026\n", ""])
def test_zero_valid_rows_are_observable_but_never_accepted(feed):
    rows, rejected = parse_nav(feed, allow_empty=True)
    summary = QualityRules().summarize(rows, rejected)
    assert summary["accepted"] == 0
    with pytest.raises(IngestionError):
        QualityRules().validate(summary, date(2026, 11, 3))
    with pytest.raises(ValueError):
        parse_nav(feed)


def test_duplicate_keys_are_deterministically_rejected():
    row = "990001;-;-;Synthetic;1.25;03-Nov-2026\n"
    rows, rejected = parse_nav(row + row.replace("1.25", "99.99"))
    assert len(rows) == len(rejected) == 1
    assert rows[0]["nav"] == "1.25"


@pytest.mark.parametrize("values", [{"max_rejected_fraction": float("nan")}, {"min_records": 0}, {"max_future_days": -1}])
def test_invalid_quality_configuration_rejected(values):
    with pytest.raises(ValueError):
        QualityRules(**values)
