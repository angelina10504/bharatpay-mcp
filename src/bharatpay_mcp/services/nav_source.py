"""Bounded AMFI source acquisition, reproducible artifacts and pure quality rules."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
import hashlib
import math
import os
from pathlib import Path
import tempfile

import httpx

from bharatpay_mcp.services.nav import AMFI_URL
from bharatpay_mcp.storage import now


class IngestionError(RuntimeError):
    """Only fixed, safe messages may cross this diagnostic boundary."""

    def __init__(self, category: str, diagnostic: str):
        self.category = category
        super().__init__(diagnostic)

    @property
    def retryable(self):
        return self.category in {"coordination_busy", "source_timeout", "source_network", "source_http",
                                 "database", "lease_lost", "interrupted"}


@dataclass(frozen=True)
class QualityRules:
    min_records: int = 1
    max_rejected_fraction: float = 1.0  # compatibility; tighten for deployment
    max_source_age_days: int | None = None
    max_future_days: int | None = None

    def __post_init__(self):
        if self.min_records < 1 or not math.isfinite(self.max_rejected_fraction) or not 0 <= self.max_rejected_fraction <= 1:
            raise ValueError("Invalid NAV quality limits")
        if any(value is not None and value < 0 for value in (self.max_source_age_days, self.max_future_days)):
            raise ValueError("NAV date limits must be nonnegative")

    @classmethod
    def from_env(cls):
        def optional(name):
            value = os.getenv(name)
            return int(value) if value else None
        return cls(int(os.getenv("NAV_MIN_RECORDS", "1")),
                   float(os.getenv("NAV_MAX_REJECTED_FRACTION", "1")),
                   optional("NAV_MAX_SOURCE_AGE_DAYS"), optional("NAV_MAX_FUTURE_DAYS"))

    def summarize(self, rows: list[dict], rejected: list[dict]) -> dict:
        dates = [date.fromisoformat(row["date"]) for row in rows]
        summary = {"parsed_count": len(rows) + len(rejected), "accepted": len(rows),
                   "rejected_count": len(rejected), "rejected_rows": rejected[:100],
                   "source_date_min": min(dates).isoformat() if dates else None,
                   "source_date_max": max(dates).isoformat() if dates else None,
                   "quality_rules": asdict(self)}
        return summary

    def validate(self, summary: dict, as_of: date):
        total = summary["parsed_count"]
        if summary["accepted"] < self.min_records:
            raise IngestionError("quality", "Source contains too few valid NAV records")
        if total and summary["rejected_count"] / total > self.max_rejected_fraction:
            raise IngestionError("quality", "Source rejected-row fraction exceeds configured limit")
        oldest, newest = (date.fromisoformat(summary[key]) for key in ("source_date_min", "source_date_max"))
        if self.max_source_age_days is not None and (as_of - oldest).days > self.max_source_age_days:
            raise IngestionError("quality", "Source contains NAV dates older than configured limit")
        if self.max_future_days is not None and (newest - as_of).days > self.max_future_days:
            raise IngestionError("quality", "Source contains future NAV dates beyond configured limit")


@dataclass(frozen=True)
class SourceArtifact:
    content: bytes
    sha256: str
    fetched_at: datetime

    @classmethod
    def of(cls, content: bytes):
        return cls(content, hashlib.sha256(content).hexdigest(), now())

    def metadata(self):
        return {"source_sha256": self.sha256, "source_bytes": len(self.content),
                "fetched_at": self.fetched_at, "source_complete": True}

    def text(self):
        try:
            return self.content.decode("utf-8-sig")
        except UnicodeDecodeError:
            raise IngestionError("parse", "Source is not valid UTF-8 text") from None


def byte_limit() -> int:
    value = int(os.getenv("NAV_MAX_SOURCE_BYTES", "15000000"))
    if value < 1:
        raise ValueError("NAV_MAX_SOURCE_BYTES must be positive")
    return value


async def download_source(client: httpx.AsyncClient, limit: int) -> SourceArtifact:
    """Enforce an entity-byte limit while streaming, regardless of length header."""
    content = bytearray()
    digest = hashlib.sha256()
    try:
        async with client.stream("GET", AMFI_URL, timeout=30, follow_redirects=True,
                                 headers={"Accept-Encoding": "identity"}) as response:
            response.raise_for_status()
            if response.headers.get("content-encoding", "identity").lower() != "identity":
                raise IngestionError("source_encoding", "Compressed source response is not supported")
            length = response.headers.get("content-length", "")
            if length.isdigit() and int(length) > limit:
                raise IngestionError("source_size", "Source exceeds configured byte limit")
            async for chunk in response.aiter_bytes(chunk_size=32768):
                if len(content) + len(chunk) > limit:
                    raise IngestionError("source_size", "Source exceeds configured byte limit")
                digest.update(chunk)
                content.extend(chunk)
    except httpx.TimeoutException:
        raise IngestionError("source_timeout", "Source request timed out") from None
    except httpx.HTTPStatusError:
        raise IngestionError("source_http", "Source returned an unsuccessful HTTP status") from None
    except httpx.HTTPError:
        raise IngestionError("source_network", "Source connection failed") from None
    return SourceArtifact(bytes(content), digest.hexdigest(), now())


def read_fixture(path: Path, limit: int) -> SourceArtifact:
    try:
        with path.open("rb") as source:
            content = source.read(limit + 1)
    except OSError:
        raise IngestionError("source_file", "Saved source file could not be read") from None
    if len(content) > limit:
        raise IngestionError("source_size", "Saved source exceeds configured byte limit")
    return SourceArtifact.of(content)


def retain_artifact(artifact: SourceArtifact, directory: Path) -> str:
    """Optional local content-addressed retention; never save a partial source."""
    temporary = None
    try:
        directory.mkdir(parents=True, exist_ok=True)
        target = directory / (artifact.sha256 + ".txt")
        with tempfile.NamedTemporaryFile(dir=directory, delete=False) as output:
            temporary = Path(output.name)
            output.write(artifact.content)
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(target)
        return target.name  # no host paths in the API
    except OSError:
        raise IngestionError("artifact", "Raw source artifact could not be retained") from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
