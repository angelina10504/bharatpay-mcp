from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from bharatpay_mcp.api.app import create_app
from bharatpay_mcp.security import hash_password, verify_password, token_hash
from bharatpay_mcp.services.batches import parse_csv


def test_password_hashes_are_salted_and_verifiable():
    a, b = hash_password("demo password"), hash_password("demo password")
    assert a != b
    assert verify_password("demo password", a)
    assert not verify_password("wrong", a)
    assert token_hash("token") != "token"


@pytest.mark.parametrize("content", [b"", b"other\nx", b"name,name\na,b", b"name\na,b", b"name\n", b"\xff"])
def test_malformed_csv_rejected(content):
    with pytest.raises(ValueError):
        parse_csv(content)


def test_csv_limits_and_bom():
    assert parse_csv(b"\xef\xbb\xbfname,pan\nDemo,ABCPE1234F\n")[0]["name"] == "Demo"
    with pytest.raises(ValueError):
        parse_csv(b"name\n" + b"Demo\n" * 501)
    with pytest.raises(ValueError):
        parse_csv(b"x" * 1_000_001)


def test_missing_storage_is_explicit_not_in_memory_success(monkeypatch):
    monkeypatch.delenv("MONGO_URL", raising=False)
    with TestClient(create_app()) as client:
        assert client.get("/api/v1/vendors").status_code == 503
        assert client.get("/ready").json()["persistence"] is False
