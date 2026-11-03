"""API contracts and validation behavior; no upstream connections allowed."""
from __future__ import annotations

import socket

import pytest
from fastapi.testclient import TestClient

from bharatpay_mcp.api.app import create_app
from bharatpay_mcp.tools.gstin import _gstin_checksum


@pytest.fixture
def client(monkeypatch):
    def no_network(*args, **kwargs):
        raise AssertionError("Unit tests must not connect to the network")

    monkeypatch.setattr(socket.socket, "connect", no_network)
    with TestClient(create_app()) as test_client:
        yield test_client


def test_health_and_openapi(client):
    assert client.get("/health").json()["status"] == "ok"
    schema = client.get("/openapi.json").json()
    assert "/api/v1/validations/pan" in schema["paths"]


def test_pan_normalizes_and_preserves_verification_limit(client):
    response = client.post("/api/v1/validations/pan", json={"pan": " abcpe1234f "})
    assert response.status_code == 200
    assert response.json()["pan"] == "ABCPE1234F"
    assert "identity" in response.json()["note"]


def test_domain_failure_is_a_result(client):
    response = client.post("/api/v1/validations/pan", json={"pan": "bad"})
    assert response.status_code == 200
    assert response.json()["valid"] is False


@pytest.mark.parametrize("body", [{}, {"pan": 123}, {"pan": " "}, {"pan": "x", "extra": True}])
def test_bad_request_shape_is_422(client, body):
    assert client.post("/api/v1/validations/pan", json=body).status_code == 422


def test_gstin_checksum_and_mutation(client):
    prefix = "29AABCT1332L1Z"
    code = prefix + _gstin_checksum(prefix)
    assert client.post("/api/v1/validations/gstin", json={"gstin": code}).json()["valid"]
    wrong = code[:-1] + ("0" if code[-1] != "0" else "1")
    assert not client.post("/api/v1/validations/gstin", json={"gstin": wrong}).json()["valid"]


def test_upi_unknown_handle_is_only_format_valid(client):
    data = client.post("/api/v1/validations/upi", json={"vpa": "demo@unknown"}).json()
    assert data["valid"]
    assert data["psp"] == "Unknown"
    assert "Activeness" in data["note"]


def test_money_rounds_before_display(client):
    data = client.post("/api/v1/format/inr", json={"amount": "1.999"}).json()
    assert data["with_symbol"] == "₹2"
    assert data["paise"] == 200
    assert data["rupees"] == 2


@pytest.mark.parametrize("amount", ["NaN", "Infinity", "-Infinity", "1e16", "oops"])
def test_invalid_money_is_422(client, amount):
    assert client.post("/api/v1/format/inr", json={"amount": amount}).status_code == 422


def test_paise_flag_requires_boolean(client):
    assert client.post("/api/v1/format/inr", json={"amount": 100, "paise": "yes"}).status_code == 422
