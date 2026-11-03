"""Regression cases for sign, rounding carries, and invalid numeric inputs."""
from __future__ import annotations

from decimal import Decimal

import pytest

from bharatpay_mcp.tools.inr import format_inr


@pytest.mark.parametrize(
    "amount, display, paise",
    [
        (-0.5, "-0.50", -50),
        (1.999, "2", 200),
        (-1.999, "-2", -200),
        (999.999, "1,000", 100000),
        (Decimal("1.005"), "1.01", 101),
        (Decimal("-1.005"), "-1.01", -101),
        (Decimal("-0.004"), "0", 0),
    ],
)
def test_rounding_is_consistent(amount, display, paise):
    result = format_inr(amount)
    assert result["indian_format"] == display
    assert result["paise"] == paise
    assert result["rupees"] == paise / 100


@pytest.mark.parametrize("amount", [float("nan"), float("inf"), float("-inf"), "bad", None, 10**16])
def test_invalid_amount_returns_error(amount):
    assert "error" in format_inr(amount)


def test_fractional_paise_round_half_up():
    assert format_inr(Decimal("100.5"), paise=True)["paise"] == 101


def test_fractional_words_preserve_sign_and_value():
    assert format_inr(-0.5)["words"] == "Minus 0.5"
