"""Unit tests for offline validators (no network calls).

Run with: pytest tests/
"""
from bharatpay_mcp.tools.gstin import _gstin_checksum, validate_gstin
from bharatpay_mcp.tools.inr import format_inr
from bharatpay_mcp.tools.pan import validate_pan
from bharatpay_mcp.tools.upi import validate_upi_vpa


# --- PAN ---
def test_pan_valid_company():
    r = validate_pan("AABCT3518Q")
    assert r["valid"]
    assert r["entity_type"] == "Company"
    assert r["entity_code"] == "C"


def test_pan_valid_individual():
    r = validate_pan("ABCPE1234F")
    assert r["valid"]
    assert r["entity_type"] == "Individual"
    assert r["surname_initial"] == "E"


def test_pan_too_short():
    r = validate_pan("ABCDE")
    assert not r["valid"]
    assert "10 characters" in r["error"]


def test_pan_bad_format():
    r = validate_pan("12345ABCDE")  # digits where letters should be
    assert not r["valid"]


def test_pan_lowercase_normalized():
    r = validate_pan("abcde1234f")
    assert r["valid"]
    assert r["pan"] == "ABCDE1234F"


# --- GSTIN ---
def test_gstin_state_extraction():
    # Format-valid (checksum is intentionally arbitrary here)
    r = validate_gstin("29AABCT1332L1ZX")
    assert r["state_code"] == "29"
    assert r["state"] == "Karnataka"
    assert r["pan"] == "AABCT1332L"


def test_gstin_too_short():
    r = validate_gstin("29ABCDE")
    assert not r["valid"]
    assert "15 characters" in r["error"]


def test_gstin_checksum_self_consistent():
    """For any 14-char prefix, our generated checksum should validate."""
    prefix = "29AABCT1332L1Z"  # ends in 'Z' as required by format
    check = _gstin_checksum(prefix)
    full = prefix + check
    r = validate_gstin(full)
    assert r["valid"], f"Self-generated GSTIN failed validation: {r}"
    assert r["checksum_valid"]


# --- UPI ---
def test_upi_google_pay_handle():
    r = validate_upi_vpa("alice@oksbi")
    assert r["valid"]
    assert r["psp"] == "Google Pay"
    assert r["underlying_bank"] == "State Bank of India"


def test_upi_phonepe_handle():
    r = validate_upi_vpa("bob@ybl")
    assert r["valid"]
    assert r["psp"] == "PhonePe"


def test_upi_unknown_handle_is_still_valid_format():
    r = validate_upi_vpa("foo@somebank")
    assert r["valid"]
    assert r["psp"] == "Unknown"


def test_upi_no_at_sign():
    r = validate_upi_vpa("noatsign")
    assert not r["valid"]


# --- INR ---
def test_inr_lakh():
    r = format_inr(100000)
    assert r["indian_format"] == "1,00,000"
    assert r["words"] == "1 Lakh"


def test_inr_crore():
    r = format_inr(10000000)
    assert r["indian_format"] == "1,00,00,000"
    assert r["words"] == "1 Crore"


def test_inr_paise_mode():
    r = format_inr(29500, paise=True)
    assert r["rupees"] == 295.0
    assert r["paise"] == 29500


def test_inr_decimals_preserved():
    r = format_inr(150000)
    assert r["words"] == "1.5 Lakh"


def test_inr_small_amount():
    r = format_inr(50)
    assert r["with_symbol"] == "₹50"
