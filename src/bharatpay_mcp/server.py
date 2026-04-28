"""BharatPay MCP server — exposes Indian fintech utilities to AI agents.

Run via stdio (standard MCP transport for Claude Desktop / Cursor / etc.):
    python -m bharatpay_mcp
or:
    bharatpay-mcp
"""
from __future__ import annotations

from fastmcp import FastMCP

from bharatpay_mcp.tools.gstin import validate_gstin as _validate_gstin
from bharatpay_mcp.tools.ifsc import lookup_ifsc as _lookup_ifsc
from bharatpay_mcp.tools.inr import format_inr as _format_inr
from bharatpay_mcp.tools.mutual_fund import get_mutual_fund_nav as _get_mf_nav
from bharatpay_mcp.tools.pan import validate_pan as _validate_pan
from bharatpay_mcp.tools.pincode import lookup_pincode as _lookup_pincode
from bharatpay_mcp.tools.upi import validate_upi_vpa as _validate_upi


mcp = FastMCP(
    "BharatPay",
    instructions=(
        "BharatPay provides Indian-fintech utility tools — IFSC bank lookups, "
        "PAN/GSTIN validation, pincode lookups, mutual fund NAVs, UPI VPA "
        "validation, and INR formatting. Use these to verify and enrich "
        "Indian financial data. Designed to complement Razorpay's official "
        "MCP server: BharatPay handles validation/lookups, Razorpay's MCP "
        "handles transaction execution."
    ),
)


# --- IFSC ---
@mcp.tool()
async def lookup_ifsc(ifsc: str) -> dict:
    """Look up bank, branch, address, and supported payment rails (NEFT/RTGS/IMPS/UPI)
    for an Indian IFSC code. Uses Razorpay's free public IFSC API.

    Args:
        ifsc: 11-character IFSC code (e.g., 'KKBK0000261').
    """
    return await _lookup_ifsc(ifsc)


# --- PAN ---
@mcp.tool()
def validate_pan(pan: str) -> dict:
    """Validate Indian PAN format and identify the entity type
    (Individual / Company / HUF / Trust / Partnership Firm / etc.) from the
    4th character.

    Args:
        pan: 10-character PAN (e.g., 'ABCDE1234F').
    """
    return _validate_pan(pan)


# --- GSTIN ---
@mcp.tool()
def validate_gstin(gstin: str) -> dict:
    """Validate Indian GSTIN: format, mod-36 checksum, and extract embedded
    state and PAN. Returns the registration state name from the leading
    state code.

    Args:
        gstin: 15-character GSTIN (e.g., '29AABCT1332L1ZS').
    """
    return _validate_gstin(gstin)


# --- Pincode ---
@mcp.tool()
async def lookup_pincode(pin: str) -> dict:
    """Look up state, district, and post offices for an Indian PIN code
    using the free India Post API.

    Args:
        pin: 6-digit PIN (e.g., '302001' for Jaipur).
    """
    return await _lookup_pincode(pin)


# --- Mutual fund NAV ---
@mcp.tool()
async def get_mutual_fund_nav(query: str) -> dict:
    """Get the latest daily NAV for an Indian mutual fund scheme. Accepts
    either an AMFI scheme code (digits only) or a name fragment for fuzzy
    search. Data sourced from AMFI India's public daily file.

    Args:
        query: Scheme code (e.g., '118989') or name fragment
               (e.g., 'parag parikh flexi cap').
    """
    return await _get_mf_nav(query)


# --- UPI VPA ---
@mcp.tool()
def validate_upi_vpa(vpa: str) -> dict:
    """Validate a UPI VPA's format and identify the issuing PSP and
    underlying bank from the handle suffix (e.g., 'oksbi' → Google Pay
    on State Bank of India).

    Args:
        vpa: VPA in 'username@psp' format (e.g., 'angelina@oksbi').
    """
    return _validate_upi(vpa)


# --- INR formatter ---
@mcp.tool()
def format_inr(amount: float, paise: bool = False) -> dict:
    """Format an INR amount in Indian numbering conventions (lakh/crore
    grouping with comma every two digits after the first three) and return
    word form. Set `paise=True` if amount is in paise (Razorpay API style,
    where 1 INR = 100 paise).

    Args:
        amount: Number to format.
        paise: If True, treat amount as paise.
    """
    return _format_inr(amount, paise=paise)


def main() -> None:
    """Entry point — run the server over stdio (standard MCP transport)."""
    mcp.run()


if __name__ == "__main__":
    main()
