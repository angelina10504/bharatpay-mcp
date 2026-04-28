"""UPI VPA (Virtual Payment Address) validation and PSP identification.

A VPA looks like 'name@psp' (e.g., 'angelina@oksbi'). The PSP suffix
identifies which app/bank issued the VPA. There is no public API to
verify whether a VPA is *active* (NPCI gates that), but we can validate
format and identify the PSP from the suffix.
"""
from __future__ import annotations

import re

VPA_PATTERN = re.compile(r"^[a-zA-Z0-9.\-_]{2,256}@[a-zA-Z][a-zA-Z0-9.\-_]+$")

# Mapping of common PSP/handle suffixes to their issuing bank or app.
# Source: NPCI public list + observed in production statements.
PSP_HANDLES = {
    # Google Pay (uses bank handles)
    "okaxis": ("Google Pay", "Axis Bank"),
    "okhdfcbank": ("Google Pay", "HDFC Bank"),
    "okicici": ("Google Pay", "ICICI Bank"),
    "oksbi": ("Google Pay", "State Bank of India"),
    # PhonePe
    "ybl": ("PhonePe", "Yes Bank"),
    "ibl": ("PhonePe", "IDFC First Bank"),
    "axl": ("PhonePe", "Axis Bank"),
    # Paytm
    "paytm": ("Paytm", "Paytm Payments Bank"),
    "ptyes": ("Paytm", "Yes Bank"),
    "ptaxis": ("Paytm", "Axis Bank"),
    # Amazon Pay
    "apl": ("Amazon Pay", "Amazon Pay (RBL)"),
    # WhatsApp Pay
    "waicici": ("WhatsApp", "ICICI Bank"),
    "waaxis": ("WhatsApp", "Axis Bank"),
    "wahdfcbank": ("WhatsApp", "HDFC Bank"),
    # Cred
    "cred": ("CRED", "Axis Bank"),
    # BHIM (NPCI's own)
    "upi": ("BHIM", "NPCI"),
    # Bank-direct VPAs
    "icici": ("iMobile / direct", "ICICI Bank"),
    "hdfcbank": ("direct", "HDFC Bank"),
    "kotak": ("direct", "Kotak Mahindra Bank"),
    "yesbank": ("direct", "Yes Bank"),
    "axisbank": ("direct", "Axis Bank"),
    "sbi": ("direct", "State Bank of India"),
    "rbl": ("direct", "RBL Bank"),
    "fbl": ("direct", "Federal Bank"),
    "idfcbank": ("direct", "IDFC First Bank"),
    "indus": ("direct", "IndusInd Bank"),
    # Fintechs
    "jupiteraxis": ("Jupiter", "Axis Bank"),
    "fam": ("Fampay", "Federal Bank"),
    "freecharge": ("Freecharge", "Axis Bank"),
    "slc": ("Slice", "Yes Bank"),
    "jio": ("Jio Pay", "Jio Payments Bank"),
}


def validate_upi_vpa(vpa: str) -> dict:
    """Validate UPI VPA format and identify its PSP and underlying bank.

    Args:
        vpa: VPA string (e.g., 'angelina@oksbi').

    Returns:
        {
          "valid": bool,
          "vpa": normalized,
          "username": part before '@',
          "handle": PSP suffix after '@',
          "psp": "Google Pay" / "PhonePe" / etc. (or "Unknown"),
          "underlying_bank": bank name (or null),
          "error": present only if invalid
        }
    """
    if not vpa:
        return {"valid": False, "error": "VPA is required."}

    code = vpa.strip().lower()
    if "@" not in code:
        return {
            "valid": False,
            "vpa": code,
            "error": "VPA must contain '@'. Format: username@psp",
        }
    if not VPA_PATTERN.match(code):
        return {
            "valid": False,
            "vpa": code,
            "error": (
                "Invalid VPA format. Username may use letters, digits, "
                "'.', '-', '_'. Handle must start with a letter."
            ),
        }

    username, _, handle = code.partition("@")
    psp_info = PSP_HANDLES.get(handle)
    psp_name = psp_info[0] if psp_info else "Unknown"
    underlying_bank = psp_info[1] if psp_info else None

    return {
        "valid": True,
        "vpa": code,
        "username": username,
        "handle": handle,
        "psp": psp_name,
        "underlying_bank": underlying_bank,
        "note": (
            "Format is valid and PSP identified. Activeness of the VPA "
            "(whether it can receive money) can only be verified via NPCI's "
            "ValidateVPA API, which is not publicly available."
        ),
    }
