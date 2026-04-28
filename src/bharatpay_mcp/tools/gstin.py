"""GSTIN (Goods and Services Tax Identification Number) validation.

15-character format: SSPPPPPPPPPPEEZ C
- SS:        2-digit state code
- PPPPPPPPPP 10-character PAN of the entity
- E:         entity number for the same PAN within the state (1-9, then A-Z)
- E:         alphabet (default 'Z')
- C:         checksum character

The checksum uses a base-36 weighted algorithm published by the GSTN.
"""
from __future__ import annotations

import re

GSTIN_PATTERN = re.compile(
    r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z][Z][0-9A-Z]$"
)

STATE_CODES = {
    "01": "Jammu and Kashmir",
    "02": "Himachal Pradesh",
    "03": "Punjab",
    "04": "Chandigarh",
    "05": "Uttarakhand",
    "06": "Haryana",
    "07": "Delhi",
    "08": "Rajasthan",
    "09": "Uttar Pradesh",
    "10": "Bihar",
    "11": "Sikkim",
    "12": "Arunachal Pradesh",
    "13": "Nagaland",
    "14": "Manipur",
    "15": "Mizoram",
    "16": "Tripura",
    "17": "Meghalaya",
    "18": "Assam",
    "19": "West Bengal",
    "20": "Jharkhand",
    "21": "Odisha",
    "22": "Chhattisgarh",
    "23": "Madhya Pradesh",
    "24": "Gujarat",
    "26": "Dadra and Nagar Haveli and Daman and Diu",
    "27": "Maharashtra",
    "29": "Karnataka",
    "30": "Goa",
    "31": "Lakshadweep",
    "32": "Kerala",
    "33": "Tamil Nadu",
    "34": "Puducherry",
    "35": "Andaman and Nicobar Islands",
    "36": "Telangana",
    "37": "Andhra Pradesh",
    "38": "Ladakh",
    "97": "Other Territory",
    "99": "Centre Jurisdiction",
}

# Charset: digits 0-9 (values 0-9), then letters A-Z (values 10-35)
_CHARSET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_CHAR_TO_VAL = {c: i for i, c in enumerate(_CHARSET)}


def _gstin_checksum(first_14: str) -> str:
    """Compute the GSTIN checksum char from the first 14 characters."""
    factor = 2
    total = 0
    for ch in reversed(first_14):
        val = _CHAR_TO_VAL[ch]
        product = val * factor
        total += product // 36 + product % 36
        factor = 1 if factor == 2 else 2
    remainder = total % 36
    check_value = (36 - remainder) % 36
    return _CHARSET[check_value]


def validate_gstin(gstin: str) -> dict:
    """Validate a GSTIN format + checksum and extract state/PAN metadata.

    Args:
        gstin: 15-character GSTIN (e.g., '29AABCT1332L1ZS').

    Returns:
        {
          "valid": bool,
          "gstin": normalized,
          "state_code": "29",
          "state": "Karnataka",
          "pan": embedded PAN,
          "entity_number": 13th char,
          "checksum_valid": bool,
          "error": present only if invalid
        }
    """
    if not gstin:
        return {"valid": False, "error": "GSTIN is required."}

    code = gstin.strip().upper()
    if len(code) != 15:
        return {
            "valid": False,
            "gstin": code,
            "error": f"GSTIN must be 15 characters (got {len(code)}).",
        }
    if not GSTIN_PATTERN.match(code):
        return {
            "valid": False,
            "gstin": code,
            "error": (
                "Invalid GSTIN format. Expected: 2 digits + 5 letters + "
                "4 digits + 1 letter + 1 alphanumeric + 'Z' + 1 alphanumeric."
            ),
        }

    state_code = code[0:2]
    state = STATE_CODES.get(state_code)
    if state is None:
        return {
            "valid": False,
            "gstin": code,
            "error": f"Unknown state code '{state_code}'.",
        }

    expected_check = _gstin_checksum(code[:14])
    checksum_ok = expected_check == code[14]

    result = {
        "valid": checksum_ok,
        "gstin": code,
        "state_code": state_code,
        "state": state,
        "pan": code[2:12],
        "entity_number": code[12],
        "default_z_char": code[13],
        "checksum_char": code[14],
        "checksum_valid": checksum_ok,
    }
    if not checksum_ok:
        result["error"] = (
            f"Checksum mismatch: expected '{expected_check}', got '{code[14]}'. "
            "The GSTIN is malformed or fabricated."
        )
    return result
