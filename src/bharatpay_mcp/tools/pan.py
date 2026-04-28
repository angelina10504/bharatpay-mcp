"""PAN (Permanent Account Number) validation and metadata extraction.

PAN format: AAAAA9999A (5 letters, 4 digits, 1 letter).
The 4th character indicates the entity type. PANs do not have a verifiable
checksum that can be computed offline (the last char is a hash, but the
algorithm is not public), so this validator returns format validity plus
extracted metadata. Verification of holder identity requires the NSDL/UTI
APIs which are not free or public.
"""
from __future__ import annotations

import re

PAN_PATTERN = re.compile(r"^[A-Z]{5}[0-9]{4}[A-Z]$")

ENTITY_TYPES = {
    "P": "Individual",
    "F": "Partnership Firm",
    "C": "Company",
    "H": "Hindu Undivided Family (HUF)",
    "A": "Association of Persons (AOP)",
    "T": "Trust",
    "B": "Body of Individuals (BOI)",
    "L": "Local Authority",
    "J": "Artificial Juridical Person",
    "G": "Government Agency",
}


def validate_pan(pan: str) -> dict:
    """Validate a PAN and extract entity-type metadata.

    Args:
        pan: 10-character PAN (e.g., 'ABCDE1234F').

    Returns:
        {
          "valid": bool,
          "pan": normalized PAN,
          "entity_type": human-readable category,
          "surname_initial": 5th char (only meaningful for individuals),
          "error": present only if invalid
        }
    """
    if not pan:
        return {"valid": False, "error": "PAN is required."}

    code = pan.strip().upper()
    if len(code) != 10:
        return {
            "valid": False,
            "pan": code,
            "error": f"PAN must be 10 characters (got {len(code)}).",
        }
    if not PAN_PATTERN.match(code):
        return {
            "valid": False,
            "pan": code,
            "error": (
                "Invalid PAN format. Expected: AAAAA9999A "
                "(5 letters, 4 digits, 1 letter)."
            ),
        }

    entity_char = code[3]
    entity_type = ENTITY_TYPES.get(entity_char, "Unknown entity type")

    result = {
        "valid": True,
        "pan": code,
        "entity_type": entity_type,
        "entity_code": entity_char,
    }
    if entity_char == "P":
        result["surname_initial"] = code[4]
    result["note"] = (
        "Format is valid. PAN holder identity can only be verified via the "
        "official NSDL/UTI APIs (paid)."
    )
    return result
