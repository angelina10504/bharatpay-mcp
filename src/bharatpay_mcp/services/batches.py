"""CSV parsing is bounded and deterministic; workers perform the I/O later."""
from __future__ import annotations

import csv
import io


def parse_csv(content: bytes) -> list[dict]:
    if len(content) > 1_000_000:
        raise ValueError("CSV must be at most 1 MB")
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("Use a UTF-8 CSV") from exc
    reader = csv.DictReader(io.StringIO(text))
    allowed = {"name", "pan", "gstin", "vpa", "ifsc", "pincode"}
    if not reader.fieldnames or "name" not in reader.fieldnames:
        raise ValueError("CSV requires a name column")
    if len(set(reader.fieldnames)) != len(reader.fieldnames) or set(reader.fieldnames) - allowed:
        raise ValueError("Duplicate or unknown CSV columns")
    rows = []
    try:
        for row in reader:
            if len(rows) >= 500:
                raise ValueError("CSV supports at most 500 rows")
            if None in row:
                raise ValueError("A row has more fields than the header")
            rows.append({k: (v or "").strip() for k, v in row.items()})
    except csv.Error as exc:
        raise ValueError("Malformed CSV") from exc
    if not rows:
        raise ValueError("CSV contains no rows")
    return rows
