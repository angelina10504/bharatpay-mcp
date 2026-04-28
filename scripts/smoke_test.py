#!/usr/bin/env python3
"""Quick standalone smoke test — runs the tools directly without MCP/Claude.

Usage:
    PYTHONPATH=src python3 scripts/smoke_test.py
"""
import asyncio
import json
import sys

sys.path.insert(0, "src")

from bharatpay_mcp.tools.gstin import validate_gstin, _gstin_checksum
from bharatpay_mcp.tools.ifsc import lookup_ifsc
from bharatpay_mcp.tools.inr import format_inr
from bharatpay_mcp.tools.mutual_fund import get_mutual_fund_nav
from bharatpay_mcp.tools.pan import validate_pan
from bharatpay_mcp.tools.pincode import lookup_pincode
from bharatpay_mcp.tools.upi import validate_upi_vpa


def show(label, value):
    print(f"\n=== {label} ===")
    if isinstance(value, dict):
        print(json.dumps(value, indent=2, ensure_ascii=False))
    else:
        print(value)


async def main():
    # 1. PAN
    show("validate_pan('AABCT3518Q')", validate_pan("AABCT3518Q"))

    # 2. GSTIN — self-generated valid one
    prefix = "29AABCT1332L1Z"
    valid_gstin = prefix + _gstin_checksum(prefix)
    show(f"validate_gstin('{valid_gstin}')", validate_gstin(valid_gstin))

    # 3. UPI
    show("validate_upi_vpa('angelina@oksbi')", validate_upi_vpa("angelina@oksbi"))

    # 4. INR
    show("format_inr(150000)", format_inr(150000))
    show("format_inr(29500, paise=True)", format_inr(29500, paise=True))

    # 5. IFSC (live API)
    show("lookup_ifsc('KKBK0000261')", await lookup_ifsc("KKBK0000261"))

    # 6. Pincode (live API)
    pin = await lookup_pincode("302001")
    summary = {
        "pin": pin.get("pin"),
        "state": pin.get("state"),
        "district": pin.get("district"),
        "post_office_count": pin.get("count"),
        "first_post_office": pin.get("post_offices", [{}])[0].get("name"),
    }
    show("lookup_pincode('302001')", summary)

    # 7. Mutual Fund NAV (live, downloads ~6 MB the first time)
    print("\n=== get_mutual_fund_nav('parag parikh flexi cap') ===")
    print("(downloading AMFI daily file, ~6 MB, one-time per process)…")
    mf = await get_mutual_fund_nav("parag parikh flexi cap")
    if "matches" in mf:
        print(f"Found {mf['match_count']} matches as of {mf['as_of_date']}:")
        for m in mf["matches"][:5]:
            print(f"  [{m['scheme_code']}] NAV ₹{m['nav']} — {m['scheme_name']}")
    else:
        print(json.dumps(mf, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    asyncio.run(main())
