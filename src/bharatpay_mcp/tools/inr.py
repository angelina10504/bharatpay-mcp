"""Indian rupee formatting utilities.

Indian numerals use a comma every 2 digits after the first 3 from the right:
1,000 → 1,000
10,000 → 10,000
1,00,000 → 1 lakh
1,00,00,000 → 1 crore
"""
from __future__ import annotations


def _indian_comma(n: int) -> str:
    """Format an integer with Indian-style grouping."""
    s = str(abs(n))
    if len(s) <= 3:
        out = s
    else:
        last3 = s[-3:]
        rest = s[:-3]
        # Group `rest` from the right in pairs of two
        groups = []
        while len(rest) > 2:
            groups.insert(0, rest[-2:])
            rest = rest[:-2]
        if rest:
            groups.insert(0, rest)
        out = ",".join(groups) + "," + last3
    return ("-" if n < 0 else "") + out


def _to_words(amount: float) -> str:
    """Convert a number to Indian word form (lakh/crore)."""
    if amount == 0:
        return "Zero"

    sign = "Minus " if amount < 0 else ""
    n = abs(amount)

    def _trim(value: float) -> str:
        # 1.00 -> '1', 1.50 -> '1.5', 1.52 -> '1.52'
        return f"{value:.2f}".rstrip("0").rstrip(".")

    if n >= 1_00_00_000:
        return f"{sign}{_trim(n / 1_00_00_000)} Crore"
    if n >= 1_00_000:
        return f"{sign}{_trim(n / 1_00_000)} Lakh"
    if n >= 1_000:
        return f"{sign}{_trim(n / 1_000)} Thousand"
    return f"{sign}{n:.0f}"


def format_inr(amount: float, paise: bool = False) -> dict:
    """Format an INR amount in Indian numbering conventions.

    Args:
        amount: Number in INR (or paise if `paise=True`).
        paise: If True, treat `amount` as paise (1 INR = 100 paise).
               Useful for Razorpay API responses where amounts are in paise.

    Returns:
        {
          "rupees": float — amount in INR,
          "paise": int — amount in paise,
          "indian_format": "1,00,000",
          "with_symbol": "₹1,00,000",
          "words": "1 Lakh"
        }
    """
    if amount is None:
        return {"error": "Amount is required."}

    try:
        value = float(amount)
    except (TypeError, ValueError):
        return {"error": f"Could not parse amount: {amount!r}"}

    rupees = value / 100 if paise else value
    paise_value = int(round(value if paise else value * 100))

    rupee_int = int(rupees)
    decimal_part = abs(rupees - rupee_int)

    formatted = _indian_comma(rupee_int)
    if decimal_part > 0.0001:
        formatted += f".{int(round(decimal_part * 100)):02d}"

    return {
        "rupees": round(rupees, 2),
        "paise": paise_value,
        "indian_format": formatted,
        "with_symbol": f"₹{formatted}",
        "words": _to_words(rupees),
    }
