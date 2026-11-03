"""Indian rupee formatting utilities.

Indian numerals use a comma every 2 digits after the first 3 from the right:
1,000 → 1,000
10,000 → 10,000
1,00,000 → 1 lakh
1,00,00,000 → 1 crore
"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext


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


def _to_words(amount: Decimal) -> str:
    """Convert a number to Indian word form (lakh/crore)."""
    if amount == 0:
        return "Zero"

    sign = "Minus " if amount < 0 else ""
    n = abs(amount)

    def _trim(value: Decimal) -> str:
        # 1.00 -> '1', 1.50 -> '1.5', 1.52 -> '1.52'
        return f"{value:.2f}".rstrip("0").rstrip(".")

    if n >= 1_00_00_000:
        return f"{sign}{_trim(n / 1_00_00_000)} Crore"
    if n >= 1_00_000:
        return f"{sign}{_trim(n / 1_00_000)} Lakh"
    if n >= 1_000:
        return f"{sign}{_trim(n / 1_000)} Thousand"
    return f"{sign}{_trim(n)}"


def format_inr(amount: float | Decimal, paise: bool = False) -> dict:
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
        value = Decimal(str(amount))
    except (InvalidOperation, ValueError, TypeError):
        return {"error": f"Could not parse amount: {amount!r}"}
    if not value.is_finite() or value.copy_abs() > Decimal("1e15"):
        return {"error": "Amount must be finite and between -1e15 and 1e15."}

    # Normalize once so display and numeric fields describe the same amount.
    with localcontext() as context:
        context.prec = 32
        paise_value = int(
            (value if paise else value * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        )
        rupees = Decimal(paise_value) / 100
        whole, fraction = divmod(abs(paise_value), 100)
        formatted = ("-" if paise_value < 0 else "") + _indian_comma(whole)
        if fraction:
            formatted += f".{fraction:02d}"
        words = _to_words(rupees)

    return {
        "rupees": float(rupees),
        "paise": paise_value,
        "indian_format": formatted,
        "with_symbol": f"₹{formatted}",
        "words": words,
    }
