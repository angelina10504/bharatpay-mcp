# CLAUDE.md

Guidance for Claude Code working in this repo.

## What this is

`bharatpay-mcp` — a public MCP server (PyPI: `bharatpay-mcp`, MIT) that exposes Indian
fintech utilities to AI agents over stdio: IFSC lookups, PAN/GSTIN validation, pincode
lookups, mutual fund NAVs, UPI VPA identification, INR formatting. Seven tools, no auth,
no API keys, no paid upstreams.

It deliberately complements Razorpay's official MCP server rather than competing:
Razorpay executes transactions, BharatPay validates and enriches. Keep that boundary —
don't add tools that move money or need credentials.

## Layout

```
src/bharatpay_mcp/
  server.py        # FastMCP instance + all @mcp.tool() registrations. Thin wrappers only.
  __main__.py      # `python -m bharatpay_mcp` → server.main()
  tools/           # one module per domain; all real logic lives here
    gstin.py  ifsc.py  inr.py  mutual_fund.py  pan.py  pincode.py  upi.py
tests/test_validators.py   # offline validators only, no network
scripts/smoke_test.py      # manual end-to-end incl. live APIs (not part of pytest)
```

## Engineering standards

These are the rules that matter here. Follow them; don't relax them for convenience.

### 1. Every tool has a pure-function offline validator

Each tool module must be able to reject bad input **without touching the network**.
Format checks, checksums, and metadata extraction are pure functions over strings —
they run before any HTTP call and they never raise.

- Fully offline tools (`pan.py`, `gstin.py`, `upi.py`, `inr.py`) are pure end to end.
- Network tools validate first, then fetch: see `_validate_format()` in
  [ifsc.py](src/bharatpay_mcp/tools/ifsc.py) and `PIN_PATTERN` in
  [pincode.py](src/bharatpay_mcp/tools/pincode.py).

The point is graceful degradation: when `ifsc.razorpay.com` or India Post is down, a
malformed IFSC still gets a precise, useful answer, and a well-formed one gets an
honest network error — never a traceback across the MCP boundary.

**Known gap:** [mutual_fund.py](src/bharatpay_mcp/tools/mutual_fund.py) only checks for
an empty query before hitting AMFI. If you touch that file, add a real offline
validator (scheme codes are digits; name fragments have a minimum length) and a test.

### 2. Tools return dicts, never raise

Every tool returns a JSON-serializable `dict`. Failures come back as
`{"error": "<human-readable explanation>", "<input_field>": "<normalized input>"}`.
Catch `httpx.RequestError` / `httpx.HTTPError` at the call site and convert. An
exception escaping a tool is a bug — the agent loses the ability to explain the failure
to the user.

Error strings are written for a model to read: say what was expected and give an
example ("Expected: 4 letters (bank) + '0' + 6 alphanumeric characters. Example:
HDFC0000123.").

### 3. No unit test may hit the network

`pytest tests/` must pass on a plane. Tests import validators directly and assert on
returned dicts. Do not add tests for `lookup_ifsc`, `lookup_pincode`, or
`get_mutual_fund_nav` that make live calls — if those paths need coverage, stub the
`httpx` response; never call the real endpoint.

Live-API checks belong in [scripts/smoke_test.py](scripts/smoke_test.py), which is run
by hand and is not collected by pytest.

The suite is 17 tests and finishes in ~0.01s. If it ever pauses, something started
making a network call — treat that as a failure, not flakiness.

There is no `[tool.pytest]` config, so `pytest tests/` only resolves imports when the
package is installed editable:

```bash
pip install -e ".[dev]" && pytest tests/ -v
```

Without that, put `src` on the path explicitly:

```bash
PYTHONPATH=src python -m pytest tests/ -q
```

```bash
PYTHONPATH=src python3 scripts/smoke_test.py
```

### 4. Checksum tests are self-consistency tests

Don't hardcode real-world GSTINs (or, later, Aadhaar numbers) as fixtures — they belong
to real entities and they rot. Instead, generate the check character with the same
implementation and assert the validator accepts the result:

```python
def test_gstin_checksum_self_consistent():
    prefix = "29AABCT1332L1Z"          # 14 chars, ends in 'Z' per format
    full = prefix + _gstin_checksum(prefix)
    r = validate_gstin(full)
    assert r["valid"] and r["checksum_valid"]
```

This catches drift between the generator and the verifier — which is the actual failure
mode — without embedding anyone's real tax ID. Any new checksum algorithm gets the same
treatment. Pair it with negative tests (wrong length, bad format, mutated check char).

### 5. FastMCP registration pattern

`server.py` holds registrations only. To add a tool:

1. Write the logic in `tools/<domain>.py` as a plain function (`async def` only if it
   does I/O). Return a dict. Validate offline first.
2. Import it into `server.py` under a `_`-prefixed alias:
   `from bharatpay_mcp.tools.aadhaar import validate_aadhaar as _validate_aadhaar`
3. Register a thin wrapper that just forwards arguments:

```python
# --- Aadhaar ---
@mcp.tool()
def validate_aadhaar(number: str) -> dict:
    """Validate an Indian Aadhaar number's Verhoeff checksum, entirely offline.

    Args:
        number: 12-digit Aadhaar (e.g., '234567890123').
    """
    return _validate_aadhaar(number)
```

Rules for the wrapper: keep the `# --- Domain ---` comment banner; annotate every
parameter and the `-> dict` return; put no logic in the body beyond the forwarding
call; mirror `async`/sync from the underlying function.

**The docstring is a prompt, not documentation.** It is what the agent reads to decide
whether to call the tool. State what it returns, the exact input format, and a concrete
example value in the `Args:` block. This is the single highest-leverage thing in the
file — tool selection accuracy lives or dies on it.

Then: add tests in `tests/test_validators.py`, add a case to `scripts/smoke_test.py`,
and add the row to the README tool table.

### 6. Upstream data sources

Every network tool names its source in the returned dict (`"source": "..."`) so the
agent can attribute the answer. Only free, public, unauthenticated endpoints:
`ifsc.razorpay.com`, `api.postalpincode.in`, AMFI's daily `NAVAll.txt`. If a feature
needs an API key or a paid tier, it doesn't go in this server.

The AMFI file is ~6 MB covering ~30,000 schemes; it's cached in-process for 6 hours
(`CACHE_TTL_SECONDS`). Never fetch it per call. `httpx.AsyncClient` is constructed with
`follow_redirects=True` there because the URL has already moved once.

### 7. Honesty about limits

Where a check can't be completed offline, the tool says so in a `note` field rather
than implying more certainty than it has — PAN format validity is not identity
verification (NSDL/UTI is paid); a well-formed VPA is not a live VPA (NPCI's ValidateVPA
is gated). Preserve those notes; add equivalents for new tools.

## Conventions

- Python ≥ 3.10. `from __future__ import annotations` at the top of every module.
- Modern typing: `str | None`, `dict[str, Any]`.
- Normalize input before validating: `.strip().upper()` for codes, `.strip().lower()`
  for VPAs. Echo the normalized value back in the result.
- Compiled module-level `re` patterns, named `<THING>_PATTERN`.
- Module docstrings explain the *domain* (what a GSTIN's 15 characters mean), not just
  the code. Keep that — it's why the algorithms are auditable.
- Dependencies are `fastmcp` and `httpx`. Adding a third needs a real reason.

## Release

Version lives in two places and must match: `pyproject.toml` `[project].version` and
`src/bharatpay_mcp/__init__.py` `__version__`. Build with hatchling, publish to PyPI as
`bharatpay-mcp`. `dist/` is gitignored.
