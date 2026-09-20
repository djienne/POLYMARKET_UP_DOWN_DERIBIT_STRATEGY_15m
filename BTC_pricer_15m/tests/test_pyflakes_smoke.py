"""Static smoke test: catch undefined-name bugs (F-undefined) before they ship.

The phantom-TP-fill bug on 2026-04-30 (real -$6.32 recorded as fake +$1.99
profit on trade 4ca5f67b, market 1777591800) was triggered by a NameError
in ``executor_live.get_proceeds_from_trades``: the function called
``time.sleep(delay)`` without ``import time`` at module scope. The
NameError got swallowed by an upstream ``except Exception`` and the
fallback fabricated a profit at the limit price.

``py_compile`` does NOT catch this — name resolution is runtime in
Python, syntax is all py_compile checks. ``pyflakes`` (and ``ruff``'s
F821 rule) DO catch it. This test enforces a clean pyflakes pass on
the trader package so the same class of bug can never ship again.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
TRADER_PKG = REPO_ROOT / "scripts" / "trader"

# pyflakes emits one diagnostic per line. Whitelist the cosmetic categories
# we don't care about — only undefined-name bugs are blocking.
COSMETIC_PATTERNS = (
    "imported but unused",
    "f-string is missing placeholders",
    "local variable",
    "redefinition of unused",
    "may be undefined, or defined from star imports",  # not actually undefined
)


def test_no_undefined_names_in_trader_package() -> None:
    """Run pyflakes on scripts/trader/ and fail if any undefined-name
    diagnostic shows up. Cosmetic warnings (unused imports, empty
    f-strings, etc.) are tolerated."""
    result = subprocess.run(  # noqa: PLW1510
        [sys.executable, "-m", "pyflakes", str(TRADER_PKG)],
        capture_output=True,
        text=True,
    )
    assert result.returncode in (0, 1), result.stderr
    assert "No module named pyflakes" not in result.stderr, "Docker test image must include pyflakes"
    blocking_lines: list[str] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        if any(p in line for p in COSMETIC_PATTERNS):
            continue
        # Anything left is something pyflakes flagged that ISN'T in the
        # cosmetic whitelist — most likely an undefined-name (F821) or
        # an unused-variable / shadowed-name issue we should look at.
        blocking_lines.append(line)
    assert not blocking_lines, (
        "pyflakes flagged blocking issues in scripts/trader/:\n"
        + "\n".join(blocking_lines)
    )
