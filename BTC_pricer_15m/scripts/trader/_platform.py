"""Small cross-platform process helpers."""

from __future__ import annotations

import subprocess
import sys
from typing import Any


def subprocess_kwargs(**kwargs) -> dict[str, Any]:
    """Return subprocess kwargs with hidden console windows on Windows."""
    if sys.platform == "win32":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return kwargs
