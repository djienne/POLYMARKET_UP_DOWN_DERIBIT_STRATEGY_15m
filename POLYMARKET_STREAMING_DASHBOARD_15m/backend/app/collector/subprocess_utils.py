from __future__ import annotations

import asyncio
import subprocess
import sys
from collections.abc import Sequence


def _creationflags() -> int:
    if sys.platform == "win32":
        return getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return 0


async def run_subprocess(
    cmd: Sequence[str],
    *,
    timeout: float,
) -> subprocess.CompletedProcess[bytes]:
    def _run() -> subprocess.CompletedProcess[bytes]:
        kwargs: dict = {
            "capture_output": True,
            "check": False,
            "timeout": timeout,
        }
        flags = _creationflags()
        if flags:
            kwargs["creationflags"] = flags
        return subprocess.run(list(cmd), **kwargs)

    return await asyncio.to_thread(_run)
