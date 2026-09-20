"""Shared atomic file-write helpers for trader runtime files."""

from __future__ import annotations

import csv
import json
import os
import tempfile
import time
from pathlib import Path
from typing import Iterable, Mapping, Sequence


def ensure_trailing_newline(path: Path) -> bool:
    """Repair a missing final newline left by a crash mid-append.

    Without this, the next append concatenates its first row onto the partial
    line, corrupting two records instead of one. Returns True if repaired.
    """
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        return False
    with open(path, "rb+") as f:
        f.seek(-1, os.SEEK_END)
        if f.read(1) == b"\n":
            return False
        f.write(b"\n")
        return True


def atomic_write_bytes(
    path: Path,
    payload: bytes,
    *,
    mode: int | None = 0o644,
    retries: int = 5,
    retry_delay_seconds: float = 0.1,
) -> None:
    """Write bytes via temp file + atomic replace, with Windows lock retries."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        if mode is not None:
            try:
                os.chmod(tmp_path, mode)
            except OSError:
                pass
        with os.fdopen(fd, "wb") as f:
            f.write(payload)
        for attempt in range(retries):
            try:
                os.replace(tmp_path, path)
                break
            except PermissionError:
                if attempt < retries - 1:
                    time.sleep(retry_delay_seconds * (attempt + 1))
                else:
                    raise
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def atomic_write_text(
    path: Path,
    payload: str,
    *,
    encoding: str = "utf-8",
    mode: int | None = 0o644,
    retries: int = 5,
) -> None:
    atomic_write_bytes(
        path,
        payload.encode(encoding),
        mode=mode,
        retries=retries,
    )


def atomic_write_json(
    path: Path,
    payload: Mapping,
    *,
    indent: int = 2,
    mode: int | None = 0o644,
    retries: int = 5,
) -> None:
    atomic_write_text(
        path,
        json.dumps(payload, indent=indent),
        mode=mode,
        retries=retries,
    )


def atomic_write_csv_rows(
    path: Path,
    header: Sequence[str],
    rows: Iterable[Sequence],
    *,
    mode: int | None = 0o644,
    retries: int = 5,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        if mode is not None:
            try:
                os.chmod(tmp_path, mode)
            except OSError:
                pass
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        for attempt in range(retries):
            try:
                os.replace(tmp_path, path)
                break
            except PermissionError:
                if attempt < retries - 1:
                    time.sleep(0.1 * (attempt + 1))
                else:
                    raise
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def atomic_write_dicts_csv(
    path: Path,
    rows: Sequence[dict],
    *,
    mode: int | None = 0o644,
    retries: int = 5,
) -> None:
    if not rows:
        return
    header = list(rows[0].keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        if mode is not None:
            try:
                os.chmod(tmp_path, mode)
            except OSError:
                pass
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)
        for attempt in range(retries):
            try:
                os.replace(tmp_path, path)
                break
            except PermissionError:
                if attempt < retries - 1:
                    time.sleep(0.1 * (attempt + 1))
                else:
                    raise
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
