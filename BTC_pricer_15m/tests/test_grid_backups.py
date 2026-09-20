"""Tests for the gzipped + tiered-retention grid backup mechanism.

The backup writer lives in `scripts/trader/main.py` and is invoked from
`save_state`. These tests exercise it in isolation against a tmp_path.
"""

from __future__ import annotations

import gzip
import json
import os
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.trader import main as trader_main


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_migration_cache():
    trader_main._MIGRATED_BACKUP_DIRS.clear()
    yield
    trader_main._MIGRATED_BACKUP_DIRS.clear()


@pytest.fixture
def backup_dir(tmp_path: Path, monkeypatch):
    """Redirect the module-level backup dir to an isolated tmp dir."""
    d = tmp_path / "grid_state"
    monkeypatch.setattr(trader_main, "GRID_BACKUP_DIR", d)
    return d


def _set_mtime(path: Path, age_seconds: float) -> None:
    """Backdate `path` by `age_seconds` so retention buckets it correctly."""
    target = time.time() - age_seconds
    os.utime(path, (target, target))


# ---------------------------------------------------------------------------
# Compression round-trip
# ---------------------------------------------------------------------------


def test_round_trip_state_snapshot_json(tmp_path: Path, backup_dir: Path):
    """Realistic-shaped JSON survives backup → gzip → decompress → parse."""
    src = tmp_path / "state_snapshot.json"
    snapshot = {
        "timestamp": "2026-04-30T00:00:00+00:00",
        "n_instances": 3,
        "markets_resolved": 42,
        "instances": {
            "0": {"trades_count": 10, "total_pnl": 1.23, "params": {"alpha_up": 1.3}},
            "1": {"trades_count": 7, "total_pnl": -0.45, "params": {"alpha_up": 1.8}},
            "2": {"trades_count": 15, "total_pnl": 2.10, "params": {"alpha_up": 2.5}},
        },
    }
    src.write_text(json.dumps(snapshot), encoding="utf-8")
    trader_main._backup_grid_file(src, min_interval_seconds=0)

    [bak] = list(backup_dir.glob("state_snapshot.json.*.bak.gz"))
    assert not list(backup_dir.glob("state_snapshot.json.*.bak"))
    with gzip.open(bak, "rb") as f:
        recovered = json.loads(f.read().decode("utf-8"))
    assert recovered == snapshot


# ---------------------------------------------------------------------------
# Throttle
# ---------------------------------------------------------------------------


def test_min_interval_throttles_backups(tmp_path: Path, backup_dir: Path):
    src = tmp_path / "state_snapshot.json"
    src.write_text("{}", encoding="utf-8")

    trader_main._backup_grid_file(src, min_interval_seconds=0)
    assert len(list(backup_dir.glob("state_snapshot.json.*.bak.gz"))) == 1

    # Second call within the throttle window is a no-op.
    trader_main._backup_grid_file(src, min_interval_seconds=300)
    assert len(list(backup_dir.glob("state_snapshot.json.*.bak.gz"))) == 1


# ---------------------------------------------------------------------------
# Tiered retention
# ---------------------------------------------------------------------------


def test_tiered_retention_keeps_dense_recent_then_thins(backup_dir: Path):
    """Keep every recent backup and the newest survivor of each older bucket."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    from datetime import datetime, timezone
    now = datetime(2026, 9, 5, 12, 30, tzinfo=timezone.utc).timestamp()
    ages = [60, 2700, 65 * 60, 75 * 60, 135 * 60, 2 * 86400 + 3600, 2 * 86400 + 10800, 8 * 86400]
    paths = []
    for index, age in enumerate(ages):
        path = backup_dir / f"state_snapshot.json.{index}.bak.gz"
        path.write_bytes(b"")
        os.utime(path, (now - age, now - age))
        paths.append(path)
    keep = trader_main._select_backups_to_keep(paths, now)
    assert keep == {paths[index] for index in (0, 1, 2, 4, 5)}


def test_tiered_retention_preserves_manual_pins(backup_dir: Path):
    """Files with `.manual.` in the name are exempt from sweep, even very old."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    now = time.time()
    auto_old = backup_dir / "leaderboard.csv.20200101T000000Z.bak.gz"
    manual_old = backup_dir / "leaderboard.csv.20200101T000000Z.manual.bak.gz"
    for p in (auto_old, manual_old):
        p.write_bytes(b"")
        _set_mtime(p, 30 * 86400)  # 30 days old

    keep = trader_main._select_backups_to_keep([auto_old, manual_old], now)
    assert manual_old in keep
    assert auto_old not in keep


# ---------------------------------------------------------------------------
# Migration of pre-existing uncompressed backups
# ---------------------------------------------------------------------------


def test_migrates_existing_uncompressed_backups(tmp_path: Path, backup_dir: Path):
    src = tmp_path / "state_snapshot.json"
    src.write_text(json.dumps({"n_instances": 7}), encoding="utf-8")

    backup_dir.mkdir(parents=True, exist_ok=True)
    legacy = backup_dir / "state_snapshot.json.20260101T000000Z.bak"
    legacy.write_bytes(b'{"legacy":true}')

    trader_main._backup_grid_file(src, min_interval_seconds=0)

    # Legacy .bak has been gzipped + removed; new .bak.gz exists.
    assert not legacy.exists()
    migrated = backup_dir / "state_snapshot.json.20260101T000000Z.bak.gz"
    assert migrated.exists()
    with gzip.open(migrated, "rb") as f:
        assert f.read() == b'{"legacy":true}'

    # And the new live backup also landed as gzipped.
    new_baks = [
        p for p in backup_dir.glob("state_snapshot.json.*.bak.gz")
        if "20260101T000000Z" not in p.name
    ]
    assert len(new_baks) == 1


def test_migration_runs_once_per_dir(backup_dir: Path):
    """Module-level cache prevents repeated re-scans for the same dir."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    legacy = backup_dir / "x.bak"
    legacy.write_bytes(b"x")

    trader_main._migrate_uncompressed_backups(backup_dir)
    assert backup_dir in trader_main._MIGRATED_BACKUP_DIRS

    # If we drop another .bak after the cache marks the dir, a second call
    # is a no-op (cache short-circuits).
    second = backup_dir / "y.bak"
    second.write_bytes(b"y")
    trader_main._migrate_uncompressed_backups(backup_dir)
    assert second.exists(), "second .bak should remain because migration is one-shot"
