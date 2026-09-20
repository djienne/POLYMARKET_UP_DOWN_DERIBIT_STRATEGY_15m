#!/usr/bin/env python3
"""Create a portable ZIP backup for the local 15m Polymarket project.

The archive is meant for moving the project to another PC. It includes:

- BTC_pricer_15m source, tests, Docker/config files, and selected runtime state.
- POLYMARKET_STREAMING_DASHBOARD_15m source/config/assets, excluding rebuildable
  or local-only folders such as node_modules, dist, logs, runtime, and .git.
- Current dry-run/live stats from BTC_pricer_15m/results, including trades,
  state snapshots, leaderboards, live state/equity/trades, market DBs, and
  settlement locks.

By default the script includes project .env files because those are usually
needed for migration. Treat the resulting ZIP as sensitive, or pass
--exclude-env if you want a source/state backup without secrets.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


SCRIPT_PATH = Path(__file__).resolve()
FREQTRADE_ROOT = SCRIPT_PATH.parents[2]
BOT_DIR = FREQTRADE_ROOT / "BTC_pricer_15m"
DASHBOARD_DIR = FREQTRADE_ROOT / "POLYMARKET_STREAMING_DASHBOARD_15m"
DEFAULT_BACKUP_DIR = FREQTRADE_ROOT / "15m_migration_backups"

COMMON_EXCLUDED_DIRS = {
    ".git",
    ".claude",
    ".codex_tmp",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    ".venv",
    "venv",
    "env",
    "node_modules",
    "dist",
    "build",
    "logs",
    "runtime",
    "backup",
    "backups",
    "live_history_backups",
}

RESULTS_EXCLUDED_DIRS = {
    ".ssh_keys",
    ".ssh_keys_wsl",
    "backups",
    "backup_cleanup_0.99",
    "calibration_inbox_grid",
    "calibration_inbox_live",
    "live_history_backups",
    "pre_reconcile_20260424_192525Z",
    "pre_restore_empty_20260425_002648",
}

EXCLUDED_SUFFIXES = {
    ".pyc",
    ".pyo",
    ".zip",
    ".tar",
    ".gz",
    ".7z",
    ".tmp",
    ".sqlite-shm",
    ".sqlite-wal",
}

SECRETISH_FILENAMES = {
    ".offload_ssh_key",
    "lighter.pem",
}

RUNTIME_EXACT_FILES = {
    ".hybrid_calibration_meta.json",
    ".hybrid_calibration_meta.grid.json",
    ".hybrid_calibration_meta.live.json",
    ".live_location",
    "15m_friction_realized.csv",
    "15m_live_equity.csv",
    "15m_live_state.json",
    "15m_live_tick_eval.current.csv",
    "15m_live_trades.csv",
    "15m_orderbook.csv",
    "15m_paper_equity.csv",
    "15m_paper_state.json",
    "15m_paper_trades.csv",
    "15m_risk_events.csv",
    "15m_risk_events.current.csv",
    "leaderboard.csv",
    "leaderboard.txt",
    "ssvi_surface_warm.json",
    "state_snapshot.json",
    "terminal_data.grid.json",
    "terminal_data.json",
    "terminal_data.live.json",
    "terminal_data.shared.json",
    "terminal_data.shared_calc.json",
    "trades.csv",
}

OPTIONAL_LOG_FILES = {
    "15m_trading.log",
    "trader.log",
}


@dataclass(frozen=True)
class CandidateFile:
    path: Path
    archive_name: str
    size: int
    reason: str


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")


def _as_posix(path: Path) -> str:
    return path.as_posix()


def _git_info(path: Path) -> dict:
    def run_git(args: list[str]) -> str | None:
        try:
            out = subprocess.check_output(
                ["git", *args],
                cwd=path,
                text=True,
                stderr=subprocess.DEVNULL,
            )
        except Exception:
            return None
        return out.strip()

    return {
        "head": run_git(["rev-parse", "--short", "HEAD"]),
        "status_short": run_git(["status", "--short"]),
    }


def _is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _skip_dir(project: Path, root: Path, name: str) -> bool:
    current = root / name
    if name in COMMON_EXCLUDED_DIRS:
        return True
    if project == BOT_DIR and _is_under(current, BOT_DIR / "results"):
        return name in RESULTS_EXCLUDED_DIRS
    return False


def _has_excluded_suffix(name: str) -> bool:
    lower = name.lower()
    return any(lower.endswith(suffix) for suffix in EXCLUDED_SUFFIXES)


def _is_runtime_db_or_settlement(rel_from_results: Path) -> bool:
    parts = rel_from_results.parts
    if len(parts) != 2:
        return False
    folder, file_name = parts
    if folder == "db" and file_name.endswith(".sqlite"):
        return True
    if folder == "settlements" and file_name.endswith(".json"):
        return True
    return False


def _include_result_file(
    rel_from_results: Path,
    *,
    include_logs: bool,
) -> tuple[bool, str]:
    name = rel_from_results.name
    if name.startswith(".clob_") or name.startswith("__sync_tmp__"):
        return False, "transient temp/latency file"
    if name.endswith(".lock") or name.endswith(".pid"):
        return False, "live process lock/pid"
    if name.startswith("terminal_data.json.tmp."):
        return False, "atomic write temp file"
    if name in SECRETISH_FILENAMES:
        return False, "private key"
    if name.startswith(".vps_") or name.startswith(".local_calibration_offload"):
        return False, "legacy/offload runtime file"
    if _has_excluded_suffix(name):
        return False, "excluded suffix"
    if _is_runtime_db_or_settlement(rel_from_results):
        return True, "runtime db/settlement"
    if name in RUNTIME_EXACT_FILES:
        return True, "current runtime stats"
    if include_logs and name in OPTIONAL_LOG_FILES:
        return True, "optional runtime logs"
    return False, "not part of migration runtime state"


def _include_project_file(
    project: Path,
    path: Path,
    *,
    include_env: bool,
    include_logs: bool,
) -> tuple[bool, str]:
    name = path.name
    if name == ".env":
        return (include_env, "project env" if include_env else "env excluded")
    if name in SECRETISH_FILENAMES:
        return False, "private key"
    if name.endswith(".lock") or name.endswith(".pid"):
        return False, "process lock/pid"
    if name.endswith(".log") and not include_logs:
        return False, "log excluded"
    if _has_excluded_suffix(name):
        return False, "excluded suffix"

    if project == BOT_DIR and _is_under(path, BOT_DIR / "results"):
        return _include_result_file(
            path.relative_to(BOT_DIR / "results"),
            include_logs=include_logs,
        )

    return True, "source/config/assets"


def iter_candidate_files(
    projects: Iterable[Path],
    *,
    include_env: bool,
    include_logs: bool,
) -> tuple[list[CandidateFile], list[tuple[str, str]]]:
    candidates: list[CandidateFile] = []
    skipped: list[tuple[str, str]] = []

    for project in projects:
        if not project.exists():
            skipped.append((str(project), "project folder missing"))
            continue
        for root_text, dirs, files in os.walk(project):
            root = Path(root_text)
            dirs[:] = sorted(
                d for d in dirs
                if not _skip_dir(project, root, d)
            )
            for file_name in sorted(files):
                path = root / file_name
                include, reason = _include_project_file(
                    project,
                    path,
                    include_env=include_env,
                    include_logs=include_logs,
                )
                rel = path.relative_to(FREQTRADE_ROOT)
                if not include:
                    skipped.append((_as_posix(rel), reason))
                    continue
                try:
                    size = path.stat().st_size
                except OSError as exc:
                    skipped.append((_as_posix(rel), f"stat failed: {exc}"))
                    continue
                candidates.append(
                    CandidateFile(
                        path=path,
                        archive_name=_as_posix(rel),
                        size=size,
                        reason=reason,
                    )
                )

    return candidates, skipped


def build_manifest(
    candidates: list[CandidateFile],
    skipped: list[tuple[str, str]],
    *,
    include_env: bool,
    include_logs: bool,
    output_path: Path,
) -> dict:
    total_bytes = sum(item.size for item in candidates)
    contains_env = any(Path(item.archive_name).name == ".env" for item in candidates)
    by_reason: dict[str, int] = {}
    for item in candidates:
        by_reason[item.reason] = by_reason.get(item.reason, 0) + 1

    return {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_root": str(FREQTRADE_ROOT),
        "output_path": str(output_path),
        "contains_env_files": contains_env,
        "treat_archive_as_sensitive": bool(contains_env),
        "options": {
            "include_env": include_env,
            "include_logs": include_logs,
        },
        "projects": {
            BOT_DIR.name: {
                "path": str(BOT_DIR),
                "git": _git_info(BOT_DIR),
            },
            DASHBOARD_DIR.name: {
                "path": str(DASHBOARD_DIR),
                "git": _git_info(DASHBOARD_DIR),
            },
        },
        "file_count": len(candidates),
        "total_uncompressed_bytes": total_bytes,
        "included_by_reason": by_reason,
        "skipped_count": len(skipped),
        "skipped_preview": skipped[:200],
    }


def migration_readme(manifest: dict) -> str:
    env_note = (
        "This archive includes .env files. Treat it like a secret."
        if manifest["contains_env_files"]
        else "This archive was created without .env files; recreate secrets/config before running live."
    )
    return f"""15m Polymarket Project Migration Backup
Created: {manifest["created_at_utc"]}

{env_note}

Included:
- BTC_pricer_15m source/config/tests/Docker files.
- POLYMARKET_STREAMING_DASHBOARD_15m source/config/assets.
- Current dry-run and live stats from BTC_pricer_15m/results:
  state snapshots, trades, equity, leaderboard, terminal files, market DBs,
  and settlement JSON files.

Excluded intentionally:
- .git folders, caches, node_modules, frontend dist, logs by default, process
  locks/PIDs, temp files, old live-history backup folders, and private SSH key
  files.

Suggested restore outline:
1. Unzip this archive under a folder named freqtrade, preserving paths.
2. Install Docker Desktop, Python, and Node.js on the new PC.
3. From BTC_pricer_15m, rebuild the local services:
   docker compose --profile local-live up -d --build redis calc grid live
4. From POLYMARKET_STREAMING_DASHBOARD_15m/frontend, run npm install.
5. From POLYMARKET_STREAMING_DASHBOARD_15m, run:
   python .\\manage.py restart --no-grid

If you only want dry-run services on the new PC, omit the live service from
the docker compose command.
"""


def create_zip(
    output_path: Path,
    candidates: list[CandidateFile],
    manifest: dict,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        output_path,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
        allowZip64=True,
    ) as zf:
        zf.writestr("MIGRATION_MANIFEST.json", json.dumps(manifest, indent=2))
        zf.writestr("MIGRATION_README.txt", migration_readme(manifest))
        for item in candidates:
            zf.write(item.path, item.archive_name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Zip BTC_pricer_15m + POLYMARKET_STREAMING_DASHBOARD_15m for migration.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output ZIP path. Default: ../15m_migration_backups/polymarket_15m_migration_<UTC>.zip",
    )
    parser.add_argument(
        "--exclude-env",
        action="store_true",
        help="Do not include project .env files.",
    )
    parser.add_argument(
        "--include-logs",
        action="store_true",
        help="Also include current trader logs. Logs are skipped by default.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be included without writing a ZIP.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = args.output
    if output_path is None:
        output_path = DEFAULT_BACKUP_DIR / f"polymarket_15m_migration_{_utc_stamp()}.zip"
    if not output_path.is_absolute():
        output_path = (Path.cwd() / output_path).resolve()

    candidates, skipped = iter_candidate_files(
        [BOT_DIR, DASHBOARD_DIR],
        include_env=not args.exclude_env,
        include_logs=args.include_logs,
    )
    manifest = build_manifest(
        candidates,
        skipped,
        include_env=not args.exclude_env,
        include_logs=args.include_logs,
        output_path=output_path,
    )

    size_mb = manifest["total_uncompressed_bytes"] / (1024 * 1024)
    print(f"Files to include: {manifest['file_count']}")
    print(f"Uncompressed size: {size_mb:.1f} MB")
    print(f"Output: {output_path}")
    if manifest["contains_env_files"]:
        print("WARNING: .env files are included; treat this ZIP as sensitive.")
    if skipped:
        print(f"Skipped files/entries: {len(skipped)}")

    if args.dry_run:
        print("Dry run only; no ZIP written.")
        return 0

    create_zip(output_path, candidates, manifest)
    final_size_mb = output_path.stat().st_size / (1024 * 1024)
    print(f"Backup written: {output_path}")
    print(f"ZIP size: {final_size_mb:.1f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
