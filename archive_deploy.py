#!/usr/bin/env python3
"""Archive and redeploy the polymarket_15m project.

The archive is intended to be a full migration bundle: source code, Docker
configs, env files, runtime state/trade history, and Git history.

Usage:
    python archive_deploy.py archive              # Create a timestamped zip
    python archive_deploy.py archive my_backup    # Create my_backup.zip
    python archive_deploy.py deploy archive.zip   # Extract and set up from zip
    python archive_deploy.py deploy archive.zip --target D:\\projects\\polymarket_15m
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath, PureWindowsPath

PROJECT_ROOT = Path(__file__).resolve().parent

INCLUDE_DIRS = [
    ".git",
    "BTC_pricer_15m/btc_pricer",
    "BTC_pricer_15m/scripts",
    "BTC_pricer_15m/tests",
    "BTC_pricer_15m/docs",
    "POLYMARKET_STREAMING_DASHBOARD_15m/backend",
    "POLYMARKET_STREAMING_DASHBOARD_15m/scripts",
    "POLYMARKET_STREAMING_DASHBOARD_15m/tests",
    "POLYMARKET_STREAMING_DASHBOARD_15m/frontend/src",
    "POLYMARKET_STREAMING_DASHBOARD_15m/frontend/public",
]

RUNTIME_INCLUDE_DIRS = [
    "BTC_pricer_15m/results",
]

RUNTIME_INCLUDE_FILES = [
    "BTC_pricer_15m/results/.hybrid_calibration_meta.grid.json",
    "BTC_pricer_15m/results/.hybrid_calibration_meta.live.json",
    "BTC_pricer_15m/results/.hybrid_calibration_meta.json",
    "BTC_pricer_15m/results/.live_location",
    "BTC_pricer_15m/results/15m_friction_realized.csv",
    "BTC_pricer_15m/results/15m_live_equity.csv",
    "BTC_pricer_15m/results/15m_live_state.json",
    "BTC_pricer_15m/results/15m_live_trades.csv",
    "BTC_pricer_15m/results/15m_orderbook.csv",
    "BTC_pricer_15m/results/15m_paper_equity.csv",
    "BTC_pricer_15m/results/15m_paper_state.json",
    "BTC_pricer_15m/results/15m_paper_trades.csv",
    "BTC_pricer_15m/results/15m_risk_events.csv",
    "BTC_pricer_15m/results/leaderboard.csv",
    "BTC_pricer_15m/results/leaderboard.txt",
    "BTC_pricer_15m/results/ssvi_surface_warm.json",
    "BTC_pricer_15m/results/state_snapshot.json",
    "BTC_pricer_15m/results/terminal_data.grid.json",
    "BTC_pricer_15m/results/terminal_data.json",
    "BTC_pricer_15m/results/terminal_data.live.json",
    "BTC_pricer_15m/results/terminal_data.shared.json",
    "BTC_pricer_15m/results/terminal_data.shared_calc.json",
    "BTC_pricer_15m/results/trades.csv",
]

INCLUDE_FILES = [
    # Root
    ".gitignore",
    "AGENTS.md",
    "CLAUDE.md",
    "NEW_MACHINE_SETUP.md",
    "OPERATIONS.md",
    "archive_deploy.py",
    "manage.py",
    # BTC_pricer_15m configs & docker
    "BTC_pricer_15m/.env",
    "BTC_pricer_15m/.gitignore",
    "BTC_pricer_15m/.gitattributes",
    "BTC_pricer_15m/.dockerignore",
    "BTC_pricer_15m/config.yaml",
    "BTC_pricer_15m/config.json",
    "BTC_pricer_15m/config_grid.json",
    "BTC_pricer_15m/config_calc.json",
    "BTC_pricer_15m/config_trader.json",
    "BTC_pricer_15m/config_trader_live.json",
    "BTC_pricer_15m/config_mc_test.yaml",
    "BTC_pricer_15m/Dockerfile",
    "BTC_pricer_15m/docker-compose.yml",
    "BTC_pricer_15m/docker-compose.vps.yml",
    "BTC_pricer_15m/docker-compose.offload.yml",
    "BTC_pricer_15m/requirements.txt",
    "BTC_pricer_15m/requirements-docker.txt",
    "BTC_pricer_15m/pytest.ini",
    "BTC_pricer_15m/cli_terminal.py",
    "BTC_pricer_15m/fetch_ptb.py",
    "BTC_pricer_15m/run_grid_native.sh",
    "BTC_pricer_15m/README.md",
    "BTC_pricer_15m/HOW_TO_RUN.md",
    "BTC_pricer_15m/HOW_TO_SWITCH_BACKEND.md",
    "BTC_pricer_15m/VPS_SETUP.md",
    "BTC_pricer_15m/AGENTS.md",
    "BTC_pricer_15m/headless_ai_reviews.md",
    # Dashboard configs & docker
    "POLYMARKET_STREAMING_DASHBOARD_15m/.env",
    "POLYMARKET_STREAMING_DASHBOARD_15m/.env.example",
    "POLYMARKET_STREAMING_DASHBOARD_15m/.gitignore",
    "POLYMARKET_STREAMING_DASHBOARD_15m/.dockerignore",
    "POLYMARKET_STREAMING_DASHBOARD_15m/Dockerfile",
    "POLYMARKET_STREAMING_DASHBOARD_15m/AGENTS.md",
    "POLYMARKET_STREAMING_DASHBOARD_15m/manage.py",
    "POLYMARKET_STREAMING_DASHBOARD_15m/live_manager.py",
    "POLYMARKET_STREAMING_DASHBOARD_15m/README.md",
    "POLYMARKET_STREAMING_DASHBOARD_15m/QUICK_START.md",
    "POLYMARKET_STREAMING_DASHBOARD_15m/SERVICES.md",
    "POLYMARKET_STREAMING_DASHBOARD_15m/DASHBOARD_GUIDE.md",
    # Frontend build files (needed for npm install / vite build)
    "POLYMARKET_STREAMING_DASHBOARD_15m/frontend/package.json",
    "POLYMARKET_STREAMING_DASHBOARD_15m/frontend/package-lock.json",
    "POLYMARKET_STREAMING_DASHBOARD_15m/frontend/index.html",
    "POLYMARKET_STREAMING_DASHBOARD_15m/frontend/vite.config.ts",
    "POLYMARKET_STREAMING_DASHBOARD_15m/frontend/tsconfig.json",
    "POLYMARKET_STREAMING_DASHBOARD_15m/frontend/postcss.config.js",
    "POLYMARKET_STREAMING_DASHBOARD_15m/frontend/tailwind.config.js",
]

REQUIRED_FILES = [
    "manage.py",
    "archive_deploy.py",
    "BTC_pricer_15m/.env",
    "BTC_pricer_15m/config.yaml",
    "BTC_pricer_15m/docker-compose.yml",
    "BTC_pricer_15m/Dockerfile",
    "BTC_pricer_15m/requirements-docker.txt",
    "BTC_pricer_15m/scripts/trader/main.py",
    "BTC_pricer_15m/scripts/trader/calc_daemon.py",
    "POLYMARKET_STREAMING_DASHBOARD_15m/.env",
    "POLYMARKET_STREAMING_DASHBOARD_15m/Dockerfile",
    "POLYMARKET_STREAMING_DASHBOARD_15m/backend/pyproject.toml",
    "POLYMARKET_STREAMING_DASHBOARD_15m/frontend/package-lock.json",
    "POLYMARKET_STREAMING_DASHBOARD_15m/frontend/src/main.tsx",
    "BTC_pricer_15m/results/15m_live_trades.csv",
    "BTC_pricer_15m/results/15m_paper_trades.csv",
    "BTC_pricer_15m/results/trades.csv",
    "BTC_pricer_15m/results/15m_live_state.json",
    "BTC_pricer_15m/results/15m_paper_state.json",
    "BTC_pricer_15m/results/state_snapshot.json",
    "BTC_pricer_15m/results/db/15m_live.sqlite",
    "BTC_pricer_15m/results/db/markets.sqlite",
]

EXCLUDE_PATTERNS = {
    "__pycache__",
    ".pytest_cache",
    "node_modules",
    ".codex_tmp",
    "dist",
    "trader.lock",
    "single_trader.lock",
    ".pyc",
}

EXCLUDE_EXTENSIONS = {".pyc", ".pyo", ".tmp", ".log", ".pid"}


def _should_exclude(rel: str) -> bool:
    parts = Path(rel).parts
    for part in parts:
        if part in EXCLUDE_PATTERNS:
            return True
    if Path(rel).suffix in EXCLUDE_EXTENSIONS:
        return True
    return False


def _add_file(
    zf: zipfile.ZipFile,
    rel: str,
    written: set[str],
) -> tuple[bool, int]:
    rel = rel.replace("\\", "/")
    if rel in written or _should_exclude(rel):
        return False, 0
    src = PROJECT_ROOT / rel
    if not src.is_file():
        return False, 0
    zf.write(src, rel)
    written.add(rel)
    return True, src.stat().st_size


def _add_dir(
    zf: zipfile.ZipFile,
    dir_rel: str,
    written: set[str],
) -> tuple[int, int]:
    dir_path = PROJECT_ROOT / dir_rel
    if not dir_path.exists():
        print(f"[archive] SKIP (not found): {dir_rel}")
        return 0, 0
    count = 0
    total_bytes = 0
    for root, dirs, files in os.walk(dir_path):
        dirs[:] = [d for d in dirs if not _should_exclude((Path(root) / d).relative_to(PROJECT_ROOT).as_posix())]
        for f in files:
            full = Path(root) / f
            rel = full.relative_to(PROJECT_ROOT).as_posix()
            added, size = _add_file(zf, rel, written)
            if added:
                total_bytes += size
                count += 1
    return count, total_bytes


def cmd_archive(args: argparse.Namespace) -> int:
    name = args.name or time.strftime("polymarket_15m_%Y%m%d_%H%M%S", time.gmtime())
    out_path = (Path(args.output_dir) / name).with_suffix(".zip")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"[archive] Creating {out_path} ...")
    count = 0
    total_bytes = 0
    written: set[str] = set()
    missing_required: list[str] = []

    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        # Include explicit files
        for rel in INCLUDE_FILES:
            added, size = _add_file(zf, rel, written)
            if added:
                total_bytes += size
                count += 1

        # Include explicit runtime state files needed to resume on another PC.
        for rel in RUNTIME_INCLUDE_FILES:
            added, size = _add_file(zf, rel, written)
            if added:
                total_bytes += size
                count += 1

        # Include directories recursively
        for dir_rel in [*INCLUDE_DIRS, *RUNTIME_INCLUDE_DIRS]:
            added_count, added_bytes = _add_dir(zf, dir_rel, written)
            count += added_count
            total_bytes += added_bytes

        for rel in REQUIRED_FILES:
            if rel.replace("\\", "/") not in written:
                missing_required.append(rel)

        manifest = {
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_root": str(PROJECT_ROOT),
            "contains_env_files": any(Path(rel).name == ".env" for rel in written),
            "contains_git_history": (
                ".git/HEAD" in written
                and any(rel.startswith(".git/objects/") for rel in written)
            ),
            "contains_trade_history": {
                "live_trades_csv": "BTC_pricer_15m/results/15m_live_trades.csv" in written,
                "paper_trades_csv": "BTC_pricer_15m/results/15m_paper_trades.csv" in written,
                "grid_trades_csv": "BTC_pricer_15m/results/trades.csv" in written,
                "live_sqlite": "BTC_pricer_15m/results/db/15m_live.sqlite" in written,
                "markets_sqlite": "BTC_pricer_15m/results/db/markets.sqlite" in written,
            },
            "treat_archive_as_sensitive": True,
            "missing_required": missing_required,
            "file_count": count,
            "total_uncompressed_bytes": total_bytes,
            "included_files": sorted(written),
            "runtime_state_files": sorted(
                rel for rel in written if rel.startswith("BTC_pricer_15m/results/")
            ),
        }
        zf.writestr(
            "MIGRATION_ARCHIVE_MANIFEST.json",
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        )
        count += 1

    size_mb = out_path.stat().st_size / (1024 * 1024)
    raw_mb = total_bytes / (1024 * 1024)
    print(f"[archive] Done: {count} files, {raw_mb:.1f} MB raw -> {size_mb:.1f} MB compressed")
    print(f"[archive] Saved to: {out_path}")
    if missing_required:
        print("[archive] WARNING: missing required migration files:")
        for rel in missing_required:
            print(f"  - {rel}")
        return 2
    if any(Path(rel).name == ".env" for rel in written):
        print("[archive] WARNING: archive contains .env files/secrets; treat this ZIP as sensitive.")
    return 0


def _validate_zip_member(name: str) -> Path:
    normalized = name.replace("\\", "/")
    posix = PurePosixPath(normalized)
    windows = PureWindowsPath(name)
    if posix.is_absolute() or windows.is_absolute() or windows.drive:
        raise ValueError(f"unsafe absolute archive member: {name!r}")
    if any(part in {"", ".", ".."} for part in posix.parts):
        raise ValueError(f"unsafe archive member path: {name!r}")
    return Path(*posix.parts)


def _extract_zip_safely(zf: zipfile.ZipFile, target: Path) -> int:
    count = 0
    target_root = target.resolve()
    for info in zf.infolist():
        rel = _validate_zip_member(info.filename)
        dest = (target / rel).resolve()
        try:
            dest.relative_to(target_root)
        except ValueError as exc:
            raise ValueError(f"archive member escapes target: {info.filename!r}") from exc
        if info.is_dir():
            dest.mkdir(parents=True, exist_ok=True)
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        with zf.open(info, "r") as src, open(dest, "wb") as out:
            shutil.copyfileobj(src, out)
        count += 1
    return count


def cmd_deploy(args: argparse.Namespace) -> int:
    archive = Path(args.archive)
    if not archive.exists():
        print(f"[deploy] ERROR: archive not found: {archive}", file=sys.stderr)
        return 1

    target = Path(args.target) if args.target else archive.parent / "polymarket_15m"
    if target.exists() and any(target.iterdir()):
        if not args.force:
            print(f"[deploy] ERROR: target directory not empty: {target}", file=sys.stderr)
            print("[deploy] Use --force to overwrite, or pick a different --target")
            return 1

    with zipfile.ZipFile(archive, "r") as zf:
        contains_env = any(Path(info.filename).name == ".env" for info in zf.infolist())
        if contains_env:
            print("[deploy] WARNING: archive contains .env files/secrets; handle extracted files carefully.")
        if args.force:
            target.parent.mkdir(parents=True, exist_ok=True)
            temp_parent = target.parent
            temp_dir = Path(tempfile.mkdtemp(prefix=f".{target.name}.extract.", dir=temp_parent))
            try:
                print(f"[deploy] Validating and extracting {archive} -> {temp_dir} ...")
                count = _extract_zip_safely(zf, temp_dir)
                if target.exists():
                    shutil.rmtree(target)
                temp_dir.replace(target)
            except Exception:
                shutil.rmtree(temp_dir, ignore_errors=True)
                raise
        else:
            print(f"[deploy] Validating and extracting {archive} -> {target} ...")
            target.mkdir(parents=True, exist_ok=True)
            count = _extract_zip_safely(zf, target)
    print(f"[deploy] Extracted {count} files")

    # Post-deploy setup
    print()
    print("=" * 60)
    print("  POST-DEPLOY CHECKLIST")
    print("=" * 60)
    print()
    print("1. Install Docker Desktop and start it")
    print("   docker info")
    print()
    print("2. (Optional) Create conda env for credential management:")
    print("   conda create -n polymarket python=3.11 -y")
    print("   conda activate polymarket")
    print("   pip install py-clob-client python-dotenv")
    print()
    print("3. Check/refresh Polymarket API credentials:")
    print(f"   Edit: {target / 'BTC_pricer_15m' / '.env'}")
    print("   If API keys are stale, derive fresh ones:")
    print("   See NEW_MACHINE_SETUP.md Step 1")
    print()
    print("4. Start or restart all local Docker services, including live:")
    print(f"   cd {target}")
    print("   python .\\manage.py restart --build")
    print()
    print("5. Verify services and dashboard liveness:")
    print("   python .\\manage.py doctor")
    print()
    print("6. Open dashboard: http://127.0.0.1:5175")
    print()
    print("7. Common ops:")
    print("   python .\\manage.py start")
    print("   python .\\manage.py stop")
    print("   python .\\manage.py logs live --tail 160")
    print()
    print("The archive also contains MIGRATION_ARCHIVE_MANIFEST.json with the exact file list.")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Archive and redeploy the polymarket_15m project.",
    )
    sub = parser.add_subparsers(dest="command")

    p_archive = sub.add_parser("archive", help="Create a zip archive of the project.")
    p_archive.add_argument("name", nargs="?", default=None, help="Archive name (default: timestamped)")
    p_archive.add_argument(
        "--output-dir", "-o", default=str(PROJECT_ROOT),
        help="Directory to write the zip (default: parent of project)",
    )
    p_archive.set_defaults(func=cmd_archive)

    p_deploy = sub.add_parser("deploy", help="Extract and set up from a zip archive.")
    p_deploy.add_argument("archive", help="Path to the zip archive")
    p_deploy.add_argument("--target", "-t", default=None, help="Target directory (default: next to archive)")
    p_deploy.add_argument("--force", "-f", action="store_true", help="Overwrite existing target directory")
    p_deploy.set_defaults(func=cmd_deploy)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if not hasattr(args, "func"):
        args = parser.parse_args(["archive"])
    try:
        return args.func(args)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
