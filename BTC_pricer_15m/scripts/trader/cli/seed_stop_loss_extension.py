"""Seed append-only stop-loss grid extensions from an existing SL history."""

from __future__ import annotations

import argparse
import copy
import csv
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from scripts.trader.config import GridConfig, InstanceParams
from scripts.trader.summary import write_leaderboard_csv
from scripts.trader.trader import PaperInstance


@dataclass(frozen=True)
class StopLossClone:
    source_id: int
    target_id: int
    target_params: InstanceParams


@dataclass(frozen=True)
class MigrationResult:
    base_count: int
    total_count: int
    mapping_count: int
    cloned_snapshot_instances: int
    adjusted_open_positions: int
    cloned_trade_rows: int
    backup_dir: Path | None


def _params_key(params: InstanceParams) -> tuple[float, float, float, float, float]:
    return (
        float(params.alpha_up),
        float(params.alpha_down),
        float(params.floor_up),
        float(params.floor_down),
        float(params.tp_percentage),
    )


def build_stop_loss_clone_map(
    config: GridConfig,
    *,
    source_sl: float,
    target_sl: float,
) -> list[StopLossClone]:
    base_params = config.generate_base_params()
    all_params = config.generate_params()
    base_count = len(base_params)

    if not any(abs(float(v) - target_sl) < 1e-9 for v in config.append_stop_loss_pct):
        raise ValueError(f"target SL {target_sl} is not in append_stop_loss_pct")

    source_by_key: dict[tuple[float, float, float, float, float], int] = {}
    for instance_id, params in enumerate(base_params):
        if abs(float(params.stop_loss_pct) - source_sl) < 1e-9:
            source_by_key[_params_key(params)] = instance_id

    mappings: list[StopLossClone] = []
    for instance_id, params in enumerate(all_params[base_count:], start=base_count):
        if abs(float(params.stop_loss_pct) - target_sl) >= 1e-9:
            continue
        key = _params_key(params)
        if key not in source_by_key:
            raise ValueError(
                "no source instance for appended stop-loss params "
                f"{params.label()} from source SL {source_sl}"
            )
        mappings.append(
            StopLossClone(
                source_id=source_by_key[key],
                target_id=instance_id,
                target_params=params,
            )
        )

    if not mappings:
        raise ValueError(f"no appended instances found for target SL {target_sl}")
    return mappings


def _backup(paths: list[Path], results_dir: Path) -> Path:
    backup_dir = (
        results_dir
        / "backups"
        / f"stop_loss_extension_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )
    backup_dir.mkdir(parents=True, exist_ok=False)
    for path in paths:
        if path.exists():
            shutil.copy2(path, backup_dir / path.name)
    return backup_dir


def _load_snapshot(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        snapshot = json.load(f)
    if not isinstance(snapshot, dict):
        raise ValueError(f"{path}: snapshot root must be an object")
    if not isinstance(snapshot.get("instances"), dict):
        raise ValueError(f"{path}: snapshot must contain instances object")
    return snapshot


def clone_snapshot(
    snapshot: dict,
    mappings: list[StopLossClone],
    *,
    total_count: int,
    target_sl: float,
) -> tuple[dict, int, int]:
    cloned = copy.deepcopy(snapshot)
    instances = cloned["instances"]
    cloned_count = 0
    adjusted_positions = 0

    for mapping in mappings:
        source_key = str(mapping.source_id)
        target_key = str(mapping.target_id)
        if target_key in instances:
            raise ValueError(f"target instance {mapping.target_id} already exists in snapshot")
        source = instances.get(source_key)
        if source is None:
            raise ValueError(f"source instance {mapping.source_id} missing from snapshot")
        target = copy.deepcopy(source)
        position = target.get("position")
        if isinstance(position, dict):
            entry_price = float(position.get("entry_price") or 0.0)
            position["stop_loss_price"] = entry_price * (1.0 - target_sl) if target_sl > 0 else 0.0
            adjusted_positions += 1
        instances[target_key] = target
        cloned_count += 1

    cloned["n_instances"] = total_count
    cloned["timestamp"] = datetime.now(timezone.utc).isoformat()
    return cloned, cloned_count, adjusted_positions


def _write_json_atomic(path: Path, payload: dict) -> None:
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f)
    tmp.replace(path)


def _target_trade_rows_exist(path: Path, mappings: list[StopLossClone]) -> bool:
    targets = {str(m.target_id) for m in mappings}
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if row.get("instance_id") in targets:
                return True
    return False


def clone_trades_file(path: Path, mappings: list[StopLossClone]) -> int:
    if not path.exists():
        raise FileNotFoundError(path)
    if _target_trade_rows_exist(path, mappings):
        raise ValueError("target instance rows already exist in trades.csv")

    source_to_target = {str(m.source_id): str(m.target_id) for m in mappings}
    cloned_rows = 0
    tmp = path.with_name(path.name + ".sl_extension.tmp")
    with path.open("r", encoding="utf-8", newline="") as src, tmp.open(
        "w", encoding="utf-8", newline=""
    ) as dst:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError(f"{path}: missing CSV header")
        writer = csv.DictWriter(dst, fieldnames=reader.fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in reader:
            writer.writerow(row)
            target_id = source_to_target.get(row.get("instance_id") or "")
            if target_id is None:
                continue
            cloned = dict(row)
            cloned["instance_id"] = target_id
            writer.writerow(cloned)
            cloned_rows += 1
    tmp.replace(path)
    return cloned_rows


def _instances_from_snapshot(snapshot: dict, config: GridConfig) -> list[PaperInstance]:
    params = config.generate_params()
    instances = [PaperInstance(i, p, config) for i, p in enumerate(params)]
    raw_instances = snapshot.get("instances") or {}
    for inst in instances:
        snap = raw_instances.get(str(inst.id))
        if snap is not None:
            inst.load_snapshot(snap)
    return instances


def run_migration(
    *,
    config_path: Path,
    results_dir: Path,
    source_sl: float = 0.20,
    target_sl: float = 0.40,
    backup: bool = True,
) -> MigrationResult:
    config = GridConfig.from_json(config_path)
    mappings = build_stop_loss_clone_map(
        config,
        source_sl=source_sl,
        target_sl=target_sl,
    )
    base_count = len(config.generate_base_params())
    total_count = len(config.generate_params())
    snapshot_path = results_dir / "state_snapshot.json"
    trades_path = results_dir / "trades.csv"
    leaderboard_path = results_dir / "leaderboard.csv"
    leaderboard_txt_path = results_dir / "leaderboard.txt"

    snapshot = _load_snapshot(snapshot_path)
    snap_count = int(snapshot.get("n_instances") or 0)
    if snap_count != base_count:
        raise ValueError(
            f"snapshot has {snap_count} instances, expected unmigrated base count {base_count}"
        )

    new_snapshot, cloned_instances, adjusted_positions = clone_snapshot(
        snapshot,
        mappings,
        total_count=total_count,
        target_sl=target_sl,
    )

    backup_dir = None
    if backup:
        config_backups = [
            config_path,
            config_path.parent / "config_grid.json",
            config_path.parent / "config_trader_live.json",
        ]
        backup_dir = _backup(
            [
                snapshot_path,
                trades_path,
                leaderboard_path,
                leaderboard_txt_path,
                *config_backups,
            ],
            results_dir,
        )

    cloned_trade_rows = clone_trades_file(trades_path, mappings)
    _write_json_atomic(snapshot_path, new_snapshot)
    instances = _instances_from_snapshot(new_snapshot, config)
    write_leaderboard_csv(instances, leaderboard_path, liquidity_mode=config.liquidity_mode)

    return MigrationResult(
        base_count=base_count,
        total_count=total_count,
        mapping_count=len(mappings),
        cloned_snapshot_instances=cloned_instances,
        adjusted_open_positions=adjusted_positions,
        cloned_trade_rows=cloned_trade_rows,
        backup_dir=backup_dir,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--results-dir", default="results")
    parser.add_argument("--source-sl", type=float, default=0.20)
    parser.add_argument("--target-sl", type=float, default=0.40)
    parser.add_argument("--no-backup", action="store_true")
    args = parser.parse_args()

    result = run_migration(
        config_path=Path(args.config),
        results_dir=Path(args.results_dir),
        source_sl=args.source_sl,
        target_sl=args.target_sl,
        backup=not args.no_backup,
    )
    print(
        "Seeded stop-loss extension: "
        f"{result.mapping_count} instances, "
        f"{result.cloned_trade_rows} trade rows, "
        f"snapshot {result.base_count}->{result.total_count}; "
        f"backup={result.backup_dir or 'disabled'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
