"""Tests for the one-time trades.csv header migration CLI."""
from __future__ import annotations

import csv
import gzip
from pathlib import Path

import pytest

from scripts.trader.cli import migrate_trades_header as migrate
from scripts.trader.trade_log import HEADER

LEGACY_HEADER = HEADER[:15]  # pre liquidity_mode/settlement_source/effective_tp_pct
NEW_COLUMNS = ("liquidity_mode", "settlement_source", "effective_tp_pct")


def _write_legacy_trades(path: Path, *, extra_rows: list[list[str]] | None = None) -> None:
    rows = [
        list(LEGACY_HEADER),
        ["0", "2026-04-12T20:51:58+00:00", "ENTRY", "UP", "btc-updown-15m-A",
         "0.4757", "", "210.2194", "", "", "898.50", "0.4804", "0.4650",
         "71311.58", "71342.09"],
        ["0", "2026-04-12T21:01:58+00:00", "TP_FILLED", "UP", "btc-updown-15m-A",
         "0.4757", "0.5500", "210.2194", "15.62", "15.62", "914.12", "0.4804",
         "0.4650", "71311.58", "71342.09"],
    ]
    rows.extend(extra_rows or [])
    with path.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)


@pytest.fixture
def env(tmp_path):
    trades = tmp_path / "trades.csv"
    _write_legacy_trades(trades)
    return {"dir": tmp_path, "trades": trades}


def _argv(env, *extra):
    return ["--trades", str(env["trades"]), *extra]


def _read_all(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_report_only_default_leaves_file_untouched(env, capsys):
    before = env["trades"].read_bytes()

    rc = migrate.main(_argv(env))

    assert rc == 0
    assert env["trades"].read_bytes() == before
    out = capsys.readouterr().out
    for col in NEW_COLUMNS:
        assert col in out
    assert "Report-only" in out
    assert not list(env["dir"].glob("trades.csv.*.bak.gz"))


def test_apply_migrates_legacy_file(env, capsys):
    rc = migrate.main(_argv(env, "--apply", "--force"))

    assert rc == 0
    with env["trades"].open(newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        rows = list(reader)
    assert header == HEADER
    assert len(rows) == 2
    assert all(len(r) == len(HEADER) for r in rows)
    by_name = _read_all(env["trades"])
    assert by_name[0]["event"] == "ENTRY"
    assert by_name[0]["entry_price"] == "0.4757"
    assert by_name[1]["pnl"] == "15.62"
    for row in by_name:
        for col in NEW_COLUMNS:
            assert row[col] == ""
    assert "header OK" in capsys.readouterr().out


def test_apply_is_idempotent_no_second_backup(env, capsys):
    assert migrate.main(_argv(env, "--apply", "--force")) == 0
    backups = list(env["dir"].glob("trades.csv.*.bak.gz"))
    assert len(backups) == 1
    after_first = env["trades"].read_bytes()
    capsys.readouterr()

    rc = migrate.main(_argv(env, "--apply", "--force"))

    assert rc == 0
    assert "Nothing to do" in capsys.readouterr().out
    assert env["trades"].read_bytes() == after_first
    assert list(env["dir"].glob("trades.csv.*.bak.gz")) == backups


def test_apply_refuses_with_fresh_trader_lock(env, capsys):
    (env["dir"] / "trader.lock").write_text("12345", encoding="utf-8")
    before = env["trades"].read_bytes()

    rc = migrate.main(_argv(env, "--apply"))

    assert rc == 2
    assert "grid appears to be running" in capsys.readouterr().out
    assert env["trades"].read_bytes() == before

    # --force bypasses
    assert migrate.main(_argv(env, "--apply", "--force")) == 0
    assert read_header(env["trades"]) == HEADER


def read_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as fh:
        return next(csv.reader(fh))


def test_short_row_padded_not_dropped(env):
    # simulate a crash-orphaned short row (5 cells)
    with env["trades"].open("a", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerow(["1", "2026-04-12T21:05:00+00:00", "ENTRY", "DOWN", "btc-updown-15m-B"])

    rc = migrate.main(_argv(env, "--apply", "--force"))

    assert rc == 0
    rows = _read_all(env["trades"])
    assert len(rows) == 3
    short = rows[2]
    assert short["market_id"] == "btc-updown-15m-B"
    assert short["entry_price"] == ""
    assert short["effective_tp_pct"] == ""


def test_extra_cells_dropped_with_counted_warning(env, capsys):
    with env["trades"].open("a", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerow(
            ["1", "2026-04-12T21:05:00+00:00", "ENTRY", "DOWN", "btc-updown-15m-B",
             "0.6000", "", "5.0", "", "", "100.0", "0.7", "0.6", "70000", "70100",
             "GARBAGE1", "GARBAGE2"]
        )

    rc = migrate.main(_argv(env, "--apply", "--force"))

    assert rc == 0
    out = capsys.readouterr().out
    assert "extra cells" in out
    rows = _read_all(env["trades"])
    assert len(rows) == 3
    flat = env["trades"].read_text(encoding="utf-8")
    assert "GARBAGE1" not in flat
    assert rows[2]["liquidity_mode"] == ""  # garbage did not bleed into new columns


def test_backup_gunzips_to_original_bytes(env):
    original = env["trades"].read_bytes()

    assert migrate.main(_argv(env, "--apply", "--force")) == 0

    backups = list(env["dir"].glob("trades.csv.*.bak.gz"))
    assert len(backups) == 1
    with gzip.open(backups[0], "rb") as fh:
        assert fh.read() == original


def test_unknown_column_in_header_refuses_apply(env, capsys):
    rows = [
        list(LEGACY_HEADER) + ["mystery_col"],
        ["0", "t", "ENTRY", "UP", "m", "0.5", "", "1", "", "", "100", "0.7",
         "0.5", "70000", "70100", "keep-me"],
    ]
    with env["trades"].open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)
    before = env["trades"].read_bytes()

    rc = migrate.main(_argv(env, "--apply", "--force"))

    assert rc == 2
    assert "mystery_col" in capsys.readouterr().out
    assert env["trades"].read_bytes() == before


def test_missing_and_empty_file(tmp_path, capsys):
    missing = tmp_path / "absent.csv"
    assert migrate.main(["--trades", str(missing)]) == 1

    empty = tmp_path / "trades.csv"
    empty.write_bytes(b"")
    assert migrate.main(["--trades", str(empty)]) == 0
    assert "Nothing to do" in capsys.readouterr().out
