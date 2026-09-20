from __future__ import annotations

from pathlib import Path

import cli_terminal


def test_terminal_json_output_uses_atomic_writer(monkeypatch, tmp_path) -> None:
    calls: list[tuple[Path, dict]] = []

    def fake_atomic_write_json(path: Path, payload: dict) -> None:
        calls.append((path, payload))

    monkeypatch.setattr(cli_terminal, "atomic_write_json", fake_atomic_write_json)

    payload = {"market_slug": "btc-updown-15m-1778834700", "avg_prob_above": 0.61}
    path = tmp_path / "nested" / "terminal_data.shared_calc.json"

    cli_terminal._write_json_output(path, payload)

    assert calls == [(path, payload)]
