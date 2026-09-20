from __future__ import annotations

import json
from dataclasses import fields
from pathlib import Path

import pytest

from scripts.trader.config import TraderConfig


def test_trader_config_rejects_unknown_non_comment_keys(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config_trader_live.json"
    cfg_path.write_text(
        json.dumps({"paper_mode": False, "_note": "ok", "max_entyr_price": 0.91}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="max_entyr_price"):
        TraderConfig.from_json(cfg_path)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("order_size_pct", 0),
        ("max_entry_price", 1.2),
        ("max_slippage_estimate_pct", -0.01),
        ("entry_cancel_timeout_s", 0),
        ("live_entry_order_retries", -1),
        ("max_concurrent_positions", 0),
        ("signature_type", 9),
        ("ipc_backend", "pipe"),
        ("persistence_backend", "memory"),
    ],
)
def test_trader_config_rejects_invalid_live_risk_values(field_name: str, value) -> None:
    with pytest.raises(ValueError, match=field_name):
        TraderConfig(**{field_name: value})


def test_trader_config_to_json_emits_every_field_with_safe_types(tmp_path: Path) -> None:
    cfg = TraderConfig(
        paper_mode=False,
        polygon_rpc_urls=("https://one.example", "https://two.example"),
        live_state_file=tmp_path / "live_state.json",
    )
    out = tmp_path / "cfg.json"

    cfg.to_json(out)
    data = json.loads(out.read_text(encoding="utf-8"))

    assert set(data) == {item.name for item in fields(TraderConfig)}
    assert data["polygon_rpc_urls"] == ["https://one.example", "https://two.example"]
    assert data["live_state_file"] == str(tmp_path / "live_state.json")
