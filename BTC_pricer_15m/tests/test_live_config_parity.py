from __future__ import annotations

import json

import pytest

from scripts.trader.config import TraderConfig
from scripts.trader.main import _verify_calc_live_config_alignment


def _write_config_yaml(path, *, order_size_pct=0.06, entry_cancel_timeout_s=2.0):
    path.write_text(
        f"""
live:
  paper_mode: false
  live_trading_enabled: true
  edge_alpha_up: 2.5
  edge_alpha_down: 1.8
  edge_floor_up: 0.45
  edge_floor_down: 0.65
  max_entry_price: 0.95
  order_size_pct: {order_size_pct}
  tp_percentage: 0.15
  no_trade_first_seconds: 300
  no_trade_last_seconds: 180
  min_order_size_usd: 2.0
  min_position_shares: 5.75
  max_slippage_estimate_pct: 0.02
  live_recheck_delay_seconds: 1.0
  live_entry_spot_recheck_enabled: true
  live_entry_spot_recheck_buffer_usd: 5.0
  entry_cancel_timeout_s: {entry_cancel_timeout_s}
  live_entry_order_retries: 5
  live_entry_retry_window_seconds: 5.0
  live_entry_retry_delay_seconds: 0.5
  live_entry_retry_price_guard: 0.02
""",
        encoding="utf-8",
    )


def test_live_config_alignment_fails_fast_on_entry_field_drift(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_config_yaml(tmp_path / "config.yaml", order_size_pct=0.06)
    live_path = tmp_path / "config_trader_live.json"
    live_path.write_text(
        json.dumps(
            {
                "paper_mode": False,
                "live_trading_enabled": True,
                "edge_alpha_up": 2.5,
                "edge_alpha_down": 1.8,
                "edge_floor_up": 0.45,
                "edge_floor_down": 0.65,
                "max_entry_price": 0.95,
                "order_size_pct": 0.07,
            }
        ),
        encoding="utf-8",
    )
    live_config = TraderConfig.from_json(live_path)

    with pytest.raises(ValueError, match="order_size_pct"):
        _verify_calc_live_config_alignment(live_config, live_path, fail_on_drift=True)


def test_live_config_alignment_can_warn_without_failing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _write_config_yaml(tmp_path / "config.yaml", entry_cancel_timeout_s=2.0)
    live_config = TraderConfig(entry_cancel_timeout_s=3.0, paper_mode=False)

    drift = _verify_calc_live_config_alignment(
        live_config,
        tmp_path / "intentional.json",
        fail_on_drift=False,
    )

    assert any("entry_cancel_timeout_s" in message for message in drift)
