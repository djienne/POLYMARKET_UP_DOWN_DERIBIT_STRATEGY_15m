"""Tests for parameter grid generation."""

from pathlib import Path
import json

import pytest
from scripts.trader.config import GridConfig, InstanceParams

FIXTURE_CONFIG = Path(__file__).parent / "fixtures" / "config_grid_test.json"


def test_grid_count():
    """Fixture grid should produce a stable known count."""
    config = GridConfig.from_json(FIXTURE_CONFIG)
    params = config.generate_params()
    assert len(params) == 4


def test_custom_grid(tmp_path):
    """A smaller grid should produce the correct count."""
    data = {
        "starting_capital": 500.0,
        "grid": {
            "alpha_up": [1.0, 2.0],
            "alpha_down": [1.0],
            "floor_up": [0.5],
            "floor_down": [0.5],
            "tp_percentage": [0.10, 0.20],
            "stop_loss_pct": [0.0],
        }
    }
    path = tmp_path / "custom_grid.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    config = GridConfig.from_json(path)

    params = config.generate_params()
    assert len(params) == 4  # 2 * 1 * 1 * 1 * 2 * 1
    assert config.starting_capital == 500.0


def test_append_stop_loss_extends_without_renumbering(tmp_path):
    data = {
        "grid": {
            "alpha_up": [1.0, 2.0],
            "alpha_down": [1.0],
            "floor_up": [0.5],
            "floor_down": [0.5],
            "tp_percentage": [0.10, 0.20],
            "stop_loss_pct": [0.0, 0.20],
        },
        "append_stop_loss_pct": [0.40],
    }
    path = tmp_path / "custom_grid.json"
    path.write_text(json.dumps(data), encoding="utf-8")

    config = GridConfig.from_json(path)
    base = config.generate_base_params()
    all_params = config.generate_params()

    assert len(base) == 8
    assert all_params[:8] == base
    assert len(all_params) == 12
    assert all(p.stop_loss_pct == 0.40 for p in all_params[8:])


def test_production_sl40_live_mirror_appended_id():
    root = Path(__file__).resolve().parents[1]
    config = GridConfig.from_json(root / "config.yaml")
    params = config.generate_params()

    assert len(config.generate_base_params()) == 864
    assert len(params) == 1296
    assert params[815] == InstanceParams(
        alpha_up=2.5,
        alpha_down=2.5,
        floor_up=0.45,
        floor_down=0.45,
        tp_percentage=0.30,
        stop_loss_pct=0.20,
    )
    assert params[1271] == InstanceParams(
        alpha_up=2.5,
        alpha_down=2.5,
        floor_up=0.45,
        floor_down=0.45,
        tp_percentage=0.30,
        stop_loss_pct=0.40,
    )


def test_append_stop_loss_rejects_duplicate_existing_axis(tmp_path):
    path = tmp_path / "bad_append.json"
    path.write_text(
        json.dumps(
            {
                "grid": {"stop_loss_pct": [0.0, 0.20]},
                "append_stop_loss_pct": [0.20],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must not duplicate"):
        GridConfig.from_json(path)


def test_grid_config_rejects_unknown_non_comment_keys(tmp_path):
    path = tmp_path / "bad_grid.json"
    path.write_text(
        json.dumps({"starting_capital": 500.0, "_note": "ok", "max_entyr_price": 0.9}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="max_entyr_price"):
        GridConfig.from_json(path)


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("starting_capital", 0),
        ("order_size_pct", 0),
        ("friction_pct", -0.01),
        ("max_entry_price", 1.2),
        ("market_scan_interval_seconds", 0),
        ("paper_delayed_book_slippage", -0.01),
    ],
)
def test_grid_config_rejects_invalid_ranges(field_name, value):
    with pytest.raises(ValueError, match=field_name):
        GridConfig(**{field_name: value})


def test_grid_config_rejects_unknown_grid_axis(tmp_path):
    path = tmp_path / "bad_axis.json"
    path.write_text(
        json.dumps({"grid": {"alpha_up": [1.0], "unknown_axis": [0.5]}}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown_axis"):
        GridConfig.from_json(path)
