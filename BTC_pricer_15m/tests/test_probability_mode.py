import pytest

from cli_terminal import build_json_output, normalize_probability_mode


def test_mc_mode_selects_surface_mc_without_bl():
    output = build_json_output(
        None,
        100_000.0,
        101_000.0,
        "up",
        1.0 / 365.0,
        surface_mc_above=0.62,
        surface_mc_below=0.38,
        surface_r2=0.91,
        trading_model="ssvi_surface",
        probability_mode="mc",
    )

    assert output["preferred_model"] == "ssvi_surface"
    assert output["probability_mode"] == "mc"
    assert output["probability_source_label"] == "SSVI Surface MC"
    assert output["selected_prob_above"] == pytest.approx(0.62)
    assert output["selected_prob_below"] == pytest.approx(0.38)
    assert output["avg_prob_above"] == pytest.approx(0.62)
    assert output["ssvi_surface"]["prob_above"] is None
    assert output["ssvi_surface"]["mc_prob_above"] == pytest.approx(0.62)
    assert output["bl_mc_divergence"] is None


def test_avg_mode_preserves_surface_mc_bl_average():
    output = build_json_output(
        None,
        100_000.0,
        101_000.0,
        "up",
        1.0 / 365.0,
        surface_mc_above=0.60,
        surface_mc_below=0.40,
        surface_bl_above=0.70,
        surface_bl_below=0.30,
        trading_model="ssvi_surface",
        probability_mode="avg",
    )

    assert output["probability_source_label"] == "SSVI Surface MC + B-L average"
    assert output["selected_prob_above"] == pytest.approx(0.65)
    assert output["selected_prob_below"] == pytest.approx(0.35)
    assert output["avg_prob_above"] == pytest.approx(0.65)
    assert output["bl_mc_divergence"] == pytest.approx(0.10)


def test_bl_mode_selects_surface_bl_only():
    output = build_json_output(
        None,
        100_000.0,
        101_000.0,
        "up",
        1.0 / 365.0,
        surface_mc_above=0.60,
        surface_mc_below=0.40,
        surface_bl_above=0.70,
        surface_bl_below=0.30,
        trading_model="ssvi_surface",
        probability_mode="bl",
    )

    assert output["probability_source_label"] == "SSVI Surface B-L"
    assert output["selected_prob_above"] == pytest.approx(0.70)
    assert output["selected_prob_below"] == pytest.approx(0.30)
    assert output["avg_prob_above"] == pytest.approx(0.70)


def test_unknown_probability_mode_fails_fast():
    with pytest.raises(ValueError):
        normalize_probability_mode("mystery")
