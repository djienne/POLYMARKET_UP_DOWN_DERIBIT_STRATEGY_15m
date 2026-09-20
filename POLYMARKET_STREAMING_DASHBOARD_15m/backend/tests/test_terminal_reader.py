import json
import os

import pytest

from app.collector import terminal_reader
from app.collector.terminal_reader import TerminalReader, parse_terminal


def test_v5_preserves_average_payoff_and_approximation_metadata():
    snapshot = parse_terminal({"schema_version": 5, "terminal_payload": {
        "selected_prob_above": .61, "selected_prob_below": .39,
        "payoff_kind": "chainlink_twap_60s", "averaging_window_seconds": 60,
        "probability_evaluated_at": "2026-09-05T10:00:00Z",
        "probability_source_label": "SSVI 60s average MC (approximate)",
        "model_assumption": "effective-volatility approximation",
    }})
    assert snapshot.probabilities.selected_above == .61
    assert snapshot.probabilities.payoff_kind == "chainlink_twap_60s"
    assert snapshot.probabilities.averaging_window_seconds == 60
    assert snapshot.probabilities.model_assumption == "effective-volatility approximation"
    assert "approximate" in snapshot.probabilities.probability_source_label


def _write_terminal(
    path,
    timestamp,
    mtime,
    timing,
    prob_above=0.61,
    prob_below=0.39,
    market_slug="btc-updown-15m-1770000000",
):
    payload = {
        "timestamp": timestamp,
        "ssvi_surface": {
            "prob_above": prob_above,
            "prob_below": prob_below,
        },
        "timing": timing,
    }
    if market_slug is not None:
        payload["market_slug"] = market_slug
    path.write_text(json.dumps(payload), encoding="utf-8")
    os.utime(path, (mtime, mtime))


def _write_shared_event(path, timestamp, mtime):
    path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "source": "shared_calc",
                "market_id": "btc-updown-15m-1770000900",
                "barrier": 70123.45,
                "ready_at_iso": timestamp,
                "prob_above": 0.75,
                "prob_below": 0.25,
                "calibration_seconds": 2.7,
                "terminal_payload": {
                    "timestamp": timestamp,
                    "avg_prob_above": 0.75,
                    "avg_prob_below": 0.25,
                    "ssvi_surface": {
                        "prob_above": 0.8,
                        "prob_below": 0.2,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    os.utime(path, (mtime, mtime))


def test_terminal_reader_derives_gap_without_carrying_source(tmp_path):
    path = tmp_path / "terminal_data.json"
    reader = TerminalReader(path)

    _write_terminal(
        path,
        "2026-04-23T10:00:00+00:00",
        1_777_000_000,
        {"calibration_s": 2.0, "used_source": "local_offload"},
    )
    first = reader.read_if_changed()
    assert first is not None
    assert first.timing.used_source == "local_offload"
    assert first.timing.used_gap_s is None

    _write_terminal(
        path,
        "2026-04-23T10:00:07.500000+00:00",
        1_777_000_008,
        {"calibration_s": 2.1},
    )
    second = reader.read_if_changed()

    assert second is not None
    assert second.timing.used_source is None
    assert second.timing.used_gap_s == pytest.approx(7.5)


def test_terminal_reader_detects_in_place_rewrite_with_tied_mtime(tmp_path):
    """When the file is rewritten with new content but the OS reports
    the same mtime (Windows NTFS ~16 ms resolution can do this if two
    writes land in the same tick), the reader must still detect the
    change via content hash and re-publish.

    Pre-fix the dashboard's spot price and Price-to-Beat got stuck on
    the value of the missed write until the user hit F5. Mirrors the
    in-place rewrite test for trades_tail (commit d89b82b)."""
    path = tmp_path / "terminal_data.json"
    reader = TerminalReader(path)

    _write_terminal(
        path,
        "2026-05-01T12:55:00+00:00",
        1_777_640_100,
        {"calibration_s": 2.0},
    )
    first = reader.read_if_changed()
    assert first is not None
    assert first.timestamp == "2026-05-01T12:55:00+00:00"

    # Rewrite with different content but pin mtime to the same value.
    _write_terminal(
        path,
        "2026-05-01T12:55:02+00:00",
        1_777_640_100,  # SAME mtime — simulates Windows tie
        {"calibration_s": 2.1},
    )
    second = reader.read_if_changed()
    assert second is not None, "missed in-place rewrite — UI would be stuck"
    assert second.timestamp == "2026-05-01T12:55:02+00:00"

    # And a third call with no further change correctly returns None.
    third = reader.read_if_changed()
    assert third is None


def test_terminal_reader_records_changed_probabilities_with_repeated_timestamp(tmp_path):
    path = tmp_path / "terminal_data.json"
    reader = TerminalReader(path)

    _write_terminal(
        path,
        "2026-05-01T12:55:00+00:00",
        1_777_640_100,
        {"calibration_s": 2.0},
        prob_above=0.61,
        prob_below=0.39,
    )
    first = reader.read_if_changed()
    assert first is not None

    _write_terminal(
        path,
        "2026-05-01T12:55:00+00:00",
        1_777_640_104,
        {"calibration_s": 2.1},
        prob_above=0.64,
        prob_below=0.36,
    )
    second = reader.read_if_changed()

    assert second is not None
    assert second.timestamp != first.timestamp
    assert second.probabilities.ssvi_surface_above == pytest.approx(0.64)
    assert reader.model_series("UP") == [
        {"t": first.timestamp, "v": 0.61},
        {"t": second.timestamp, "v": 0.64},
    ]


def test_terminal_reader_prefers_fresh_shared_payload_over_stale_terminal(tmp_path):
    stale = tmp_path / "terminal_data.json"
    fresh = tmp_path / "terminal_data.shared_calc.json"
    reader = TerminalReader([stale, fresh])

    _write_terminal(
        stale,
        "2026-05-01T12:55:00+00:00",
        1_777_640_100,
        {"calibration_s": 2.0},
        prob_above=0.1,
        prob_below=0.9,
    )
    _write_terminal(
        fresh,
        "2026-05-01T12:55:04+00:00",
        1_777_640_104,
        {"calibration_s": 2.1},
        prob_above=0.75,
        prob_below=0.25,
    )

    snap = reader.read_if_changed()

    assert snap is not None
    assert reader.path == fresh
    assert snap.timestamp == "2026-05-01T12:55:04+00:00"
    assert snap.probabilities.ssvi_surface_above == pytest.approx(0.75)


def test_terminal_reader_unwraps_shared_event_payload(tmp_path):
    path = tmp_path / "terminal_data.shared.json"
    reader = TerminalReader(path)

    _write_shared_event(path, "2026-05-01T12:55:04+00:00", 1_777_640_104)

    snap = reader.read_if_changed()

    assert snap is not None
    assert snap.timestamp == "2026-05-01T12:55:04+00:00"
    assert snap.market.slug == "btc-updown-15m-1770000900"
    assert snap.market.barrier == pytest.approx(70123.45)
    assert snap.probabilities.avg_above == pytest.approx(0.75)
    assert snap.timing.calibration_s == pytest.approx(2.7)


def test_terminal_reader_parses_selected_probability_metadata(tmp_path):
    path = tmp_path / "terminal_data.json"
    reader = TerminalReader(path)
    path.write_text(
        json.dumps(
            {
                "timestamp": "2026-05-01T12:55:04+00:00",
                "market_slug": "btc-updown-15m-1770000900",
                "probability_mode": "mc",
                "probability_source_label": "SSVI Surface MC",
                "selected_prob_above": 0.62,
                "selected_prob_below": 0.38,
                "avg_prob_above": 0.62,
                "avg_prob_below": 0.38,
                "ssvi_surface": {
                    "prob_above": None,
                    "prob_below": None,
                    "mc_prob_above": 0.62,
                    "mc_prob_below": 0.38,
                },
            }
        ),
        encoding="utf-8",
    )

    snap = reader.read_if_changed()

    assert snap is not None
    assert snap.probabilities.selected_above == pytest.approx(0.62)
    assert snap.probabilities.mc_above == pytest.approx(0.62)
    assert snap.probabilities.ssvi_surface_above is None
    assert snap.probabilities.probability_mode == "mc"
    assert snap.probabilities.probability_source_label == "SSVI Surface MC"


def test_terminal_reader_prefers_slugged_current_window_over_newer_no_slug(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(terminal_reader.time, "time", lambda: 1_778_780_800)
    slugged = tmp_path / "terminal_data.live.json"
    no_slug = tmp_path / "terminal_data.shared_calc.json"
    reader = TerminalReader([slugged, no_slug])

    _write_terminal(
        slugged,
        "2026-05-14T17:46:00+00:00",
        1_778_780_790,
        {"calibration_s": 2.0},
        prob_above=0.62,
        market_slug="btc-updown-15m-1778780700",
    )
    _write_terminal(
        no_slug,
        "2026-05-14T17:46:05+00:00",
        1_778_780_795,
        {"calibration_s": 2.1},
        prob_above=0.64,
        market_slug=None,
    )

    snap = reader.read_if_changed()

    assert snap is not None
    assert reader.path == slugged
    assert snap.market.slug == "btc-updown-15m-1778780700"
    assert snap.probabilities.ssvi_surface_above == pytest.approx(0.62)


def test_terminal_reader_rejects_newer_previous_window_when_current_exists(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(terminal_reader.time, "time", lambda: 1_778_780_810)
    current = tmp_path / "terminal_data.live.json"
    previous = tmp_path / "terminal_data.grid.json"
    reader = TerminalReader([current, previous])

    _write_terminal(
        current,
        "2026-05-14T17:45:05+00:00",
        1_778_780_805,
        {"calibration_s": 2.0},
        prob_above=0.61,
        market_slug="btc-updown-15m-1778780700",
    )
    _write_terminal(
        previous,
        "2026-05-14T17:45:06+00:00",
        1_778_780_808,
        {"calibration_s": 2.1},
        prob_above=0.1,
        market_slug="btc-updown-15m-1778779800",
    )

    snap = reader.read_if_changed()

    assert snap is not None
    assert reader.path == current
    assert snap.market.slug == "btc-updown-15m-1778780700"
    assert snap.probabilities.ssvi_surface_above == pytest.approx(0.61)


def test_terminal_reader_keeps_current_snapshot_when_current_file_is_half_written(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(terminal_reader.time, "time", lambda: 1_778_834_720)
    current = tmp_path / "terminal_data.shared_calc.json"
    previous = tmp_path / "terminal_data.previous.json"
    reader = TerminalReader([current, previous])

    _write_terminal(
        current,
        "2026-05-15T08:45:00+00:00",
        1_778_834_718,
        {"calibration_s": 2.0},
        prob_above=0.61,
        market_slug="btc-updown-15m-1778834700",
    )
    _write_terminal(
        previous,
        "2026-05-15T08:44:58+00:00",
        1_778_834_717,
        {"calibration_s": 2.1},
        prob_above=0.12,
        market_slug="btc-updown-15m-1778833800",
    )
    first = reader.read_if_changed()

    assert first is not None
    assert first.market.slug == "btc-updown-15m-1778834700"

    current.write_text("", encoding="utf-8")
    os.utime(current, (1_778_834_719, 1_778_834_719))

    second = reader.read_if_changed()

    assert second is None
    assert reader.latest is not None
    assert reader.latest.market.slug == "btc-updown-15m-1778834700"


def test_terminal_reader_infers_current_slug_for_only_no_slug_payload(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(terminal_reader.time, "time", lambda: 1_778_780_820)
    path = tmp_path / "terminal_data.shared_calc.json"
    reader = TerminalReader(path)

    _write_terminal(
        path,
        "2026-05-14T17:46:20+00:00",
        1_778_780_820,
        {"calibration_s": 2.0},
        market_slug=None,
    )

    snap = reader.read_if_changed()

    assert snap is not None
    assert snap.market.slug == "btc-updown-15m-1778780700"
    assert snap.market.window_start_unix == 1_778_780_700
    assert snap.market.window_end_unix == 1_778_781_600
