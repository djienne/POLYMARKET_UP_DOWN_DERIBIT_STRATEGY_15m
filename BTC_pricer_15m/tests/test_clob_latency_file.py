"""The CLOB latency hint file must never leave its temp file behind.

Regression: the old hand-rolled writer swallowed a failed rename without
unlinking, and ~770k ``.clob_*.tmp`` files piled up in results/ by 2026-11.
"""
from __future__ import annotations

import os
from collections import deque

from scripts.trader import order_book


def _isolate(monkeypatch, tmp_path):
    target = tmp_path / ".clob_latency_ms"
    monkeypatch.setattr(order_book, "_LATENCY_PATH", target)
    monkeypatch.setattr(order_book, "_LATENCY_WINDOW", deque(maxlen=12))
    return target


def test_writes_median_and_sample_count(tmp_path, monkeypatch):
    target = _isolate(monkeypatch, tmp_path)
    order_book._record_clob_latency(10.0)
    order_book._record_clob_latency(20.0)
    assert target.read_text().split()[:2] == ["15.0", "2"]
    assert [p.name for p in tmp_path.iterdir()] == [target.name]


def test_failed_rename_leaves_no_temp_file(tmp_path, monkeypatch):
    _isolate(monkeypatch, tmp_path)

    def refuse(src, dst):
        raise PermissionError("target held open by a reader")

    monkeypatch.setattr(os, "replace", refuse)
    order_book._record_clob_latency(10.0)
    order_book._record_clob_latency(20.0)  # must not raise either
    assert list(tmp_path.iterdir()) == []
