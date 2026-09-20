"""Tests for the read-only availability endpoint helper."""
from __future__ import annotations

import csv
from pathlib import Path
import httpx
import pytest
from fastapi import FastAPI
from app.config import settings

from app.api.routes_http import _read_csv_dicts, router


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)


def test_read_csv_dicts_missing_file_is_empty(tmp_path):
    assert _read_csv_dicts(tmp_path / "nope.csv", 100) == []


def test_read_csv_dicts_returns_rows(tmp_path):
    p = tmp_path / "availability_ledger.csv"
    _write_csv(p, ["scope", "confidence"], [["platform", "confirmed"], ["market", "inferred"]])
    rows = _read_csv_dicts(p, 100)
    assert len(rows) == 2
    assert rows[0]["scope"] == "platform"
    assert rows[1]["confidence"] == "inferred"


def test_read_csv_dicts_limits_to_last_n(tmp_path):
    p = tmp_path / "voids.csv"
    _write_csv(p, ["market_id"], [[f"m{i}"] for i in range(10)])
    rows = _read_csv_dicts(p, 3)
    assert [r["market_id"] for r in rows] == ["m7", "m8", "m9"]


@pytest.mark.asyncio
async def test_availability_endpoint_enforces_auth_and_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(settings, "results_dir", tmp_path)
    monkeypatch.setattr(settings, "dashboard_api_token", "test-token")
    _write_csv(tmp_path / "availability_ledger.csv", ["market_id"], [["older"], ["latest"]])
    app = FastAPI()
    app.include_router(router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://test") as client:
        assert (await client.get("/api/availability")).status_code == 401
        client.headers["X-Dashboard-Token"] = "test-token"
        response = await client.get("/api/availability?limit=1")
        assert response.json() == {"intervals": [{"market_id": "latest"}], "voids": []}
        assert (await client.get("/api/availability?limit=0")).status_code == 422
