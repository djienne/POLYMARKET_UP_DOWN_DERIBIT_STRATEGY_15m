from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Deque, Optional, Sequence

from ..config import settings
from ..events.bus import bus
from ..models import (
    MarketInfo,
    PolymarketPrices,
    ProbabilityBundle,
    TerminalSnapshot,
    TimingInfo,
)

MAX_MODEL_POINTS = 1000

log = logging.getLogger(__name__)


def _slug_window_start(slug: Optional[str]) -> Optional[int]:
    if not slug:
        return None
    import re

    m = re.search(r"btc-updown-15m-(\d+)", slug)
    if not m:
        return None
    return int(m.group(1))


def _slug_from_epoch(epoch: Optional[float]) -> Optional[str]:
    if epoch is None:
        return None
    return f"btc-updown-15m-{int(epoch // 900) * 900}"


def _is_expired_slug(slug: Optional[str], now: Optional[float] = None) -> bool:
    start = _slug_window_start(slug)
    if start is None:
        return False
    if now is None:
        now = time.time()
    return start + 900 <= now


def _to_title(slug: Optional[str]) -> Optional[str]:
    # We don't have the title in terminal_data.json; leave None and let UI derive.
    return None


def _normalize_terminal_payload(raw: dict) -> dict:
    payload = raw.get("terminal_payload")
    if not isinstance(payload, dict):
        return raw
    normalized = dict(payload)
    if normalized.get("market_slug") is None and raw.get("market_id") is not None:
        normalized["market_slug"] = raw.get("market_id")
    if normalized.get("target_price") is None and raw.get("barrier") is not None:
        normalized["target_price"] = raw.get("barrier")
    if normalized.get("timestamp") is None and raw.get("ready_at_iso") is not None:
        normalized["timestamp"] = raw.get("ready_at_iso")
    for key in (
        "spot_source",
        "spot_source_label",
        "spot_sequence",
        "spot_published_at_ms",
        "spot_details",
    ):
        if normalized.get(key) is None and raw.get(key) is not None:
            normalized[key] = raw.get(key)
    timing = dict(normalized.get("timing") or {})
    if timing.get("calibration_s") is None and raw.get("calibration_seconds") is not None:
        timing["calibration_s"] = raw.get("calibration_seconds")
    if timing:
        normalized["timing"] = timing
    return normalized


def parse_terminal(raw: dict, path_mtime: Optional[float] = None) -> TerminalSnapshot:
    raw = _normalize_terminal_payload(raw)
    ssvi = raw.get("ssvi_surface") or {}
    heston = raw.get("heston") or {}
    timing = raw.get("timing") or {}

    probs = ProbabilityBundle(
        ssvi_surface_above=ssvi.get("prob_above"),
        ssvi_surface_below=ssvi.get("prob_below"),
        mc_above=ssvi.get("mc_prob_above"),
        mc_below=ssvi.get("mc_prob_below"),
        heston_above=heston.get("prob_above") if isinstance(heston, dict) else None,
        heston_below=heston.get("prob_below") if isinstance(heston, dict) else None,
        bl_above=raw.get("bl_prob_above"),
        bl_below=raw.get("bl_prob_below"),
        avg_above=raw.get("avg_prob_above"),
        avg_below=raw.get("avg_prob_below"),
        selected_above=raw.get("selected_prob_above") if raw.get("selected_prob_above") is not None else raw.get("avg_prob_above"),
        selected_below=raw.get("selected_prob_below") if raw.get("selected_prob_below") is not None else raw.get("avg_prob_below"),
        probability_mode=raw.get("probability_mode"),
        probability_source_label=raw.get("probability_source_label"),
        payoff_kind=raw.get("payoff_kind"),
        averaging_window_seconds=raw.get("averaging_window_seconds"),
        probability_evaluated_at=raw.get("probability_evaluated_at"),
        model_assumption=raw.get("model_assumption"),
        bl_mc_divergence=raw.get("bl_mc_divergence"),
        preferred_model=raw.get("preferred_model"),
    )

    slug = raw.get("market_slug") or raw.get("slug")
    window_start = None
    window_end = None
    if slug:
        import re
        m = re.search(r"btc-updown-15m-(\d+)", slug)
        if m:
            window_start = int(m.group(1))
            window_end = window_start + 900

    market = MarketInfo(
        slug=slug,
        title=_to_title(slug),
        window_start_unix=window_start,
        window_end_unix=window_end,
        spot_price=raw.get("spot_price"),
        spot_source=raw.get("spot_source"),
        spot_source_label=raw.get("spot_source_label"),
        spot_sequence=raw.get("spot_sequence"),
        spot_published_at_ms=raw.get("spot_published_at_ms"),
        spot_age_seconds=(raw.get("spot_details") or {}).get("source_age_seconds")
        if isinstance(raw.get("spot_details"), dict)
        else None,
        barrier=raw.get("target_price") or raw.get("barrier"),
        direction=raw.get("direction"),
        ttm_days=raw.get("ttm_days"),
        ttm_seconds=(raw.get("ttm_days") or 0.0) * 86400.0 if raw.get("ttm_days") else None,
    )

    polymarket = PolymarketPrices(
        best_bid=raw.get("poly_best_bid"),
        best_ask=raw.get("poly_best_ask"),
        mid=raw.get("poly_mid"),
        prob_up=raw.get("poly_prob_up"),
        prob_down=raw.get("poly_prob_down"),
    )

    t = TimingInfo(
        calibration_s=timing.get("calibration_s"),
        surface_fit_s=timing.get("surface_fit_s"),
        mc_s=timing.get("mc_s"),
        bl_s=timing.get("bl_s"),
        surface_bl_s=timing.get("surface_bl_s"),
        used_gap_s=timing.get("used_gap_s"),
        used_source=timing.get("used_source"),
    )

    age = None
    if path_mtime is not None:
        age = max(0.0, time.time() - path_mtime)

    return TerminalSnapshot(
        timestamp=raw.get("timestamp"),
        market=market,
        probabilities=probs,
        polymarket=polymarket,
        timing=t,
        age_seconds=age,
    )


def _snapshot_epoch(snap: TerminalSnapshot, fallback_mtime: Optional[float]) -> Optional[float]:
    if snap.timestamp:
        text = snap.timestamp.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            pass
    return fallback_mtime


@dataclass
class _TerminalCandidate:
    mtime: float
    path: Path
    raw: dict
    raw_bytes: bytes
    content_hash: str
    snapshot: TerminalSnapshot
    explicit_slug: bool

    @property
    def slug(self) -> Optional[str]:
        return self.snapshot.market.slug


@dataclass
class _TerminalReadFailure:
    mtime: float
    path: Path
    reason: str


def _observed_gap_s(
    prev_snap: Optional[TerminalSnapshot],
    snap: TerminalSnapshot,
    prev_mtime: Optional[float],
    mtime: float,
) -> Optional[float]:
    if prev_snap is None:
        return None
    prev_epoch = _snapshot_epoch(prev_snap, prev_mtime)
    cur_epoch = _snapshot_epoch(snap, mtime)
    if prev_epoch is not None and cur_epoch is not None:
        gap = cur_epoch - prev_epoch
        if gap > 0:
            return gap
    if prev_mtime is not None:
        gap = mtime - prev_mtime
        if gap > 0:
            return gap
    return None


def _merge_timing(
    prev: Optional[TimingInfo],
    incoming: TimingInfo,
    observed_gap_s: Optional[float] = None,
) -> TimingInfo:
    if prev is None:
        if incoming.used_gap_s is None and observed_gap_s is not None:
            incoming.used_gap_s = observed_gap_s
        return incoming
    return TimingInfo(
        calibration_s=incoming.calibration_s,
        surface_fit_s=incoming.surface_fit_s,
        mc_s=incoming.mc_s,
        bl_s=incoming.bl_s,
        surface_bl_s=incoming.surface_bl_s,
        # Source/cadence fields describe this specific timing payload. Do not
        # carry them forward, or the UI can show stale source labels after the
        # live trader restarts.
        used_gap_s=incoming.used_gap_s if incoming.used_gap_s is not None else observed_gap_s,
        used_source=incoming.used_source,
    )


class TerminalReader:
    def __init__(self, path: Path | Sequence[Path]) -> None:
        self.paths = [path] if isinstance(path, Path) else list(path)
        if not self.paths:
            raise ValueError("TerminalReader needs at least one path")
        self.path = self.paths[0]
        self._last_mtime: Optional[float] = None
        self._last_observed_mtime: Optional[float] = None
        # SHA-256 of file content as of the last successful read. Used to
        # detect in-place rewrites where mtime ties — common on Windows
        # NTFS (~16 ms mtime resolution) when calc rewrites the file
        # twice within a single tick. Without this, the mtime-only check
        # silently drops the second write and the dashboard's spot price
        # gets stuck until the user F5's. Mirrors the trades_tail fix
        # in commit d89b82b.
        self._last_content_hash: Optional[str] = None
        self._last_path: Optional[Path] = None
        self._last: Optional[TerminalSnapshot] = None
        self._last_rejected_signature: Optional[tuple[str, Optional[str], Optional[str]]] = None
        # Per-market model probability history (reset when slug changes)
        self._model_up: Deque[tuple[str, float]] = deque(maxlen=MAX_MODEL_POINTS)
        self._model_down: Deque[tuple[str, float]] = deque(maxlen=MAX_MODEL_POINTS)
        self._cur_slug: Optional[str] = None

    def _candidate_paths(self) -> list[tuple[float, Path]]:
        candidates: list[tuple[float, Path]] = []
        for path in self.paths:
            try:
                candidates.append((path.stat().st_mtime, path))
            except FileNotFoundError:
                continue
            except OSError as exc:
                log.warning("terminal stat failed for %s: %s", path, exc)
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates

    @staticmethod
    def _infer_missing_slug(snap: TerminalSnapshot, path_mtime: float) -> bool:
        if snap.market.slug:
            return False
        inferred = _slug_from_epoch(_snapshot_epoch(snap, path_mtime) or path_mtime)
        if not inferred:
            return False
        start = _slug_window_start(inferred)
        snap.market.slug = inferred
        snap.market.window_start_unix = start
        snap.market.window_end_unix = start + 900 if start is not None else None
        return True

    def _candidate_score(self, candidate: _TerminalCandidate, now: float) -> tuple:
        start = _slug_window_start(candidate.slug)
        current_start = int(now // 900) * 900
        is_current = start == current_start
        is_future = start is not None and start > current_start
        is_expired = _is_expired_slug(candidate.slug, now)
        is_same_as_last = (
            self._last is not None
            and candidate.slug is not None
            and candidate.slug == self._last.market.slug
        )
        return (
            1 if is_current else 0,
            1 if is_future else 0,
            0 if is_expired else 1,
            1 if candidate.explicit_slug else 0,
            1 if is_same_as_last else 0,
            candidate.mtime,
        )

    def _read_candidates(self) -> tuple[list[_TerminalCandidate], list[_TerminalReadFailure]]:
        out: list[_TerminalCandidate] = []
        failures: list[_TerminalReadFailure] = []
        for path_mtime, path in self._candidate_paths():
            try:
                raw_bytes = path.read_bytes()
            except OSError as e:
                log.warning("terminal read failed for %s: %s", path, e)
                failures.append(_TerminalReadFailure(path_mtime, path, str(e)))
                continue
            new_hash = hashlib.sha256(raw_bytes).hexdigest()
            try:
                raw = json.loads(raw_bytes.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                log.warning("terminal parse failed for %s: %s", path, e)
                failures.append(_TerminalReadFailure(path_mtime, path, str(e)))
                continue
            snap = parse_terminal(raw, path_mtime)
            explicit_slug = bool(snap.market.slug)
            self._infer_missing_slug(snap, path_mtime)
            out.append(
                _TerminalCandidate(
                    mtime=path_mtime,
                    path=path,
                    raw=raw,
                    raw_bytes=raw_bytes,
                    content_hash=new_hash,
                    snapshot=snap,
                    explicit_slug=explicit_slug,
                )
            )
        return out, failures

    def _select_candidate(
        self,
        candidates: list[_TerminalCandidate],
        failures: list[_TerminalReadFailure],
    ) -> Optional[_TerminalCandidate]:
        if not candidates:
            return None
        now = time.time()
        selected = max(candidates, key=lambda item: self._candidate_score(item, now))
        if self._should_keep_last_instead(selected, failures, now):
            return None
        newest = max(candidates, key=lambda item: item.mtime)
        if newest.path != selected.path:
            newest_start = _slug_window_start(newest.slug)
            selected_start = _slug_window_start(selected.slug)
            regressed = (
                newest_start is None
                or (
                    selected_start is not None
                    and newest_start is not None
                    and newest_start < selected_start
                )
                or _is_expired_slug(newest.slug, now)
            )
            if regressed:
                sig = (str(newest.path), newest.slug, selected.slug)
                if sig != self._last_rejected_signature:
                    self._last_rejected_signature = sig
                    log.info(
                        "terminal candidate skipped: newest=%s slug=%s selected=%s slug=%s",
                        newest.path.name,
                        newest.slug,
                        selected.path.name,
                        selected.slug,
                    )
        return selected

    def _should_keep_last_instead(
        self,
        selected: _TerminalCandidate,
        failures: list[_TerminalReadFailure],
        now: float,
    ) -> bool:
        if self._last is None:
            return False
        selected_start = _slug_window_start(selected.slug)
        last_slug = self._last.market.slug
        last_start = _slug_window_start(last_slug)
        if selected_start is None or last_start is None:
            return False

        failed_newer_or_same = any(failure.mtime >= selected.mtime for failure in failures)
        selected_expired = _is_expired_slug(selected.slug, now)
        selected_regresses = selected_start < last_start
        last_still_active = not _is_expired_slug(last_slug, now)
        keep_last = (selected_expired and failed_newer_or_same) or (
            selected_regresses and (last_still_active or failed_newer_or_same)
        )
        if not keep_last:
            return False

        sig = ("keep_last", selected.slug, last_slug)
        if sig != self._last_rejected_signature:
            self._last_rejected_signature = sig
            failed_files = ", ".join(failure.path.name for failure in failures) or "(none)"
            log.info(
                "terminal candidate skipped: keeping last valid slug=%s instead of selected=%s "
                "(selected_expired=%s failed_newer_or_same=%s failed_files=%s)",
                last_slug,
                selected.slug,
                selected_expired,
                failed_newer_or_same,
                failed_files,
            )
        return True

    def _record_snapshot(self, raw: dict, path_mtime: float) -> Optional[TerminalSnapshot]:
        snap = parse_terminal(raw, path_mtime)
        self._infer_missing_slug(snap, path_mtime)
        if self._last is not None and self._last.timestamp == snap.timestamp:
            prev_epoch = _snapshot_epoch(self._last, self._last_observed_mtime)
            synthetic_epoch = path_mtime
            if prev_epoch is not None and synthetic_epoch <= prev_epoch:
                synthetic_epoch = max(time.time(), prev_epoch + 1e-6)
            snap.timestamp = datetime.fromtimestamp(
                synthetic_epoch,
                tz=timezone.utc,
            ).isoformat()
        prev_snap = None
        prev_timing = None
        if self._last is not None and self._last.market.slug == snap.market.slug:
            prev_snap = self._last
            prev_timing = self._last.timing
        snap.timing = _merge_timing(
            prev_timing,
            snap.timing,
            _observed_gap_s(prev_snap, snap, self._last_observed_mtime, path_mtime),
        )
        # Transition logging: when the slug changes (= market boundary
        # crossed) or the barrier (Price-To-Beat) changes, log timing so
        # we can correlate against frontend display delays.
        prev_slug = self._last.market.slug if self._last is not None else None
        prev_barrier = self._last.market.barrier if self._last is not None else None
        new_slug = snap.market.slug
        new_barrier = snap.market.barrier
        now_wall = time.time()
        file_age_at_read_s = max(0.0, now_wall - path_mtime)
        if prev_slug != new_slug:
            log.info(
                "terminal market_transition: %s ? %s | new strike=$%.2f | "
                "file mtime=%s file_age_at_read=%.2fs",
                prev_slug, new_slug,
                new_barrier if new_barrier is not None else float("nan"),
                datetime.fromtimestamp(path_mtime, tz=timezone.utc).isoformat(),
                file_age_at_read_s,
            )
        elif prev_barrier != new_barrier and new_barrier is not None:
            delta = (new_barrier - prev_barrier) if prev_barrier is not None else None
            log.info(
                "terminal barrier_update: %s strike $%s ? $%.2f%s | file_age=%.2fs",
                new_slug or "(no-slug)",
                f"{prev_barrier:.2f}" if prev_barrier is not None else "—",
                new_barrier,
                f" (?=${delta:+.2f})" if delta is not None else "",
                file_age_at_read_s,
            )
        self._last_observed_mtime = path_mtime
        self._last = snap
        self._record_model_probabilities(snap)
        return snap

    def _record_model_probabilities(self, snap: TerminalSnapshot) -> None:
        # Record model probabilities for the current market using the selected trader value.
        probs = snap.probabilities
        up = (
            probs.selected_above
            if probs.selected_above is not None
            else probs.avg_above
            if probs.avg_above is not None
            else probs.mc_above
            if probs.mc_above is not None
            else probs.ssvi_surface_above
        )
        down = (
            probs.selected_below
            if probs.selected_below is not None
            else probs.avg_below
            if probs.avg_below is not None
            else probs.mc_below
            if probs.mc_below is not None
            else probs.ssvi_surface_below
        )
        if up is not None or down is not None:
            ts = snap.timestamp or datetime.now(timezone.utc).isoformat()
            if up is not None:
                self._model_up.append((ts, float(up)))
            if down is not None:
                self._model_down.append((ts, float(down)))

    def read_if_changed(self) -> Optional[TerminalSnapshot]:
        candidates, failures = self._read_candidates()
        selected = self._select_candidate(candidates, failures)
        if selected is None:
            return None
        path_mtime, path = selected.mtime, selected.path
        self.path = path
        # Content-hash gate: short-circuit only when the FILE CONTENT is
        # unchanged. mtime alone is unreliable on Windows NTFS (~16 ms
        # resolution) — when calc rewrites the file twice within a single
        # tick, the second write would otherwise be silently dropped,
        # leaving the dashboard's spot/barrier values stuck until F5.
        # Mirrors the trades_tail fix from commit d89b82b.
        if (
            self._last_path == path
            and self._last_content_hash is not None
            and selected.content_hash == self._last_content_hash
        ):
            # still refresh age
            if self._last is not None:
                observed_mtime = self._last_observed_mtime or path_mtime
                self._last.age_seconds = max(0.0, time.time() - observed_mtime)
            return None
        self._last_mtime = path_mtime
        self._last_content_hash = selected.content_hash
        self._last_path = path
        return self._record_snapshot(selected.raw, path_mtime)

    def reset_history_if_new_slug(self, slug: Optional[str]) -> None:
        if slug and slug != self._cur_slug:
            self._cur_slug = slug
            self._model_up.clear()
            self._model_down.clear()

    def model_series(self, side: str) -> list[dict]:
        dq = self._model_up if side == "UP" else self._model_down
        return [{"t": t, "v": v} for (t, v) in dq]

    @property
    def latest(self) -> Optional[TerminalSnapshot]:
        # Ensure age is fresh
        if self._last is not None and self._last_mtime is not None:
            self._last.age_seconds = max(0.0, time.time() - self._last_mtime)
        return self._last


async def run_terminal_loop(reader: "TerminalReader", stop: asyncio.Event) -> None:
    last_logged_slug: Optional[str] = None
    while not stop.is_set():
        snap = reader.read_if_changed()
        if snap is not None:
            t0 = time.time()
            await bus.publish("terminal.update", snap.model_dump())
            publish_ms = (time.time() - t0) * 1000.0
            # Log every market transition with end-to-end push latency so
            # delays in the dashboard are easy to localize later.
            if snap.market.slug != last_logged_slug:
                log.info(
                    "terminal.update PUSHED: slug=%s strike=$%.2f publish_ms=%.1f",
                    snap.market.slug,
                    snap.market.barrier if snap.market.barrier is not None else float("nan"),
                    publish_ms,
                )
                last_logged_slug = snap.market.slug
        try:
            await asyncio.wait_for(stop.wait(), timeout=settings.poll_interval_seconds)
        except asyncio.TimeoutError:
            pass
