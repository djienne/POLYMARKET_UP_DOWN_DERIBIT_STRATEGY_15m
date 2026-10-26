"""Authoritative shared BTC/USD spot producer.

Runs one current-spot feed per stack and fans accepted ticks out through Redis.
Consumers should use ``spot_ipc`` instead of opening their own current spot
websockets.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

import yaml

from scripts.polymarket_btc_15m import _get_binance_rest_current_btc_price
from scripts.trader.chainlink_feed import ChainlinkPriceFeed
from scripts.trader.spot_aggregator import SpotAggregator
from scripts.trader.spot_ipc import (
    DEFAULT_CHANNEL,
    DEFAULT_LATEST_KEY,
    DEFAULT_MAX_AGE_SECONDS,
    DEFAULT_REDIS_URL,
    DEFAULT_STREAM,
    RedisSpotPublisher,
    SpotTick,
)

log = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config.yaml")
HEALTH_PATH = Path("results/spot_daemon.health")


@dataclass(frozen=True)
class SpotDaemonConfig:
    redis_url: str = DEFAULT_REDIS_URL
    latest_key: str = DEFAULT_LATEST_KEY
    stream: str = DEFAULT_STREAM
    channel: str = DEFAULT_CHANNEL
    redis_maxlen: int = 20_000
    rtds_stale_after_seconds: float = DEFAULT_MAX_AGE_SECONDS
    ws_fallback_enabled: bool = True
    ws_sources: tuple[str, ...] = (
        "coinbase",
        "kraken",
        "bitstamp",
        "gemini",
        "bitfinex",
        "binance",
    )
    ws_stale_after_seconds: float = 10.0
    ws_outlier_bps: float = 20.0
    ws_min_sources: int = 3
    rest_last_resort_enabled: bool = True
    publish_interval_seconds: float = 0.1
    fallback_republish_seconds: float = 1.0
    health_path: Path = HEALTH_PATH

    @classmethod
    def from_file(cls, path: Path) -> "SpotDaemonConfig":
        raw: dict = {}
        if path.exists():
            loaded = yaml.safe_load(path.read_text(encoding="utf-8")) if path.suffix.lower() in {".yaml", ".yml"} else json.loads(path.read_text(encoding="utf-8"))
            loaded = loaded or {}
            spot = dict(loaded.get("spot") or {})
            calc = dict(loaded.get("calc") or {})
            raw = {**calc, **spot}
        return cls(
            redis_url=str(raw.get("redis_url") or DEFAULT_REDIS_URL),
            latest_key=str(raw.get("latest_key") or DEFAULT_LATEST_KEY),
            stream=str(raw.get("stream") or raw.get("redis_stream") or DEFAULT_STREAM),
            channel=str(raw.get("channel") or DEFAULT_CHANNEL),
            redis_maxlen=int(raw.get("redis_maxlen") or 20_000),
            rtds_stale_after_seconds=float(
                raw.get("rtds_stale_after_seconds")
                or raw.get("spot_rtds_stale_after_seconds")
                or DEFAULT_MAX_AGE_SECONDS
            ),
            ws_fallback_enabled=bool(
                raw.get("ws_fallback_enabled", raw.get("spot_ws_fallback_enabled", True))
            ),
            ws_sources=tuple(
                raw.get("ws_sources")
                or raw.get("spot_ws_sources")
                or ("coinbase", "kraken", "bitstamp", "gemini", "bitfinex", "binance")
            ),
            ws_stale_after_seconds=float(
                raw.get("ws_stale_after_seconds")
                or raw.get("spot_ws_stale_after_seconds")
                or 10.0
            ),
            ws_outlier_bps=float(raw.get("ws_outlier_bps") or raw.get("spot_ws_outlier_bps") or 20.0),
            ws_min_sources=int(raw.get("ws_min_sources") or raw.get("spot_ws_min_sources") or 3),
            rest_last_resort_enabled=bool(
                raw.get("rest_last_resort_enabled", raw.get("spot_rest_last_resort_enabled", True))
            ),
            publish_interval_seconds=float(raw.get("publish_interval_seconds") or 0.1),
            fallback_republish_seconds=float(raw.get("fallback_republish_seconds") or 1.0),
            health_path=Path(raw.get("health_path") or HEALTH_PATH),
        )


class SpotDaemon:
    def __init__(self, config: SpotDaemonConfig) -> None:
        self.config = config
        self._stop = False
        self._sequence = 0
        self._last_signature: Optional[tuple[str, Optional[int], float]] = None
        self._last_publish_at = 0.0
        self._last_twap_timestamp = 0
        self._publisher = RedisSpotPublisher(
            redis_url=config.redis_url,
            latest_key=config.latest_key,
            stream=config.stream,
            channel=config.channel,
            maxlen=config.redis_maxlen,
        )
        self._chainlink = ChainlinkPriceFeed()
        self._aggregator = SpotAggregator(
            chainlink_feed=self._chainlink,
            enabled_feeds=list(config.ws_sources) if config.ws_fallback_enabled else [],
            rtds_stale_after_s=config.rtds_stale_after_seconds,
            ws_stale_after_s=config.ws_stale_after_seconds,
            outlier_bps=config.ws_outlier_bps,
            min_sources=config.ws_min_sources,
            rest_last_resort_enabled=config.rest_last_resort_enabled,
            rest_fallback_fn=_get_binance_rest_current_btc_price,
        )

    def start(self) -> None:
        self._chainlink.start()
        self._aggregator.start()
        log.info(
            "spot daemon started: redis=%s stream=%s channel=%s",
            self.config.redis_url,
            self.config.stream,
            self.config.channel,
        )

    def stop(self) -> None:
        self._stop = True
        try:
            self._aggregator.stop()
        finally:
            self._chainlink.stop()
            self._publisher.close()

    def request_stop(self) -> None:
        self._stop = True

    def run_forever(self) -> None:
        self.start()
        try:
            while not self._stop:
                try:
                    self._run_once()
                except Exception as exc:  # noqa: BLE001
                    log.warning("spot publish loop failed: %s", exc)
                time.sleep(max(0.01, self.config.publish_interval_seconds))
        finally:
            self.stop()

    def _run_once(self) -> None:
        self._publish_twap()
        result = self._aggregator.get_aggregated_spot()
        if result is None:
            return
        price, source, details = result
        source_ts_ms, received_at_ms = self._timestamps_for_source(source, details)
        rounded_price = round(float(price), 8)
        signature = (source, source_ts_ms, rounded_price)
        now = time.time()
        is_fallback = source != "polymarket_rtds"
        if (
            signature == self._last_signature
            and (not is_fallback or now - self._last_publish_at < self.config.fallback_republish_seconds)
        ):
            return
        self._sequence += 1
        tick = SpotTick.create(
            price=float(price),
            source=source,
            sequence=self._sequence,
            source_timestamp_ms=source_ts_ms,
            received_at_ms=received_at_ms,
            stale_after_seconds=self.config.rtds_stale_after_seconds,
            details=details,
        )
        self._publisher.publish(tick)
        self._last_signature = signature
        self._last_publish_at = now
        self._touch_health()
        log.info("spot tick #%d: $%.2f [%s]", tick.sequence, tick.price, tick.source)

    def _publish_twap(self) -> None:
        observation = self._chainlink.get_latest_twap()
        if observation is None or observation["source_timestamp_ms"] <= self._last_twap_timestamp:
            return
        self._publisher.publish_twap(observation)
        # Preserve source observations for payoff/model comparisons. One
        # compact daily file, with no credential or wallet information.
        day = datetime.fromtimestamp(observation["source_timestamp_ms"] / 1000.0, timezone.utc).strftime("%Y-%m-%d")
        path = self.config.health_path.parent / "oracle" / f"chainlink_twap_60s_{day}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(observation, separators=(",", ":")) + "\n")
        self._last_twap_timestamp = observation["source_timestamp_ms"]

    @staticmethod
    def _timestamps_for_source(source: str, details: dict) -> tuple[Optional[int], Optional[int]]:
        if source == "polymarket_rtds":
            rtds = details.get("rtds") if isinstance(details.get("rtds"), dict) else {}
            source_ts = rtds.get("source_timestamp_ms")
            receive_age = float(rtds.get("receive_age_seconds") or 0.0)
            received_at = int(time.time() * 1000 - receive_age * 1000)
            return (int(source_ts) if source_ts is not None else None, received_at)
        if source.startswith("ws_median"):
            return int(details.get("source_timestamp_ms") or 0), int(details.get("received_at_ms") or 0)
        now_ms = int(time.time() * 1000)
        return now_ms, now_ms

    def _touch_health(self) -> None:
        try:
            self.config.health_path.parent.mkdir(parents=True, exist_ok=True)
            self.config.health_path.write_text(str(time.time()), encoding="utf-8")
        except OSError as exc:
            log.debug("spot health write failed: %s", exc)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # -v is for this daemon's lines; websockets' DEBUG dumped every frame
    # (~75/s), rolling Docker's 100 MB log cap every ~35 min.
    logging.getLogger("websockets").setLevel(logging.INFO)
    config = SpotDaemonConfig.from_file(args.config)
    daemon = SpotDaemon(config)

    def _handle_signal(signum, _frame):  # noqa: ARG001
        log.info("received signal %s, stopping", signum)
        daemon.request_stop()

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    daemon.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
