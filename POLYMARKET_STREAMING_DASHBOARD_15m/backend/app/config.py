from pathlib import Path
from typing import Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Project root: POLYMARKET_STREAMING_DASHBOARD/ (two up from this file).
PROJECT_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", "../.env", str(PROJECT_ROOT / ".env")),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    results_dir: Path = Field(default=Path("../BTC_pricer_15m/results"))
    config_dir: Path = Field(default=Path("../BTC_pricer_15m"))
    mode: Literal["dry_run", "live"] = "dry_run"
    default_instance_id: int = 1271

    backend_host: str = "127.0.0.1"
    backend_port: int = 8798

    poll_interval_seconds: float = 0.5
    # Dominant source of entry-marker latency — stat is cheap, no reason
    # to wait 5 s. At 1 s the entry event surfaces ~1-2 s after the trader
    # fill instead of up to 7 s, so the marker no longer lands visibly in
    # the past on the live chart.
    state_poll_interval_seconds: float = 1.0
    leaderboard_poll_interval_seconds: float = 30.0
    calibration_timeout_seconds: float = 360.0

    docker_container: str = "btc_pricer_15m_grid"
    docker_logs_url: str = "http://docker-logs:8799"
    # Short cadence so new "Model: UP=X% DOWN=Y%" lines surface within a second
    docker_poll_interval_seconds: float = 1.0

    polymarket_gamma_url: str = "https://gamma-api.polymarket.com"
    polymarket_clob_url: str = "https://clob.polymarket.com"
    # 1s cadence is still comfortably below Polymarket's documented /book limit.
    polymarket_poll_interval_seconds: float = 1.0
    polymarket_request_timeout_seconds: float = 6.0
    # Statuspage state changes slowly; live CLOB/Gamma health is checked by the
    # market poller, so this can stay gentle without weakening the gate.
    # status.polymarket.com rate-limits per IP; 60 s across six pollers drew ~50% HTTP 429.
    polymarket_status_poll_interval_seconds: float = 300.0

    # Local Docker only: latency is measured from the dashboard host / local
    # trader side. Remote execution settings were intentionally removed.
    polymarket_probe_interval_seconds: float = 30.0

    redis_url: str = "redis://redis:6379/0"
    spot_latest_key: str = "market:spot:btc_usd:latest"
    spot_channel: str = "market.spot.btc_usd"
    spot_max_age_seconds: float = 10.0
    market_context_latest_key: str = "market:context:btc_updown_15m:latest"
    market_context_channel: str = "market.context.btc_updown_15m"
    market_context_current_grace_seconds: float = 30.0
    leaderboard_latest_key: str = "grid:leaderboard:15m:latest"
    leaderboard_channel: str = "grid.leaderboard.15m"

    log_level: str = "INFO"
    dashboard_api_token: Optional[str] = None
    debug: bool = False

    @staticmethod
    def _resolve(p: Path) -> Path:
        p = p.expanduser()
        if not p.is_absolute():
            p = (PROJECT_ROOT / p).resolve()
        else:
            p = p.resolve()
        return p

    @property
    def resolved_results_dir(self) -> Path:
        return self._resolve(self.results_dir)

    @property
    def resolved_config_dir(self) -> Path:
        return self._resolve(self.config_dir)

    def state_snapshot_path(self) -> Path:
        return self.resolved_results_dir / "state_snapshot.json"

    def live_state_path(self) -> Path:
        return self.resolved_results_dir / "15m_live_state.json"

    def live_equity_path(self) -> Path:
        return self.resolved_results_dir / "15m_live_equity.csv"

    def trades_path(self) -> Path:
        name = "trades.csv" if self.mode == "dry_run" else "15m_live_trades.csv"
        return self.resolved_results_dir / name

    def live_trades_path(self) -> Path:
        """Path to the live trader's CSV, ALWAYS 15m_live_trades.csv regardless
        of dashboard mode. Lets the dashboard tail BOTH grid and live trades
        files concurrently when both trader processes are running — fixes
        the bug where live closes (TP_FILLED / LOSS_EXPIRY / STOP_LOSS) never
        reached the trade feed because trades_path() picked one CSV by mode.
        """
        return self.resolved_results_dir / "15m_live_trades.csv"

    def terminal_path(self) -> Path:
        return self.resolved_results_dir / "terminal_data.json"

    def terminal_paths(self) -> list[Path]:
        shared_paths = [
            self.resolved_results_dir / "terminal_data.shared.json",
            self.resolved_results_dir / "terminal_data.shared_calc.json",
        ]
        if self.mode == "dry_run":
            return [
                *shared_paths,
                self.terminal_path(),
            ]
        return [
            *shared_paths,
            self.resolved_results_dir / "terminal_data.live.json",
            self.terminal_path(),
        ]

    def leaderboard_path(self) -> Path:
        return self.resolved_results_dir / "leaderboard.csv"

    def lock_path(self) -> Path:
        return self.resolved_results_dir / "trader.lock"

    def orderbook_path(self) -> Path:
        return self.resolved_results_dir / "15m_orderbook.csv"

    def trader_log_paths(self) -> list[Path]:
        return [
            self.resolved_results_dir / "trader.log",
            self.resolved_results_dir / "15m_trading.log",
        ]

    def grid_config_path(self) -> Path:
        return self.resolved_config_dir / "config_grid.json"

    def live_config_path(self) -> Path:
        return self.resolved_config_dir / "config_trader_live.json"

    def config_yaml_path(self) -> Path:
        return self.resolved_config_dir / "config.yaml"

    def live_location_path(self) -> Path:
        """Marker file written by live_manager.py: "local" | "stopped"."""
        return self.resolved_results_dir / ".live_location"


settings = Settings()
