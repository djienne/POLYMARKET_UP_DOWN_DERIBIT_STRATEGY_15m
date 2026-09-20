from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .api.routes_http import router as http_router
from .api.routes_ws import router as ws_router
from .api.state_hub import get_hub
from .collector.calibration_watcher import run_calibration_loop
from .collector.docker_log_tail import run_docker_log_loop
from .collector.event_loop_monitor import run_event_loop_monitor
from .collector.equity_reader import run_equity_loop
from .collector.leaderboard_reader import run_leaderboard_loop
from .collector.leaderboard_redis import run_redis_leaderboard_loop
from .collector.liveness import run_liveness_loop
from .collector.location_probe import run_location_probe_loop
from .collector.market_context_reader import run_market_context_loop
from .collector.orderbook_tail import run_orderbook_loop
from .collector.polymarket_client import run_polymarket_loop
from .collector.polymarket_status import run_polymarket_status_loop
from .collector.spot_reader import run_spot_loop
from .collector.state_reader import run_state_loop
from .collector.terminal_reader import run_terminal_loop
from .collector.trades_tail import run_trades_loop
from .config import settings

log = logging.getLogger(__name__)


def _active_trade_tails(hub) -> list:
    """Return trade tails to seed/poll without watching the same file twice."""
    primary = getattr(hub, "trades_dry", hub.trades)
    tails = [primary]
    try:
        primary_path = primary.path.resolve()
        live_path = hub.trades_live.path.resolve()
    except OSError:
        primary_path = primary.path
        live_path = hub.trades_live.path
    if live_path != primary_path:
        tails.append(hub.trades_live)
    else:
        log.info("skipping duplicate live trade tail for %s", live_path)
    return tails


def _active_state_readers(hub) -> list:
    readers = [hub.state_dry]
    try:
        dry_path = hub.state_dry.path.resolve()
        live_path = hub.state_live.path.resolve()
    except OSError:
        dry_path = hub.state_dry.path
        live_path = hub.state_live.path
    if live_path != dry_path:
        readers.append(hub.state_live)
    else:
        log.info("skipping duplicate live state reader for %s", live_path)
    return readers


def _collector_supervisor(stop: asyncio.Event):
    """Done-callback for collector tasks: a dead collector means silently
    stale dashboard data, so surface it loudly. Reading task.exception() here
    also suppresses asyncio's "exception was never retrieved" noise."""

    def _on_task_done(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            log.error("collector task %s died: %r", task.get_name(), exc, exc_info=exc)
        elif not stop.is_set():
            log.error("collector task %s exited prematurely", task.get_name())

    return _on_task_done


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    hub = get_hub()

    # Prime readers once
    state_readers = _active_state_readers(hub)
    for reader in state_readers:
        reader.read_if_changed()
    hub.leaderboard.read_if_changed()
    hub.equity_live.read_if_changed()
    try:
        hub.redis_leaderboard.load_latest()
    except Exception:
        log.exception("redis leaderboard seed failed")
    hub.terminal.read_if_changed()
    try:
        hub.spot.load_latest()
    except Exception:
        log.exception("spot seed failed")
    try:
        hub.market_context.load_latest()
    except Exception:
        log.exception("market context seed failed")
    hub.orderbook.seed()  # populate UP/DOWN price history
    trade_tails = _active_trade_tails(hub)
    for tail in trade_tails:
        tail.request_seed("startup")
    try:
        await hub.polymarket.poll()
    except Exception:
        log.exception("polymarket seed failed")
    try:
        await hub.polymarket_status.poll()
    except Exception:
        log.exception("polymarket status seed failed")
    try:
        await hub.docker_log.poll(since_seconds=1200)
    except Exception:
        log.exception("docker log seed failed")

    stop = asyncio.Event()
    tasks = [
        asyncio.create_task(run_liveness_loop(stop), name="run_liveness_loop"),
        asyncio.create_task(run_event_loop_monitor(stop), name="run_event_loop_monitor"),
        asyncio.create_task(run_equity_loop(hub.equity_live, stop), name="run_equity_loop"),
        asyncio.create_task(run_location_probe_loop(stop), name="run_location_probe_loop"),
        asyncio.create_task(run_terminal_loop(hub.terminal, stop), name="run_terminal_loop"),
        *[
            asyncio.create_task(run_state_loop(reader, stop), name=f"run_state_loop[{i}]")
            for i, reader in enumerate(state_readers)
        ],
        *[
            asyncio.create_task(run_trades_loop(tail, stop), name=f"run_trades_loop[{i}]")
            for i, tail in enumerate(trade_tails)
        ],
        asyncio.create_task(run_redis_leaderboard_loop(hub.redis_leaderboard, stop), name="run_redis_leaderboard_loop"),
        asyncio.create_task(run_leaderboard_loop(hub.leaderboard, stop), name="run_leaderboard_loop"),
        asyncio.create_task(run_orderbook_loop(hub.orderbook, stop), name="run_orderbook_loop"),
        asyncio.create_task(run_spot_loop(hub.spot, stop), name="run_spot_loop"),
        asyncio.create_task(run_market_context_loop(hub.market_context, stop), name="run_market_context_loop"),
        asyncio.create_task(run_docker_log_loop(hub.docker_log, stop), name="run_docker_log_loop"),
        asyncio.create_task(run_polymarket_loop(hub.polymarket, stop), name="run_polymarket_loop"),
        asyncio.create_task(run_polymarket_status_loop(hub.polymarket_status, stop), name="run_polymarket_status_loop"),
        asyncio.create_task(run_calibration_loop(hub.calibration, stop), name="run_calibration_loop"),
    ]

    on_task_done = _collector_supervisor(stop)
    for t in tasks:
        t.add_done_callback(on_task_done)
    log.info("dashboard backend started: mode=%s results=%s", settings.mode, settings.resolved_results_dir)
    try:
        yield
    finally:
        stop.set()
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


def create_app() -> FastAPI:
    app = FastAPI(title="Polymarket Streaming Dashboard", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5175",
            "http://127.0.0.1:5175",
            "http://localhost:8798",
            "http://127.0.0.1:8798",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(http_router)
    app.include_router(ws_router)

    # Optional: serve frontend static build when it exists
    static_dir = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"
    if static_dir.exists():
        app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="frontend")
    return app


app = create_app()
