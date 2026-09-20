"""Legacy execution compatibility plus independent current-market fee checks."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Optional

import pytest

from scripts.trader.config import DEFAULT_DRY_RUN_FRICTION_PCT, TraderConfig
from scripts.trader.executor import Executor, OrderResult, normalize_limit_buy
from scripts.trader.executor_paper import PaperExecutor
from scripts.trader.order_book import OrderBookSnapshot, clone_order_book
from scripts.trader.position import Direction, MicroPosition, TradeResult
from scripts.trader.single_trader import SingleTrader
from scripts.trader.state import StateManager
from tests.conftest import make_order_book


# --------------------------------------------------------------------
# Shared fixtures
# --------------------------------------------------------------------


def _book(best_ask=0.55, best_bid=0.53, depth=1000.0) -> OrderBookSnapshot:
    book = make_order_book(best_bid=best_bid, best_ask=best_ask, depth=depth)
    return OrderBookSnapshot(
        token_id="tok",
        best_bid=book.best_bid,
        best_ask=book.best_ask,
        bids=book.bids[:2],
        asks=book.asks[:2],
        timestamp=book.timestamp,
    )


class _StubOrderBookClient:
    """Returns a single canned book — same data for paper and mock-live."""

    def __init__(self, book: OrderBookSnapshot):
        self._book = book

    def fetch_order_book(self, token_id, **kwargs):
        return clone_order_book(self._book)

    def simulate_market_buy(self, ob, usd_amount):
        remaining = usd_amount
        shares = 0.0
        cost = 0.0
        for price, size in ob.asks:
            if remaining <= 0:
                break
            level_cost = price * size
            if level_cost <= remaining:
                shares += size
                cost += level_cost
                remaining -= level_cost
            else:
                shares += remaining / price
                cost += remaining
                remaining = 0.0
        return shares, (cost / shares if shares > 0 else 0.0), cost

    def simulate_market_sell(self, ob, shares):
        remaining = shares
        proceeds = 0.0
        for price, size in ob.bids:
            if remaining <= 0:
                break
            if size <= remaining:
                proceeds += size * price
                remaining -= size
            else:
                proceeds += remaining * price
                remaining = 0.0
        shares_sold = shares - remaining
        return proceeds, (proceeds / shares_sold if shares_sold > 0 else 0.0), shares_sold


class _MockLiveExecutor(Executor):
    """Uses the SAME book-walk as PaperExecutor but reports is_live=True.

    A legacy zero-fee fixture, not a model of current production fee economics.
    Orders and positions are tracked in memory.
    """

    def __init__(self, order_book_client: _StubOrderBookClient):
        self._client = order_book_client
        self._positions: dict[str, float] = {}
        self._orders: dict[str, dict] = {}
        # Synthetic SELL trade history so get_proceeds_from_trades can
        # report on-chain proceeds without relying on Polymarket's
        # /data/trades endpoint. Populated by fill_limit_order /
        # sell_market.
        self._sell_trades: list[dict] = []
        self._next = 1

    @property
    def is_live(self) -> bool:
        return True

    def get_usdc_balance(self):
        return None  # let trader fall back to state.current_capital

    def buy_market(self, token_id, usd_amount):
        ob = self._client.fetch_order_book(token_id)
        shares, avg, cost = self._client.simulate_market_buy(ob, usd_amount)
        if cost < usd_amount * 0.99:
            return OrderResult(success=False, error_message="Insufficient liquidity")
        self._positions[token_id] = self._positions.get(token_id, 0.0) + shares
        oid = f"live_buy_{self._next}"
        self._next += 1
        return OrderResult(
            success=True, order_id=oid,
            shares_filled=shares, avg_fill_price=avg, total_cost=cost,
        )

    def buy_limit(self, token_id, price, shares, order_type="GTC", wait_for_fill_seconds=None):
        price, shares, _ = normalize_limit_buy(price, shares)
        ob = self._client.fetch_order_book(token_id)
        remaining = shares
        cost = 0.0
        for ask_price, ask_size in sorted(ob.asks, key=lambda level: level[0]):
            if ask_price > price:
                break
            take = min(remaining, ask_size)
            cost += take * ask_price
            remaining -= take
            if remaining <= 1e-9:
                break
        filled = shares - remaining
        if order_type.upper() == "FOK" and remaining > 1e-9:
            return OrderResult(success=False, error_message="Limit FOK not filled")
        if filled <= 1e-9:
            return OrderResult(success=False, error_message="Limit GTC: no asks at <= cap")
        self._positions[token_id] = self._positions.get(token_id, 0.0) + filled
        oid = f"live_buy_{self._next}"
        self._next += 1
        return OrderResult(
            success=True, order_id=oid,
            shares_filled=filled, avg_fill_price=cost / filled, total_cost=cost,
        )

    def sell_market(self, token_id, shares):
        ob = self._client.fetch_order_book(token_id)
        proceeds, avg, sold = self._client.simulate_market_sell(ob, shares)
        if sold < shares * 0.99:
            return OrderResult(success=False, error_message="Insufficient liquidity")
        self._positions[token_id] = max(0.0, self._positions.get(token_id, 0.0) - sold)
        oid = f"live_sell_{self._next}"
        self._next += 1
        self._sell_trades.append(
            {"order_id": oid, "token_id": token_id, "shares": sold, "price": avg}
        )
        return OrderResult(
            success=True, order_id=oid,
            shares_filled=sold, avg_fill_price=avg, total_cost=proceeds,
        )

    def sell_limit(self, token_id, price, shares, order_type="GTC"):
        if shares < 5.0:
            return OrderResult(success=False, error_message="min 5 shares")
        oid = f"live_lim_{self._next}"
        self._next += 1
        self._orders[oid] = {
            "status": "live",
            "size_matched": 0.0,
            "original_size": shares,
            "price": price,
            "side": "SELL",
            "token_id": token_id,
        }
        return OrderResult(success=True, order_id=oid,
                           shares_filled=0.0, avg_fill_price=price, total_cost=0.0)

    def get_order_status(self, order_id):
        return self._orders.get(order_id)

    def cancel_order(self, order_id):
        if order_id in self._orders:
            self._orders[order_id]["status"] = "cancelled"
            return True
        return False

    def get_position(self, token_id, strict=False):
        return self._positions.get(token_id, 0.0)

    def get_best_bid(self, token_id):
        return self._client.fetch_order_book(token_id).best_bid

    def get_best_ask(self, token_id):
        return self._client.fetch_order_book(token_id).best_ask

    def wait_for_position(self, token_id, target, comparison="gte", timeout_seconds=15.0):
        return self._positions.get(token_id, 0.0)

    def wait_for_order_on_book(self, order_id, timeout_seconds=15.0, verify_live=True):
        return self._orders.get(order_id)

    def get_usdc_balance(self):  # noqa: F811  (intentional override)
        return None

    def get_open_orders(self):
        return []

    def cancel_all_orders(self):
        for oid, order in self._orders.items():
            if order.get("status") == "live":
                order["status"] = "cancelled"
        return True

    # Test helper: simulate TP fill at the limit price.
    def fill_limit_order(self, order_id: str):
        order = self._orders.get(order_id)
        if not order:
            return
        order["status"] = "matched"
        order["size_matched"] = order["original_size"]
        order["price"] = order["price"]
        tok = order["token_id"]
        self._positions[tok] = max(0.0, self._positions.get(tok, 0.0) - order["original_size"])
        self._sell_trades.append(
            {
                "order_id": order_id,
                "token_id": tok,
                "shares": order["original_size"],
                "price": order["price"],
            }
        )

    def get_proceeds_from_trades(
        self,
        order_id=None,
        token_id=None,
        since_timestamp=None,
        target_shares=None,
    ):
        """Test stand-in for the live executor's /data/trades query.
        Sums recorded sells matching the order_id (or token_id if not
        provided). Returns 0.0 if nothing matched (mirrors the live
        path's "no fills found" semantics — caller still raises
        ProceedsUnverifiedError on 0)."""
        total = 0.0
        for tr in self._sell_trades:
            if order_id and tr.get("order_id") != order_id:
                continue
            if token_id and tr.get("token_id") != token_id:
                continue
            total += float(tr["shares"]) * float(tr["price"])
        return total


def _base_config(tmp_path: Path, paper_mode: bool, tag: str, **over) -> TraderConfig:
    prefix = "paper" if paper_mode else "live"
    return TraderConfig(
        paper_mode=paper_mode,
        live_trading_enabled=True,
        require_confirmation=False,
        paper_starting_capital=100.0,
        starting_capital=100.0,
        order_size_pct=0.50,
        min_order_size_usd=2.0,
        friction_pct=DEFAULT_DRY_RUN_FRICTION_PCT,
        tp_percentage=0.15,
        max_entry_price=0.95,
        # Gates off so parity focus stays on fill economics.
        max_slippage_estimate_pct=0.0,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
        reconciliation_enabled=False,
        state_file=tmp_path / f"{prefix}_state_{tag}.json",
        live_state_file=tmp_path / f"live_state_{tag}.json",
        trades_file=tmp_path / f"{prefix}_trades_{tag}.csv",
        live_trades_file=tmp_path / f"live_trades_{tag}.csv",
        equity_file=tmp_path / f"{prefix}_equity_{tag}.csv",
        live_equity_file=tmp_path / f"live_equity_{tag}.csv",
        orderbook_file=tmp_path / f"orderbook_{tag}.csv",
        risk_events_file=tmp_path / f"risk_{tag}.csv",
        friction_realized_file=tmp_path / f"friction_{tag}.csv",
        **over,
    )


def _make_single_trader(config: TraderConfig, executor: Executor, book_client: _StubOrderBookClient) -> SingleTrader:
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = config
    trader.verbose = False
    trader.state = StateManager(config)
    trader.state.current_market = {
        "market_id": "mkt-1",
        "title": "BTC test",
        "reference_price": 83000.0,
        "clob_token_ids": {"UP": "tok"},
    }
    trader.order_book_client = book_client
    trader.executor = executor
    trader.log = lambda message: None
    trader._current_market_id = "mkt-1"
    trader._tp_status_failures = {}
    trader._tp_backup_failures = {}
    trader._recently_settled = {}
    trader._start_time = datetime.now(timezone.utc)
    trader._last_heartbeat = None
    trader._last_reconciliation = None
    trader._last_tick = None
    trader._build_entry_context_from_tick = lambda opp: None
    return trader


def _opportunity():
    tick = SimpleNamespace(
        market_id="mkt-1",
        elapsed_seconds=400.0,
        time_remaining_seconds=500.0,
        prob_above=0.9,
        prob_below=0.1,
    )
    return {
        "direction": "UP",
        "polymarket_prob": 0.55,
        "tick": tick,
        "token_id": "tok",
        "edge": 1.10,
    }


# --------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------


class TestLegacyEntryCompatibility:

    def test_limit_buy_normalization_preserves_valid_polymarket_amounts(self):
        # Per upstream py-clob-client ROUNDING_CONFIG for tick_size=0.01:
        # size (shares) = 2 decimals, amount (USDC) = 4 decimals.
        # price=0.95, shares=6.0526...  →  size quantized to 6.05;
        # amount = 0.95 × 6.05 = 5.7475 fits 4dp → accepted.
        price, shares, max_cost = normalize_limit_buy(0.95, 5.75 / 0.95)

        assert price == 0.95
        assert shares == 6.05
        assert max_cost == 5.7475
        assert shares <= 5.75 / 0.95

    def test_limit_buy_normalization_uses_2dp_size_for_tick_001(self):
        """For tick=0.01 (BTC up/down markets) Polymarket requires shares
        rounded to 2 decimals, not 4. The earlier 4-decimal default caused
        order rejections when target_shares had >2 decimals (e.g. $6.41 /
        $0.57 = 11.2543...)."""
        price, shares, max_cost = normalize_limit_buy(
            0.57, 6.41 / 0.57, tick_size="0.01",
        )
        assert price == 0.57
        # shares quantized to 2dp; amount = 0.57 × shares fits 4dp
        assert (shares * 100) == int(shares * 100)
        # The amount must fit 4 decimals — the API constraint.
        from decimal import Decimal
        amt = Decimal(str(price)) * Decimal(str(shares))
        assert amt == amt.quantize(Decimal("0.0001"))

    def test_limit_buy_normalization_uses_3dp_price_2dp_size_for_tick_0001(self):
        """For tick=0.001, price has 3 decimals; size still 2dp; amount 5dp."""
        price, shares, max_cost = normalize_limit_buy(
            0.123, 12.345, tick_size="0.001",
        )
        # price quantized to 3dp
        assert price == 0.123
        # size to 2dp
        assert (shares * 100) == int(shares * 100)
        from decimal import Decimal
        amt = Decimal(str(price)) * Decimal(str(shares))
        assert amt == amt.quantize(Decimal("0.00001"))  # 5dp

    def test_limit_buy_normalization_unknown_tick_size_raises(self):
        with pytest.raises(ValueError, match="unknown tick_size"):
            normalize_limit_buy(0.5, 10.0, tick_size="0.5")

    def test_limit_buy_normalization_default_tick_is_001(self):
        """Default behavior (no tick_size passed) falls back to 0.01."""
        a = normalize_limit_buy(0.95, 5.75 / 0.95)
        b = normalize_limit_buy(0.95, 5.75 / 0.95, tick_size="0.01")
        assert a == b

    def test_paper_rejects_fok_entry_when_book_moved_above_cap(self, tmp_path):
        book = _book(best_ask=0.99, best_bid=0.98)
        paper_client = _StubOrderBookClient(book)
        paper_cfg = _base_config(tmp_path, paper_mode=True, tag="p_cap")
        paper_trader = _make_single_trader(
            paper_cfg,
            PaperExecutor(order_book_client=paper_client, config=paper_cfg),
            paper_client,
        )

        result = paper_trader.execute_entry(_opportunity())

        assert result.position is None


    def test_legacy_entry_shares_price_and_friction_accounting(self, tmp_path):
        book = _book(best_ask=0.55, best_bid=0.53)
        paper_client = _StubOrderBookClient(book)
        live_client = _StubOrderBookClient(book)
        paper_cfg = _base_config(tmp_path, paper_mode=True, tag="p2")
        live_cfg = _base_config(tmp_path, paper_mode=False, tag="l2")
        paper_trader = _make_single_trader(
            paper_cfg,
            PaperExecutor(order_book_client=paper_client, config=paper_cfg),
            paper_client,
        )
        live_trader = _make_single_trader(
            live_cfg, _MockLiveExecutor(live_client), live_client,
        )

        paper_pos = paper_trader.execute_entry(_opportunity()).position
        live_pos = live_trader.execute_entry(_opportunity()).position

        assert paper_pos is not None and live_pos is not None
        assert paper_pos.shares == pytest.approx(live_pos.shares, rel=0, abs=1e-6)
        assert paper_pos.entry_price == pytest.approx(live_pos.entry_price, rel=0, abs=1e-6)
        raw_cost = paper_pos.shares * paper_pos.entry_price
        # Paper adds the configured friction buffer.
        assert paper_pos.cost_basis == pytest.approx(raw_cost * (1 + paper_cfg.friction_pct), rel=1e-6)
        # Live cost basis = actual fill cost with no added buffer.
        assert live_pos.cost_basis == pytest.approx(raw_cost, rel=1e-6)


class TestLegacyExitCompatibility:
    """Keep the old fee=None fixture contract separate from modern market fees."""

    def test_tp_exit_pnl_differs_by_friction(self, tmp_path):
        book = _book(best_ask=0.55, best_bid=0.63)  # TP at ~0.63
        paper_client = _StubOrderBookClient(book)
        live_client = _StubOrderBookClient(book)
        paper_cfg = _base_config(tmp_path, paper_mode=True, tag="p3")
        live_cfg = _base_config(tmp_path, paper_mode=False, tag="l3")
        paper_trader = _make_single_trader(
            paper_cfg,
            PaperExecutor(order_book_client=paper_client, config=paper_cfg),
            paper_client,
        )
        live_trader = _make_single_trader(
            live_cfg, _MockLiveExecutor(live_client), live_client,
        )

        paper_pos = paper_trader.execute_entry(_opportunity()).position
        live_pos = live_trader.execute_entry(_opportunity()).position
        assert paper_pos and live_pos

        exit_price = paper_pos.tp_price  # same for both
        # Live now requires the TP order to actually have filled before
        # _record_exit will accept a TP_FILLED record (no more
        # fabricate-at-limit fallback). Simulate the fill on the mock
        # so get_proceeds_from_trades returns a real number.
        live_trader.executor.fill_limit_order(live_pos.tp_order_id)

        paper_trade = paper_trader._record_exit(paper_pos, exit_price, TradeResult.TP_FILLED)
        live_trade = live_trader._record_exit(live_pos, exit_price, TradeResult.TP_FILLED)

        # Paper: proceeds reduced by friction_pct; cost_basis inflated by friction_pct.
        # Live: raw in, raw out.
        shares = paper_pos.shares
        friction = paper_cfg.friction_pct

        raw_proceeds = shares * exit_price
        raw_cost = shares * paper_pos.entry_price

        expected_paper_pnl = raw_proceeds * (1 - friction) - raw_cost * (1 + friction)
        expected_live_pnl = raw_proceeds - raw_cost

        assert paper_trade.pnl == pytest.approx(expected_paper_pnl, rel=1e-4, abs=1e-4)
        assert live_trade.pnl == pytest.approx(expected_live_pnl, rel=1e-4, abs=1e-4)
        # Live P&L should be strictly greater (paper buffer is strictly positive).
        assert live_trade.pnl > paper_trade.pnl

    def test_expiry_settlement_has_no_friction(self, tmp_path):
        book = _book()
        paper_client = _StubOrderBookClient(book)
        paper_cfg = _base_config(tmp_path, paper_mode=True, tag="p4")
        paper_trader = _make_single_trader(
            paper_cfg,
            PaperExecutor(order_book_client=paper_client, config=paper_cfg),
            paper_client,
        )
        pos = paper_trader.execute_entry(_opportunity()).position
        trade = paper_trader._record_exit(pos, 1.0, TradeResult.WIN_EXPIRY)
        # Expiry settlement: proceeds = shares * 1.0 (no friction), cost_basis keeps buffer.
        assert trade.proceeds == pytest.approx(pos.shares * 1.0, rel=1e-6)


class TestCurrentMarketFees:
    FEES = {"enabled": True, "rate": .1, "exponent": 1, "takerOnly": True}

    def test_market_buy_keeps_fee_inside_budget_then_sell_reports_net_proceeds(self):
        book = OrderBookSnapshot("tok", .3, .4, [(.3, 100), (.2, 1000)], [(.4, 100), (.6, 1000)], "")
        executor = PaperExecutor(order_book_client=_StubOrderBookClient(book))
        executor.set_fee_schedule(self.FEES)
        bought = executor.buy_market("tok", 100)
        assert bought.success
        assert bought.shares_filled == pytest.approx(192.30768333333334)
        assert bought.total_cost == pytest.approx(100)
        assert bought.metadata["fee_usd"] == pytest.approx(4.61539)
        assert executor.get_position("tok") == pytest.approx(bought.shares_filled)
        # Changing the current market cannot change fees on the token already held.
        executor.set_fee_schedule({"enabled": False})
        sold = executor.sell_market("tok", bought.shares_filled)
        assert sold.success and sold.shares_filled == 192.3
        gross = 30 + .2 * 92.3
        fee = 2.1 + 1.4768
        assert sold.avg_fill_price == pytest.approx(gross / 192.3)
        assert sold.total_cost == pytest.approx(gross - fee)
        assert sold.metadata["fee_usd"] == pytest.approx(fee)
        assert executor.get_position("tok") == pytest.approx(bought.shares_filled - 192.3)

    def test_limit_buy_charges_each_executed_level_not_the_vwap_fee(self):
        book = OrderBookSnapshot("tok", .3, .4, [(.3, 1000)], [(.4, 100), (.6, 1000)], "")
        executor = PaperExecutor(order_book_client=_StubOrderBookClient(book))
        executor.set_fee_schedule(self.FEES)
        result = executor.buy_limit("tok", .6, 150, order_type="FOK")
        assert result.success and result.shares_filled == 150
        assert result.avg_fill_price == pytest.approx(70 / 150)
        assert result.total_cost == pytest.approx(73.6)  # notional70 + fee2.4 + fee1.2
        assert result.metadata["fee_usd"] == pytest.approx(3.6)

    def test_insufficient_sell_depth_leaves_position_and_book_unchanged(self):
        book = OrderBookSnapshot("tok", .5, .6, [(.5, 9.95)], [(.6, 1000)], "")
        executor = PaperExecutor(order_book_client=_StubOrderBookClient(book))
        executor.set_fee_schedule(self.FEES, token_id="tok")
        executor.set_position("tok", 10)
        result = executor.sell_market("tok", 10)
        assert not result.success
        assert executor.get_position("tok") == 10
        assert book.bids == [(.5, 9.95)]
