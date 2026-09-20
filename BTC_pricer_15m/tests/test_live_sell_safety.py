"""Regression tests for live sell cleanup and stop-loss safety."""

from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace

from scripts.trader.config import TraderConfig
from scripts.trader.executor import CancelResult, OrderResult
from scripts.trader.executor_live import LiveExecutor
from scripts.trader.position import Direction, MicroPosition, TradeResult
from scripts.trader.single_trader import SingleTrader
from tests.conftest import make_tick


def _install_fake_clob_types(monkeypatch):
    # Signing/hash correctness is exercised with the real SDK in the durable
    # submission tests; these fixtures exercise cancellation and balance races.
    monkeypatch.setattr(LiveExecutor, "_signed_order_id", lambda self, order, options: "fixture-signed-order")
    pkg = types.ModuleType("py_clob_client_v2")

    class MarketOrderArgs:
        def __init__(self, token_id: str, amount: float, side: str, **kwargs):
            self.token_id = token_id
            self.amount = amount
            self.side = side

    class OrderArgs:
        def __init__(self, token_id: str, price: float, size: float, side: str):
            self.token_id = token_id
            self.price = price
            self.size = size
            self.side = side

    class PartialCreateOrderOptions:
        def __init__(self, tick_size=None, neg_risk=None):
            self.tick_size = tick_size
            self.neg_risk = neg_risk

    class OrderType:
        FOK = "FOK"
        GTC = "GTC"
        GTD = "GTD"

    class Side:
        BUY = "BUY"
        SELL = "SELL"

    class AssetType:
        COLLATERAL = "COLLATERAL"
        CONDITIONAL = "CONDITIONAL"

    class BalanceAllowanceParams:
        def __init__(self, asset_type=None, token_id=None):
            self.asset_type = asset_type
            self.token_id = token_id

    class TradeParams:
        def __init__(self, maker_address=None, market=None, asset_id=None):
            self.maker_address = maker_address
            self.market = market
            self.asset_id = asset_id
            self.after = None

    pkg.MarketOrderArgs = MarketOrderArgs
    pkg.OrderArgs = OrderArgs
    pkg.OrderType = OrderType
    pkg.Side = Side
    pkg.PartialCreateOrderOptions = PartialCreateOrderOptions
    pkg.AssetType = AssetType
    pkg.BalanceAllowanceParams = BalanceAllowanceParams
    pkg.TradeParams = TradeParams
    pkg.OpenOrderParams = lambda: None
    monkeypatch.setitem(sys.modules, "py_clob_client_v2", pkg)


class _FakeBook:
    def __init__(self):
        self.bids = [SimpleNamespace(price="0.50")]
        self.asks = []


class _FakeClient:
    def __init__(self, response, pre_balance=None):
        self.response = response
        self.balance = "100000000"
        self.pre_balance = pre_balance
        self.submitted = False

    def get_order_book(self, token_id):
        return _FakeBook()

    def create_market_order(self, market_args, options=None):
        return market_args

    def create_order(self, order_args, options=None):
        return order_args

    def post_order(self, signed_order, order_type):
        self.submitted = True
        return self.response

    def create_and_post_market_order(self, *args, **kwargs):
        return self.response

    def create_and_post_order(self, *args, **kwargs):
        return self.response

    def get_tick_size(self, token_id):
        return "0.01"

    def get_neg_risk(self, token_id):
        return False

    def get_balance_allowance(self, params):
        if not self.submitted and self.pre_balance is not None:
            return {"balance": self.pre_balance}
        return {"balance": self.balance}

    def get_open_orders(self, params):
        return []


def test_sell_market_cancels_unresolved_delayed_sell(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({"status": "delayed", "orderID": "sell-123"})

    cancelled = []
    monkeypatch.setattr(executor, "_poll_delayed_order", lambda order_id: None)
    monkeypatch.setattr(executor, "cancel_order", lambda order_id: cancelled.append(order_id) or True)

    result = executor.sell_market("token-1", 12.5)

    assert cancelled == ["sell-123"]
    assert result.success is False
    assert result.order_id is None
    assert "Delayed sell order not filled" in result.error_message


def test_sell_market_cancels_unresolved_accepted_sell(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({"success": True, "status": "open", "orderID": "sell-456"})

    cancelled = []
    monkeypatch.setattr(executor, "_poll_delayed_order", lambda order_id: None)
    monkeypatch.setattr(executor, "cancel_order", lambda order_id: cancelled.append(order_id) or True)

    result = executor.sell_market("token-1", 12.5)

    assert cancelled == ["sell-456"]
    assert result.success is False
    assert result.order_id is None
    assert "Accepted sell order not matched" in result.error_message


def test_buy_market_cancels_unresolved_delayed_buy(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({"status": "delayed", "orderID": "buy-123"}, pre_balance="0")
    executor._client.balance = "0"
    executor._post_cancel_reconcile_seconds = 0.0

    cancelled = []
    monkeypatch.setattr(executor, "_poll_delayed_order", lambda order_id: None)
    monkeypatch.setattr(executor, "cancel_order", lambda order_id: cancelled.append(order_id) or True)

    class _BuyBook:
        def __init__(self):
            self.asks = [SimpleNamespace(price="0.50")]
            self.bids = []

    monkeypatch.setattr(executor._client, "get_order_book", lambda token_id: _BuyBook())

    result = executor.buy_market("token-1", 12.5)

    assert cancelled == ["buy-123"]
    assert result.success is False
    assert result.order_id is None
    assert "Delayed buy order not filled" in result.error_message


def test_buy_market_preserves_order_id_when_cancel_fails(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({"success": True, "status": "open", "orderID": "buy-456"}, pre_balance="0")

    monkeypatch.setattr(executor, "_poll_delayed_order", lambda order_id: None)
    monkeypatch.setattr(executor, "cancel_order", lambda order_id: False)

    class _BuyBook:
        def __init__(self):
            self.asks = [SimpleNamespace(price="0.50")]
            self.bids = []

    monkeypatch.setattr(executor._client, "get_order_book", lambda token_id: _BuyBook())

    result = executor.buy_market("token-1", 12.5)

    assert result.success is False
    assert result.order_id == "buy-456"
    assert "buy order may still be live" in result.error_message


def test_limit_buy_fok_matched_waits_for_visible_balance(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({
        "status": "matched",
        "orderID": "buy-visible",
        "takingAmount": "10",
        "makingAmount": "5.30",
    }, pre_balance="0")
    executor._client.balance = "10000000"

    result = executor.buy_limit("token-1", 0.53, 10.0, order_type="FOK")

    assert result.success is True
    assert result.shares_filled == 10.0
    assert result.avg_fill_price == 0.53


def test_limit_buy_fok_matched_but_balance_lag_is_ambiguous(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({
        "status": "matched",
        "orderID": "buy-lag",
        "takingAmount": "10",
        "makingAmount": "5.30",
    }, pre_balance="0")
    executor._client.balance = "0"
    monkeypatch.setattr(executor, "_wait_for_position_at_least", lambda *args, **kwargs: (False, 0.0))

    result = executor.buy_limit("token-1", 0.53, 10.0, order_type="FOK")

    assert result.success is False
    assert result.order_id == "buy-lag"
    assert result.ambiguous is True
    assert "balance is not visible" in result.error_message


def test_limit_buy_gtc_partial_fill_cancel_failure_is_ambiguous(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({"success": True, "status": "open", "orderID": "buy-partial"}, pre_balance="0")
    executor._client.balance = "6000000"

    monkeypatch.setattr(
        executor,
        "get_order_status",
        lambda order_id: {
            "status": "live",
            "size_matched": "6",
            "original_size": "10",
            "price": "0.55",
        },
    )
    monkeypatch.setattr(executor, "cancel_order", lambda order_id: False)

    result = executor.buy_limit(
        "token-1", 0.55, 10.0, order_type="GTC", wait_for_fill_seconds=0.001,
    )

    assert result.success is False
    assert result.order_id == "buy-partial"
    assert result.ambiguous is True
    assert result.shares_filled == 6.0
    assert "residual" in result.error_message


def test_limit_buy_gtc_partial_fill_succeeds_after_cancel_confirmed(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({"success": True, "status": "open", "orderID": "buy-partial"}, pre_balance="0")

    executor._client.balance = "6000000"
    cancel_called = False

    def fake_cancel(order_id):
        nonlocal cancel_called
        cancel_called = True
        return True

    def fake_status(order_id):
        return {
            "status": "cancelled" if cancel_called else "live",
            "size_matched": "6",
            "original_size": "10",
            "price": "0.55",
        }

    monkeypatch.setattr(executor, "get_order_status", fake_status)
    monkeypatch.setattr(executor, "cancel_order", fake_cancel)
    monkeypatch.setattr(executor, "_wait_for_position_at_least", lambda *args, **kwargs: (True, 6.0))

    result = executor.buy_limit(
        "token-1", 0.55, 10.0, order_type="GTC", wait_for_fill_seconds=0.001,
    )

    assert result.success is True
    assert result.order_id == "buy-partial"
    assert result.shares_filled == 6.0
    assert result.avg_fill_price == 0.55


def test_limit_buy_gtc_unfilled_cancel_runs_clean_reconcile(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({"success": True, "status": "open", "orderID": "buy-clean"}, pre_balance="0")
    executor._post_cancel_reconcile_seconds = 0.0

    cancel_called = False

    def fake_cancel(order_id):
        nonlocal cancel_called
        cancel_called = True
        return True

    def fake_status(order_id):
        return {
            "status": "cancelled" if cancel_called else "live",
            "size_matched": "0",
            "original_size": "10",
            "price": "0.55",
        }

    monkeypatch.setattr(executor, "get_position", lambda token_id, strict=False: 0.0)
    monkeypatch.setattr(executor, "get_order_status", fake_status)
    monkeypatch.setattr(executor, "cancel_order", fake_cancel)

    result = executor.buy_limit(
        "token-1", 0.55, 10.0, order_type="GTC", wait_for_fill_seconds=0.001,
    )

    assert result.success is False
    assert result.ambiguous is False
    assert result.order_id == "buy-clean"
    assert result.metadata["final_reason"] == "unfilled_timeout"


def test_limit_buy_gtc_post_cancel_late_balance_becomes_success(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({"success": True, "status": "open", "orderID": "buy-late-fill"}, pre_balance="0")
    executor._post_cancel_reconcile_seconds = 0.0

    cancel_called = False
    balances = iter([0.0, 10.0])

    def fake_cancel(order_id):
        nonlocal cancel_called
        cancel_called = True
        return True

    def fake_status(order_id):
        return {
            "status": "cancelled" if cancel_called else "live",
            "size_matched": "0",
            "original_size": "10",
            "price": "0.55",
        }

    monkeypatch.setattr(executor, "get_position", lambda token_id, strict=False: next(balances))
    monkeypatch.setattr(executor, "get_order_status", fake_status)
    monkeypatch.setattr(executor, "cancel_order", fake_cancel)
    monkeypatch.setattr(executor, "_wait_for_position_at_least", lambda *args, **kwargs: (True, 10.0))

    result = executor.buy_limit(
        "token-1", 0.55, 10.0, order_type="GTC", wait_for_fill_seconds=0.001,
    )

    assert result.success is True
    assert result.order_id == "buy-late-fill"
    assert result.shares_filled == 10.0
    assert result.metadata["final_reason"] == "post_cancel_fill_detected"


def test_limit_buy_gtc_post_cancel_unreadable_balance_is_ambiguous(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({"success": True, "status": "open", "orderID": "buy-unreadable"}, pre_balance="0")
    executor._post_cancel_reconcile_seconds = 0.0

    cancel_called = False

    def fake_cancel(order_id):
        nonlocal cancel_called
        cancel_called = True
        return True

    def fake_status(order_id):
        return {
            "status": "cancelled" if cancel_called else "live",
            "size_matched": "0",
            "original_size": "10",
            "price": "0.55",
        }

    def unreadable_balance(token_id, strict=False):
        raise RuntimeError("balance rpc unavailable")

    monkeypatch.setattr(executor, "get_position", unreadable_balance)
    monkeypatch.setattr(executor, "get_order_status", fake_status)
    monkeypatch.setattr(executor, "cancel_order", fake_cancel)

    result = executor.buy_limit(
        "token-1", 0.55, 10.0, order_type="GTC", wait_for_fill_seconds=0.001,
    )

    assert result.success is False
    assert result.order_id == "buy-unreadable"
    assert result.ambiguous is True
    assert result.metadata["final_reason"] == "post_cancel_balance_unreadable"


def test_get_proceeds_from_trades_matches_exact_maker_order(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._funder = "0xabc"
    executor._cached_uuid = "owner-1"
    executor._client = _FakeClient({})

    def fake_get_trades(params):
        assert params.asset_id == "token-1"
        return [{
            "trader_side": "MAKER",
            "taker_order_id": "different-order",
            "match_time": "2026-05-01T00:00:00Z",
            "maker_orders": [{
                "owner": "owner-1",
                "order_id": "our-maker-order",
                "matched_amount": "6",
                "price": "0.55",
                "asset_id": "token-1",
                "side": "SELL",
            }],
        }]

    executor._client.get_trades = fake_get_trades

    proceeds = executor.get_proceeds_from_trades(
        order_id="our-maker-order",
        token_id="token-1",
        since_timestamp=1.0,
        target_shares=6.0,
    )

    assert proceeds == 3.3
    assert executor.get_proceeds_from_trades("unrelated-order", "token-1", 1.0, 6.0) is None


def test_limit_sell_fok_matched_waits_for_balance_reduction(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({
        "status": "matched",
        "orderID": "sell-visible",
        "takingAmount": "7.20",
        "makingAmount": "10",
    })
    executor._client.balance = "10000000"
    monkeypatch.setattr(executor, "_wait_for_position_at_most", lambda *args, **kwargs: (True, 0.0))

    result = executor.sell_limit("token-1", 0.72, 10.0, order_type="FOK")

    assert result.success is True
    assert result.shares_filled == 10.0
    assert result.avg_fill_price == 0.72
    assert result.total_cost == 7.2


def test_sell_market_cancelled_resolution_is_not_ambiguous(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({"status": "delayed", "orderID": "sell-999"})

    monkeypatch.setattr(executor, "_poll_delayed_order", lambda order_id: {"status": "cancelled"})
    monkeypatch.setattr(executor, "cancel_order", lambda order_id: (_ for _ in ()).throw(AssertionError("cancel_order should not be called")))

    result = executor.sell_market("token-1", 12.5)

    assert result.success is False
    assert result.order_id is None
    assert "cancelled" in result.error_message


class _DummyState:
    def __init__(self):
        self.current_market = {
            "title": "Bitcoin Up or Down - Test",
            "reference_price": 83000.0,
            "clob_token_ids": {"UP": "token-1"},
        }
        self.current_capital = 100.0
        self.pending_entry = None
        self.known_token_ids = set()
        self.open_positions = []
        self.save_calls = 0
        self.risk_events: list[tuple[str, dict]] = []
        self.saved_risk_events = []
        self.saved_pending_entries = []

    def save(self):
        self.save_calls += 1
        self.saved_risk_events.append(list(self.risk_events))
        self.saved_pending_entries.append(dict(self.pending_entry) if self.pending_entry else None)

    def add_position(self, position):
        self.open_positions.append(position)
        self.current_capital -= position.cost_basis

    def log_risk_event(self, kind, details=None):
        self.risk_events.append((kind, details or {}))

    def log_equity_event(self, *args, **kwargs):
        pass

    def log_friction_realized(self, *args, **kwargs):
        pass


def test_execute_entry_tracks_ambiguous_pending_buy():
    class _Executor:
        is_live = True

        def get_usdc_balance(self):
            return None

        def buy_market(self, token_id, usd_amount):
            return OrderResult(
                success=False,
                order_id="buy-789",
                error_message="Buy order may still be live",
                ambiguous=True,
            )

    trader = SingleTrader.__new__(SingleTrader)
    # Disable new pre-trade gates so this test continues to exercise the
    # ambiguous-pending-buy fallthrough path.
    trader.config = TraderConfig(
        paper_mode=False,
        max_slippage_estimate_pct=0.0,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
    )
    trader.executor = _Executor()
    trader.state = _DummyState()
    trader.log = lambda message: None

    tick = make_tick()
    opportunity = {
        "direction": "UP",
        "polymarket_prob": 0.50,
        "tick": tick,
        "token_id": "token-1",
    }

    result = trader.execute_entry(opportunity)

    assert result.position is None
    assert trader.state.pending_entry["order_id"] == "buy-789"
    assert trader.state.pending_entry["token_id"] == "token-1"
    assert trader.state.pending_entry["market_id"] == tick.market_id
    assert "token-1" in trader.state.known_token_ids
    assert trader.state.saved_pending_entries[-1]["order_id"] == "buy-789"


def test_reconcile_pending_entry_adopts_matched_buy():
    class _Executor:
        is_live = True

        def get_order_status(self, order_id):
            return {"status": "matched", "size_matched": 10.0, "price": 0.55}

        def get_position(self, token_id, strict=False):
            return 10.0

        def get_trades(self, token_id, since_timestamp=None):
            return [{"taker_order_id": "buy-123", "size": 10.0, "price": 0.55, "side": "BUY"}]

    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    trader.executor = _Executor()
    trader.state = _DummyState()
    trader.state.pending_entry = {
        "order_id": "buy-123",
        "token_id": "token-1",
        "direction": "UP",
        "market_id": "btc-updown-15m-test",
        "market_title": "Bitcoin Up or Down - Test",
        "reference_price": 83000.0,
        "usd_amount": 5.5,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    trader.log = lambda message: None
    tp_calls = []
    trader._place_backup_tp_order = lambda position, reason: tp_calls.append((position.token_id, reason))

    trader._reconcile_pending_entry()

    assert trader.state.pending_entry is None
    assert len(trader.state.open_positions) == 1
    assert trader.state.open_positions[0].shares == 10.0
    assert trader.state.open_positions[0].reference_price == 83000.0
    assert tp_calls == [("token-1", "adopted_pending_entry")]


def test_reconcile_pending_entry_keeps_actual_shares_when_trade_history_is_partial():
    class _Executor:
        is_live = True

        def get_order_status(self, order_id):
            return {"status": "matched", "size_matched": 6.0, "price": 0.55}

        def get_position(self, token_id, strict=False):
            return 10.0

        def get_trades(self, token_id, since_timestamp=None):
            return [{"taker_order_id": "buy-124", "size": 6.0, "price": 0.55, "side": "BUY"}]

    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    trader.executor = _Executor()
    trader.state = _DummyState()
    trader.state.pending_entry = {
        "order_id": "buy-124",
        "token_id": "token-1",
        "direction": "UP",
        "market_id": "btc-updown-15m-test",
        "market_title": "Bitcoin Up or Down - Test",
        "reference_price": 83000.0,
        "usd_amount": 5.5,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    trader.log = lambda message: None
    tp_calls = []
    trader._place_backup_tp_order = lambda position, reason: tp_calls.append((position.shares, reason))

    trader._reconcile_pending_entry()

    assert trader.state.pending_entry is None
    assert len(trader.state.open_positions) == 1
    assert trader.state.open_positions[0].shares == 10.0
    assert tp_calls == [(10.0, "adopted_pending_entry")]


def test_settle_positions_uses_position_reference_price():
    class _State:
        def __init__(self, position):
            self.open_positions = [position]
            self.closed = []

        def close_position(self, position, trade):
            self.closed.append(trade)
            self.open_positions = []

        def log_equity_event(self, **kwargs):
            return None

        def save(self):
            return None

    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig()
    trader.executor = SimpleNamespace(is_live=False)
    trader.log = lambda message: None

    position = MicroPosition(
        direction=Direction.UP,
        token_id="token-1",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.0,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
        reference_price=83000.0,
    )
    trader.state = _State(position)

    tick = make_tick(
        is_expired=True,
        barrier=82000.0,
        final_spot=82500.0,
        spot_price=82500.0,
    )

    trader._settle_positions(tick)

    assert len(trader.state.closed) == 1
    assert trader.state.closed[0].result == TradeResult.LOSS_EXPIRY
    assert trader.state.closed[0].reference_price == 83000.0


def test_execute_stop_loss_live_skips_fallback_when_market_sell_is_ambiguous():
    class _Executor:
        def __init__(self):
            self.sell_limit_calls = []

        def cancel_order(self, order_id):
            return True

        def get_position(self, token_id, strict=False):
            return 10.0

        def sell_market(self, token_id, shares):
            return OrderResult(
                success=False,
                order_id="sell-789",
                error_message="Sell order may still be live",
            )

        def sell_limit(self, token_id, price, shares):
            self.sell_limit_calls.append((token_id, price, shares))
            return OrderResult(success=True, order_id="limit-1")

    saves = []
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig()
    trader.executor = _Executor()
    trader.state = SimpleNamespace(save=lambda: saves.append(True))
    trader._record_exit = lambda *args, **kwargs: None
    trader._sell_residual_shares = lambda *args, **kwargs: None

    position = MicroPosition(
        direction=Direction.UP,
        token_id="token-1",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.40,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
    )

    trader._execute_stop_loss_live(position, current_bid=0.39)

    assert trader.executor.sell_limit_calls == []
    assert saves == [True]
    assert position.pending_sl_order_id is None


def test_execute_stop_loss_live_allows_fallback_when_market_sell_is_confirmed_gone(monkeypatch):
    class _Trade:
        pnl = 1.0
        pnl_pct = 1.0

    class _Executor:
        def __init__(self):
            self.sell_limit_calls = []

        def cancel_order(self, order_id):
            return True

        def get_position(self, token_id, strict=False):
            return 10.0

        def sell_market(self, token_id, shares):
            return OrderResult(
                success=False,
                order_id=None,
                error_message="Delayed sell order not filled: cancelled",
            )

        def sell_limit(self, token_id, price, shares):
            self.sell_limit_calls.append((token_id, price, shares))
            return OrderResult(success=True, order_id="limit-2")

        def get_order_status(self, order_id):
            return {"status": "matched", "price": 0.38}

    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda seconds: None)

    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig()
    trader.executor = _Executor()
    trader.state = SimpleNamespace(save=lambda: None)
    trader._record_exit = lambda *args, **kwargs: _Trade()
    trader._sell_residual_shares = lambda *args, **kwargs: None

    position = MicroPosition(
        direction=Direction.UP,
        token_id="token-1",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.40,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
    )

    trader._execute_stop_loss_live(position, current_bid=0.39)

    assert trader.executor.sell_limit_calls == [("token-1", 0.38, 10.0)]


def test_execute_stop_loss_live_records_unknown_post_exit_balance(monkeypatch):
    class _Trade:
        pnl = 1.0
        pnl_pct = 1.0

    class _Executor:
        def __init__(self):
            self.position_strict_flags = []

        def cancel_order(self, order_id):
            return True

        def get_position(self, token_id, strict=False):
            self.position_strict_flags.append(strict)
            if len(self.position_strict_flags) == 1:
                return 10.0
            raise RuntimeError("balance unavailable")

        def sell_market(self, token_id, shares):
            return OrderResult(
                success=True,
                avg_fill_price=0.38,
                shares_filled=shares,
            )

    monkeypatch.setattr("scripts.trader.single_trader.time.sleep", lambda seconds: None)

    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig()
    trader.executor = _Executor()
    trader.state = _DummyState()
    trader._record_exit = lambda *args, **kwargs: _Trade()
    trader._sell_residual_shares = (
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("residual sell requires a verified balance")
        )
    )

    position = MicroPosition(
        direction=Direction.UP,
        token_id="token-1",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.40,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
    )

    trader._execute_stop_loss_live(position, current_bid=0.39)

    assert trader.executor.position_strict_flags == [True, True]
    assert trader.state.risk_events[0][0] == "stop_loss_balance_unknown"
    assert trader.state.saved_risk_events[-1][0][0] == "stop_loss_balance_unknown"


def test_live_stop_loss_check_skips_in_closing_window():
    class _Executor:
        is_live = True

        def get_best_bid(self, token_id):
            raise AssertionError("closing window must not fetch bid only for SL")

    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig()
    trader.executor = _Executor()
    trader.state = _DummyState()
    trader._last_tick = make_tick(elapsed_seconds=720, time_remaining_seconds=180)
    trader._tp_status_failures = {}
    trader.log = lambda message: None

    position = MicroPosition(
        direction=Direction.UP,
        token_id="token-1",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.40,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
    )
    trader.state.open_positions = [position]

    trader.check_tp_fills()

    assert trader.state.open_positions == [position]


def test_dry_run_stop_loss_check_skips_in_closing_window():
    class _Executor:
        is_live = False

        def get_best_bid(self, token_id):
            return 0.39

        def get_best_ask(self, token_id):
            return 0.41

    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig()
    trader.executor = _Executor()
    trader.state = _DummyState()
    trader._last_tick = make_tick(elapsed_seconds=720, time_remaining_seconds=180)
    trader.close_position_stop_loss = (
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("closing window must not close via SL")
        )
    )
    trader.log = lambda message: None

    position = MicroPosition(
        direction=Direction.UP,
        token_id="token-1",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.40,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
    )
    trader.state.open_positions = [position]

    trader.check_tp_fills()

    assert trader.state.open_positions == [position]


def test_pending_sl_order_is_cancelled_in_closing_window():
    class _Executor:
        is_live = True

        def __init__(self):
            self.cancelled = []

        def get_order_status(self, order_id):
            return {"status": "live"}

        def cancel_order(self, order_id):
            self.cancelled.append(order_id)
            return True

        def get_best_bid(self, token_id):
            raise AssertionError("pending SL should be handled before bid checks")

    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig()
    trader.executor = _Executor()
    trader.state = _DummyState()
    trader._last_tick = make_tick(elapsed_seconds=720, time_remaining_seconds=180)
    trader._tp_status_failures = {}
    trader.log = lambda message: None

    position = MicroPosition(
        direction=Direction.UP,
        token_id="token-1",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.40,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
        pending_sl_order_id="sl-1",
    )
    trader.state.open_positions = [position]

    trader.check_tp_fills()

    assert trader.executor.cancelled == ["sl-1"]
    assert position.pending_sl_order_id is None
    assert trader.state.save_calls == 1


def test_matched_pending_sl_order_is_recorded_in_closing_window():
    class _Executor:
        is_live = True

        def get_order_status(self, order_id):
            return {"status": "matched", "price": 0.38, "size_matched": 10.0}

        def cancel_order(self, order_id):
            raise AssertionError("matched SL should be recorded, not cancelled")

        def get_best_bid(self, token_id):
            raise AssertionError("matched SL should be handled before bid checks")

    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig()
    trader.executor = _Executor()
    trader.state = _DummyState()
    trader._last_tick = make_tick(elapsed_seconds=720, time_remaining_seconds=180)
    trader._tp_status_failures = {}
    trader.log = lambda message: None
    recorded = []

    def _record_exit(position, exit_price, result, **kwargs):
        recorded.append((position.id, exit_price, result, kwargs.get("proceeds_override")))

    trader._record_exit = _record_exit

    position = MicroPosition(
        direction=Direction.UP,
        token_id="token-1",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.40,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
        pending_sl_order_id="sl-1",
    )
    trader.state.open_positions = [position]

    trader.check_tp_fills()

    assert position.pending_sl_order_id is None
    assert recorded == [(position.id, 0.38, TradeResult.STOP_LOSS, 3.8)]


def test_backup_tp_cancel_failure_blocks_replacement():
    class _Executor:
        is_live = True

        def __init__(self):
            self.sell_limit_calls = []

        def cancel_order(self, order_id):
            return False

        def get_order_status(self, order_id):
            return {"status": "live"}

        def get_open_orders(self):
            return []

        def get_position(self, token_id, strict=False):
            raise AssertionError("balance should not be read after unconfirmed cancel")

        def sell_limit(self, token_id, price, shares):
            self.sell_limit_calls.append((token_id, price, shares))
            return OrderResult(success=True, order_id="new-tp")

    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    trader.executor = _Executor()
    trader.state = _DummyState()
    trader._tp_backup_failures = {}

    position = MicroPosition(
        direction=Direction.UP,
        token_id="token-1",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.0,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
        tp_order_id="old-tp",
    )

    trader._place_backup_tp_order(position, reason="test")

    assert trader.executor.sell_limit_calls == []
    assert position.tp_order_id == "old-tp"
    assert trader.state.risk_events[0][0] == "backup_tp_cancel_unconfirmed"


def test_backup_tp_strict_balance_failure_blocks_replacement():
    class _Executor:
        is_live = True

        def __init__(self):
            self.sell_limit_calls = []

        def cancel_order(self, order_id):
            return True

        def get_open_orders(self):
            return []

        def get_position(self, token_id, strict=False):
            assert strict is True
            raise RuntimeError("balance unavailable")

        def sell_limit(self, token_id, price, shares):
            self.sell_limit_calls.append((token_id, price, shares))
            return OrderResult(success=True, order_id="new-tp")

    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    trader.executor = _Executor()
    trader.state = _DummyState()
    trader._tp_backup_failures = {}

    position = MicroPosition(
        direction=Direction.UP,
        token_id="token-1",
        entry_price=0.50,
        shares=10.0,
        cost_basis=5.0,
        tp_price=0.60,
        stop_loss_price=0.0,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
        tp_order_id="old-tp",
    )

    trader._place_backup_tp_order(position, reason="test")

    assert trader.executor.sell_limit_calls == []
    assert trader.state.risk_events[0][0] == "backup_tp_balance_unknown"


def test_sell_limit_rejects_rounded_size_below_minimum(monkeypatch):
    _install_fake_clob_types(monkeypatch)
    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _FakeClient({"success": True, "orderID": "sell-too-small"})

    result = executor.sell_limit("token-1", 0.72, 1.0)

    assert result.success is False
    assert result.metadata["final_reason"] == "sell_size_below_minimum"


def test_cancel_all_orders_fails_on_not_canceled_response():
    class _Client:
        def cancel_all(self):
            return {"canceled": ["ok"], "not_canceled": {"bad": "live"}}

    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _Client()

    assert executor.cancel_all_orders() is False


def test_cancel_all_orders_detailed_reports_not_canceled_response():
    class _Client:
        def cancel_all(self):
            return {"canceled": ["ok"], "not_canceled": {"bad": "live"}}

    executor = LiveExecutor()
    executor._initialized = True
    executor._client = _Client()

    result = executor.cancel_all_orders_detailed()

    assert result.success is False
    assert result.canceled == ["ok"]
    assert result.not_canceled == {"bad": "live"}
    assert result.error == "orders_not_canceled"


# ----------------------------------------------------------------------------
# P1: live FOK target_shares sized off executable price (not max_entry_price)
# ----------------------------------------------------------------------------


def _make_entry_executor(captured: list, *, is_live: bool = True):
    """Stub executor that records buy_limit calls and reports a successful fill."""

    class _Executor:
        def __init__(self):
            self.is_live = is_live

        def get_usdc_balance(self):
            return None

        def buy_limit(self, token_id, price, shares, order_type="GTC",
                      wait_for_fill_seconds=None):
            captured.append({
                "token_id": token_id,
                "price": price,
                "shares": shares,
                "order_type": order_type,
                "wait_for_fill_seconds": wait_for_fill_seconds,
            })
            # Return failure so execute_entry returns None without entering
            # the post-fill TP/state path; we only care about what the
            # executor was called with.
            return OrderResult(
                success=False,
                order_id=None,
                error_message="captured for test",
                submission_state="rejected",
            )

    return _Executor()


def test_live_buy_limit_sizes_target_shares_from_executable_price():
    """target_shares = order_size / executable_price, NOT order_size /
    max_entry_price. With order_size=$6.00, max_entry_price=0.95,
    polymarket_prob=0.50, max_slippage_estimate_pct=0.02, the executor
    should receive 12 shares (6.00/0.50) at a tightened limit_price of
    0.51 (= ceil(0.50 × 1.02 × 100) / 100), NOT the loose 0.95 cap."""
    captured: list = []
    trader = SingleTrader.__new__(SingleTrader)
    # Disable upstream gates to isolate the buy_limit sizing path.
    trader.config = TraderConfig(
        paper_mode=False,
        max_slippage_estimate_pct=0.02,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
        max_entry_price=0.95,
        order_size_pct=0.06,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
        live_entry_order_retries=0,
    )
    trader.executor = _make_entry_executor(captured)
    state = _DummyState()
    state.current_capital = 100.0  # 6% × 100 = $6.00 order_size
    trader.state = state
    trader.log = lambda message: None
    # Bypass _check_concurrency_gate which inspects state.position
    trader._check_concurrency_gate = lambda: (True, "")
    # Bypass _estimate_slippage_from_book (no order_book_client on this stub
    # trader). Returns (slip_pct, depth_usd, best_ask) — within the configured
    # 2% slip cap so the gate passes.
    trader._estimate_slippage_from_book = lambda *_a, **_k: (0.0, 1000.0, 0.50)
    # Bypass the recheck (book-walk) — exercising the post-recheck call site.
    trader._simulate_live_entry_recheck = lambda *_a, **_k: 0.50

    tick = make_tick()
    opportunity = {
        "direction": "UP",
        "polymarket_prob": 0.50,
        "tick": tick,
        "token_id": "token-1",
    }

    trader.execute_entry(opportunity)

    assert len(captured) == 1, f"expected 1 buy_limit call, got {len(captured)}"
    call = captured[0]
    # Atomic notional cap: limit_price = ceil(fill × (1+slip)) tick-rounded,
    # bounded above by max_entry_price.
    assert call["price"] == 0.51, (
        f"limit_price should atomically cap at ceil(0.50 × 1.02) = 0.51, "
        f"not the loose 0.95 cap; got price={call['price']}"
    )
    # 6.00 / 0.50 = 12.0 (NOT 6.00 / 0.95 ≈ 6.32)
    assert abs(call["shares"] - 12.0) < 1e-6, (
        f"target_shares should be sized off executable 0.50, not cap 0.95; "
        f"got shares={call['shares']:.4f}"
    )


def test_live_buy_limit_rounds_target_shares_up_when_affordable():
    captured: list = []
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(
        paper_mode=False,
        max_slippage_estimate_pct=0.0,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
        max_entry_price=0.95,
        order_size_pct=0.0575,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
        live_entry_order_retries=0,
    )
    trader.executor = _make_entry_executor(captured)
    state = _DummyState()
    state.current_capital = 100.0
    trader.state = state
    trader.log = lambda message: None
    trader._check_concurrency_gate = lambda: (True, "")
    trader._simulate_live_entry_recheck = lambda *_a, **_k: 0.95

    trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.95,
        "tick": make_tick(),
        "token_id": "token-1",
    })

    assert len(captured) == 1
    assert captured[0]["price"] == 0.95
    assert captured[0]["shares"] == 6.06


def test_live_buy_limit_respects_affordable_share_cap():
    captured: list = []
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(
        paper_mode=False,
        max_slippage_estimate_pct=0.0,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
        max_entry_price=0.95,
        order_size_pct=0.10,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
        live_entry_order_retries=0,
    )
    trader.executor = _make_entry_executor(captured)
    state = _DummyState()
    state.current_capital = 5.75
    trader.state = state
    trader.log = lambda message: None
    trader._check_concurrency_gate = lambda: (True, "")
    trader._simulate_live_entry_recheck = lambda *_a, **_k: 0.95

    trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.95,
        "tick": make_tick(),
        "token_id": "token-1",
    })

    assert len(captured) == 1
    assert captured[0]["shares"] == 6.05


def test_live_entry_retries_failed_order_without_order_id():
    captured: list = []
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(
        paper_mode=False,
        max_slippage_estimate_pct=0.02,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
        max_entry_price=0.95,
        order_size_pct=0.06,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
        live_entry_order_retries=2,
        live_entry_retry_window_seconds=60.0,
        live_entry_retry_delay_seconds=0.0,
    )
    trader.executor = _make_entry_executor(captured)
    state = _DummyState()
    state.current_capital = 100.0
    trader.state = state
    trader.log = lambda message: None
    trader._check_concurrency_gate = lambda: (True, "")
    trader._estimate_slippage_from_book = lambda *_a, **_k: (0.0, 1000.0, 0.50)
    trader._simulate_live_entry_recheck = lambda *_a, **_k: 0.50

    trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.50,
        "tick": make_tick(),
        "token_id": "token-1",
    })

    assert len(captured) == 3


def test_live_shadow_retries_failed_order_without_order_id():
    captured: list = []
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(
        paper_mode=True,
        live_shadow_mode=True,
        max_slippage_estimate_pct=0.02,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
        max_entry_price=0.95,
        order_size_pct=0.06,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
        live_entry_order_retries=2,
        live_entry_retry_window_seconds=60.0,
        live_entry_retry_delay_seconds=0.0,
    )
    trader.executor = _make_entry_executor(captured, is_live=False)
    state = _DummyState()
    state.current_capital = 100.0
    trader.state = state
    trader.log = lambda message: None
    trader._check_concurrency_gate = lambda: (True, "")
    trader._estimate_slippage_from_book = lambda *_a, **_k: (0.0, 1000.0, 0.50)
    trader._simulate_live_entry_recheck = lambda *_a, **_k: 0.50

    trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.50,
        "tick": make_tick(),
        "token_id": "token-1",
    })

    assert len(captured) == 3


def test_live_entry_retry_stops_when_entry_price_moves_too_far():
    captured: list = []
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(
        paper_mode=False,
        max_slippage_estimate_pct=0.02,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
        max_entry_price=0.95,
        order_size_pct=0.06,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
        live_entry_spot_recheck_enabled=False,
        live_entry_order_retries=2,
        live_entry_retry_window_seconds=60.0,
        live_entry_retry_delay_seconds=0.0,
        live_entry_retry_price_guard=0.02,
    )
    trader.executor = _make_entry_executor(captured)
    state = _DummyState()
    state.current_capital = 100.0
    trader.state = state
    trader.log = lambda message: None
    trader._check_concurrency_gate = lambda: (True, "")
    trader._estimate_slippage_from_book = lambda *_a, **_k: (0.0, 1000.0, 0.50)
    trader.order_book_client = object()
    recheck_prices = iter([0.50, 0.53])
    trader._simulate_live_entry_recheck = lambda *_a, **_k: next(recheck_prices)

    trader.execute_entry({
        "direction": "UP",
        "polymarket_prob": 0.50,
        "tick": make_tick(),
        "token_id": "token-1",
    })

    assert len(captured) == 1
    assert any(kind == "entry_retry_rejected_price_move" for kind, _ in state.risk_events)


def test_live_buy_limit_clamps_zero_or_negative_fill_price():
    """Degenerate polymarket_prob=0 falls back to limit_price; no DivisionByZero."""
    captured: list = []
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(
        paper_mode=False,
        max_slippage_estimate_pct=0.0,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
        max_entry_price=0.95,
        order_size_pct=0.06,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
        live_entry_order_retries=0,
    )
    trader.executor = _make_entry_executor(captured)
    state = _DummyState()
    state.current_capital = 100.0
    trader.state = state
    trader.log = lambda message: None
    trader._check_concurrency_gate = lambda: (True, "")
    trader._simulate_live_entry_recheck = lambda *_a, **_k: 0.0

    tick = make_tick()
    opportunity = {
        "direction": "UP",
        "polymarket_prob": 0.0,  # degenerate
        "tick": tick,
        "token_id": "token-1",
    }

    trader.execute_entry(opportunity)

    assert len(captured) == 1
    # With polymarket_prob=0 falsy, fall back to cap_price=0.95
    # Round 6.00 / 0.95 up to the venue 0.01-share precision.
    assert captured[0]["shares"] == 6.32
    # slip_budget=0 + fill=0.95 → limit_price = min(0.95, ceil(0.95)) = 0.95
    assert captured[0]["price"] == 0.95


def test_live_buy_limit_caps_limit_price_at_fill_plus_slip_budget():
    """Atomic notional cap: limit_price = ceil(fill × (1+slip)) per penny tick,
    bounded above by max_entry_price. With fill=0.50, slip=0.02 → 0.51 (not
    0.95). The FOK rejects if the book moves beyond the slip budget between
    recheck and venue receipt — bounding worst-case notional to
    order_size × (1 + slip + 1tick/fill_price) ≈ 1.04× rather than ~1.9×."""
    captured: list = []
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(
        paper_mode=False,
        max_slippage_estimate_pct=0.02,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
        max_entry_price=0.95,
        order_size_pct=0.06,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
        live_entry_order_retries=0,
    )
    trader.executor = _make_entry_executor(captured)
    state = _DummyState()
    state.current_capital = 100.0
    trader.state = state
    trader.log = lambda message: None
    trader._check_concurrency_gate = lambda: (True, "")
    trader._estimate_slippage_from_book = lambda *_a, **_k: (0.0, 1000.0, 0.50)
    trader._simulate_live_entry_recheck = lambda *_a, **_k: 0.50

    tick = make_tick()
    opportunity = {
        "direction": "UP",
        "polymarket_prob": 0.50,
        "tick": tick,
        "token_id": "token-1",
    }

    trader.execute_entry(opportunity)

    assert len(captured) == 1
    # ceil(0.50 × 1.02 × 100) / 100 = ceil(51) / 100 = 0.51
    assert captured[0]["price"] == 0.51
    assert abs(captured[0]["shares"] - 12.0) < 1e-6


def test_live_buy_limit_keeps_safety_cap_when_fill_plus_slip_exceeds_cap():
    """When fill × (1+slip) would exceed max_entry_price, the cap wins.
    fill=0.93, slip=0.05 → 0.93×1.05 = 0.9765 > 0.95 → limit_price clamps
    to 0.95 (the absolute safety ceiling)."""
    captured: list = []
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(
        paper_mode=False,
        max_slippage_estimate_pct=0.05,
        min_liquidity_depth_usd=0.0,
        require_tp_confirmed_before_new_entry=False,
        max_entry_price=0.95,
        order_size_pct=0.06,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
        live_entry_order_retries=0,
    )
    trader.executor = _make_entry_executor(captured)
    state = _DummyState()
    state.current_capital = 100.0
    trader.state = state
    trader.log = lambda message: None
    trader._check_concurrency_gate = lambda: (True, "")
    trader._estimate_slippage_from_book = lambda *_a, **_k: (0.0, 1000.0, 0.93)
    trader._simulate_live_entry_recheck = lambda *_a, **_k: 0.93

    tick = make_tick()
    opportunity = {
        "direction": "UP",
        "polymarket_prob": 0.93,
        "tick": tick,
        "token_id": "token-1",
    }

    trader.execute_entry(opportunity)

    assert len(captured) == 1
    # min(0.95, ceil(0.9765 × 100)/100) = min(0.95, 0.98) = 0.95
    assert captured[0]["price"] == 0.95
    # Round 6.00 / 0.93 up to the venue 0.01-share precision.
    assert captured[0]["shares"] == 6.46


# ----------------------------------------------------------------------------
# P2(a): top-of-book depth gate no longer rejects when recheck handles it
# ----------------------------------------------------------------------------


def test_live_entry_no_longer_rejects_on_thin_top_of_book():
    """With min_liquidity_depth_usd=$50 and top-of-book depth=$10, an order
    of $5.75 used to be rejected on the flat depth gate. The recheck now
    walks the book and is the authoritative liquidity gate; thin
    top-of-book no longer blocks if the recheck succeeds."""
    captured: list = []
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(
        paper_mode=False,
        max_slippage_estimate_pct=0.0,
        min_liquidity_depth_usd=50.0,  # would have rejected $10 top depth
        live_recheck_delay_seconds=0.0,  # keep min_depth in scope for the test
        require_tp_confirmed_before_new_entry=False,
        max_entry_price=0.95,
        order_size_pct=0.06,
        min_order_size_usd=5.75,
        min_position_shares=5.75,
        live_entry_order_retries=0,
    )
    trader.executor = _make_entry_executor(captured)
    state = _DummyState()
    state.current_capital = 100.0
    trader.state = state
    trader.log = lambda message: None
    trader._check_concurrency_gate = lambda: (True, "")
    # Stub the slippage estimator: top-of-book depth is only $10.
    trader._estimate_slippage_from_book = lambda *_a, **_k: (0.0, 10.0, 0.50)
    # Recheck succeeds (simulates the book-walk finding fill at lower levels).
    trader._simulate_live_entry_recheck = lambda *_a, **_k: 0.50

    tick = make_tick()
    opportunity = {
        "direction": "UP",
        "polymarket_prob": 0.50,
        "tick": tick,
        "token_id": "token-1",
    }

    trader.execute_entry(opportunity)

    # Pre-fix this would have logged entry_rejected_liquidity. Now the
    # recheck is the only gate, so the buy_limit call IS made.
    assert len(captured) == 1
    rejected = [k for (k, _) in state.risk_events if k == "entry_rejected_liquidity"]
    assert rejected == [], f"depth gate fired unexpectedly: {state.risk_events}"


# ----------------------------------------------------------------------------
# P2(b): settlement guard refuses when on-chain balance < local shares
# ----------------------------------------------------------------------------


def _make_settlement_executor(
    onchain_shares: float,
    *,
    statuses: list[dict | None] | None = None,
    cancel_result: CancelResult | None = None,
):
    """Stub executor for the resolution-readiness check."""

    class _Executor:
        is_live = True

        def __init__(self):
            self.statuses = list(statuses or [])
            self.position_reads = 0
            self.cancel_calls: list[str] = []

        def get_order_status(self, order_id):
            if self.statuses:
                return self.statuses.pop(0)
            return None

        def cancel_order_detailed(self, order_id, retries=3):
            self.cancel_calls.append(order_id)
            if cancel_result is not None:
                return cancel_result
            return CancelResult(success=True)

        def cancel_order(self, order_id):
            return self.cancel_order_detailed(order_id).success

        def get_position(self, token_id, strict=False):
            self.position_reads += 1
            return onchain_shares

    return _Executor()


def _make_settlement_position(local_shares: float = 10.0):
    return MicroPosition(
        direction=Direction.UP,
        token_id="token-1",
        entry_price=0.50,
        shares=local_shares,
        cost_basis=local_shares * 0.50,
        tp_price=0.60,
        stop_loss_price=0.40,
        opened_at=datetime.now(timezone.utc),
        market_id="btc-updown-15m-test",
        peak_price=0.50,
        tp_order_id=None,
        trail_order_id=None,
    )


def test_settlement_blocked_when_onchain_balance_below_local():
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    trader.executor = _make_settlement_executor(onchain_shares=0.0)
    state = _DummyState()
    trader.state = state
    trader.log = lambda message: None

    position = _make_settlement_position(local_shares=10.0)

    out = trader._confirm_live_orders_inactive_for_resolution(position)

    assert out is False
    kinds = [kind for kind, _ in state.risk_events]
    assert "settlement_blocked_balance_mismatch" in kinds, (
        f"expected settlement_blocked_balance_mismatch in risk events; got {kinds}"
    )


def test_settlement_proceeds_when_onchain_within_tolerance():
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    trader.executor = _make_settlement_executor(onchain_shares=9.999)
    state = _DummyState()
    trader.state = state
    trader.log = lambda message: None

    position = _make_settlement_position(local_shares=10.0)

    # Within 0.5 absolute floor (and within 5% relative).
    out = trader._confirm_live_orders_inactive_for_resolution(position)
    assert out is True


def test_settlement_proceeds_when_onchain_exceeds_local():
    """Asymmetric tolerance: on-chain ≥ local should proceed since
    booking local_shares × payout doesn't inflate P&L."""
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    trader.executor = _make_settlement_executor(onchain_shares=15.0)
    state = _DummyState()
    trader.state = state
    trader.log = lambda message: None

    position = _make_settlement_position(local_shares=10.0)

    out = trader._confirm_live_orders_inactive_for_resolution(position)
    assert out is True


def test_settlement_proceeds_when_tp_order_terminally_absent_and_shares_held():
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    terminal_absent = CancelResult(
        success=False,
        terminal_absent=True,
        reason="order can't be found - already canceled or matched",
    )
    executor = _make_settlement_executor(
        onchain_shares=10.0,
        statuses=[None, None],
        cancel_result=terminal_absent,
    )
    trader.executor = executor
    trader.state = _DummyState()
    trader.log = lambda message: None

    position = _make_settlement_position(local_shares=10.0)
    position.tp_order_id = "tp-missing"

    out = trader._confirm_live_orders_inactive_for_resolution(position)

    assert out is True
    assert executor.cancel_calls == ["tp-missing"]
    assert executor.position_reads == 1


def test_settlement_blocks_terminally_absent_tp_when_wallet_shares_are_short():
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    terminal_absent = CancelResult(
        success=False,
        terminal_absent=True,
        reason="order can't be found - already canceled or matched",
    )
    executor = _make_settlement_executor(
        onchain_shares=0.0,
        statuses=[None, None],
        cancel_result=terminal_absent,
    )
    trader.executor = executor
    state = _DummyState()
    trader.state = state
    trader.log = lambda message: None

    position = _make_settlement_position(local_shares=10.0)
    position.tp_order_id = "tp-missing"

    out = trader._confirm_live_orders_inactive_for_resolution(position)

    assert out is False
    assert executor.position_reads == 1
    assert [kind for kind, _ in state.risk_events] == [
        "settlement_blocked_balance_mismatch"
    ]


def test_settlement_blocks_retryable_active_order_cancel_failure():
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    retryable = CancelResult(success=False, retryable=True, reason="network unavailable")
    executor = _make_settlement_executor(
        onchain_shares=10.0,
        statuses=[{"status": "live"}],
        cancel_result=retryable,
    )
    trader.executor = executor
    trader.state = _DummyState()
    trader.log = lambda message: None

    position = _make_settlement_position(local_shares=10.0)
    position.tp_order_id = "tp-live"

    out = trader._confirm_live_orders_inactive_for_resolution(position)

    assert out is False
    assert executor.cancel_calls == ["tp-live"]
    assert executor.position_reads == 0


def test_settlement_proceeds_after_successful_cancel_with_unknown_post_status():
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    executor = _make_settlement_executor(
        onchain_shares=10.0,
        statuses=[{"status": "live"}, None],
        cancel_result=CancelResult(success=True),
    )
    trader.executor = executor
    trader.state = _DummyState()
    trader.log = lambda message: None

    position = _make_settlement_position(local_shares=10.0)
    position.tp_order_id = "tp-cancelled"

    out = trader._confirm_live_orders_inactive_for_resolution(position)

    assert out is True
    assert executor.cancel_calls == ["tp-cancelled"]
    assert executor.position_reads == 1


def test_settlement_filled_tp_status_closes_tp_before_expiry():
    trader = SingleTrader.__new__(SingleTrader)
    trader.config = TraderConfig(paper_mode=False)
    executor = _make_settlement_executor(
        onchain_shares=10.0,
        statuses=[{"status": "filled", "price": 0.62}],
    )
    trader.executor = executor
    trader.state = _DummyState()
    trader.log = lambda message: None
    close_calls = []
    trader.close_position_tp = (
        lambda position, exit_price: close_calls.append((position.id, exit_price))
    )
    trader._close_trail_filled = (
        lambda position, exit_price: (_ for _ in ()).throw(
            AssertionError("trail close should not be called")
        )
    )

    position = _make_settlement_position(local_shares=10.0)
    position.tp_order_id = "tp-filled"

    out = trader._confirm_live_orders_inactive_for_resolution(position)

    assert out is False
    assert close_calls == [(position.id, 0.62)]
    assert executor.cancel_calls == []
    assert executor.position_reads == 0
