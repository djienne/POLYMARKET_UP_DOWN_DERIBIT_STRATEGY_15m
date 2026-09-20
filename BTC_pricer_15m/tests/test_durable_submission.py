"""Exercise signed submission, durable state and restart recovery without networking."""
from datetime import datetime, timedelta, timezone
from dataclasses import replace
from decimal import Decimal
import json

import pytest
from requests.exceptions import ReadTimeout

from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions, Side
from py_clob_client_v2.config import get_contract_config
from py_clob_client_v2.order_utils import ExchangeOrderBuilderV2
from py_clob_client_v2.order_utils.model.order_data_v2 import OrderDataV2
from py_clob_client_v2.signer import Signer

from scripts.trader.executor_live import LiveExecutor
from scripts.trader.executor import CancelResult, OrderResult
from scripts.trader.position import TradeResult
from scripts.trader.state import StateManager
from tests.conftest import make_tick
from tests.test_live_trader_safety import _trader, _LiveExec

# Public fixture key; never a user wallet. The transport below cannot send HTTP.
FIXTURE_KEY = "0x" + "01" * 32


class SignedTransport:
    def __init__(self, state_path):
        self.signer = Signer(FIXTURE_KEY, 137)
        self.state_path = state_path
        self.posts = []
        self.balance = 0.0
        self.status = None
        self.salt = 12345

    def get_tick_size(self, token_id):
        return "0.01"

    def get_neg_risk(self, token_id):
        return False

    def create_order(self, args, options=None):
        builder = ExchangeOrderBuilderV2(
            get_contract_config(137).exchange_v2, 137, self.signer,
            generate_salt=lambda: self.salt,
        )
        q, p = Decimal(str(args.size)), Decimal(str(args.price))
        return builder.build_signed_order(OrderDataV2(
            maker=self.signer.address(), signer=self.signer.address(),
            tokenId=args.token_id, makerAmount=str(int(q * p * 1_000_000)),
            takerAmount=str(int(q * 1_000_000)), side=0, signatureType=0,
            timestamp="1788595200000",
        ))

    def post_order(self, signed, order_type):
        # The durable intent must already be readable before the venue receives
        # anything. Read the actual exported state, not a captured callback arg.
        pending = json.loads(self.state_path.read_text())["pending_entry"]
        assert pending["submission_state"] == "submitting"
        assert pending["submitted_order_hash"].startswith("0x")
        assert "signature" not in json.dumps(pending)
        self.posts.append((signed, pending["order_id"]))
        raise ReadTimeout("fixture venue accepted order, but response was lost")


def make_trader(tmp_path):
    executor = LiveExecutor(private_key=FIXTURE_KEY)
    trader = _trader(
        tmp_path, executor=executor, live_entry_order_retries=5,
        live_entry_retry_delay_seconds=0, live_entry_retry_window_seconds=30,
        clob_health_gate_enabled=False, availability_ledger_enabled=False,
        persistence_backend="sqlite", live_sqlite_db_file=tmp_path / "live.sqlite",
    )
    transport = SignedTransport(trader.config.live_state_file)
    executor._client = transport
    executor._initialized = True
    executor._config = trader.config
    executor.get_usdc_balance = lambda: 100.0
    executor.get_position = lambda *a, **k: transport.balance
    executor.get_order_status = lambda order_id: transport.status
    executor.get_trades = lambda *a, **k: []
    executor.get_open_orders = lambda **k: []
    trader._record_clob_order_outcome = lambda result: None
    tick = make_tick(prob_above=0.99, prob_below=0.01, poly_prob_up=0.55)
    trader.state.current_market = {
        "market_id": tick.market_id, "title": "Fixture market",
        "reference_price": tick.barrier,
    }
    opportunity = {"direction": "UP", "token_id": "123", "model_prob": 0.99,
                   "polymarket_prob": 0.55, "tick": tick}
    return trader, transport, opportunity


def test_accepted_timeout_is_one_submission_and_survives_restart(tmp_path):
    trader, venue, opportunity = make_trader(tmp_path)
    result = trader.execute_entry(opportunity)
    assert result.ambiguous
    assert len(venue.posts) == 1
    assert trader.state.pending_entry["submitted_order_hash"] == venue.posts[0][1]
    assert trader.state.open_positions == []

    restored = StateManager(trader.config)
    assert restored.load()
    trader.state = restored
    # Unknown order status plus zero current balance does NOT prove no order.
    trader._reconcile_pending_entry()
    assert trader.state.pending_entry is not None
    again = trader.execute_entry(opportunity)
    assert again.rejection_reason == "pending_entry_unresolved"
    assert len(venue.posts) == 1


def test_signed_order_identity_is_stable_and_changes_with_salt(tmp_path):
    trader, venue, _ = make_trader(tmp_path)
    options = PartialCreateOrderOptions(tick_size="0.01", neg_risk=False)
    args = OrderArgs(token_id="123", price=0.55, size=10.0, side=Side.BUY)
    signed = venue.create_order(args, options)
    order_id = trader.executor._signed_order_id(signed, options)
    # Independently recover the signer from the signed EIP-712 message. This
    # protects the actual SDK/signature boundary, which cancellation tests mock.
    from eth_account import Account
    from eth_account.messages import encode_typed_data
    builder = ExchangeOrderBuilderV2(get_contract_config(137).exchange_v2, 137, venue.signer)
    message = encode_typed_data(full_message=builder.build_order_typed_data(signed))
    from eth_utils import keccak
    assert order_id == "0x" + keccak(b"\x19" + message.version + message.header + message.body).hex()
    assert Account.recover_message(message, signature=signed.signature) == venue.signer.address()
    venue.salt += 1
    assert trader.executor._signed_order_id(venue.create_order(args, options), options) != order_id


def test_persistence_failure_prevents_any_post(tmp_path, monkeypatch):
    trader, venue, opportunity = make_trader(tmp_path)
    def failed_save():
        raise OSError("fixture disk unavailable")
    monkeypatch.setattr(trader.state, "save", failed_save)
    with pytest.raises(OSError, match="disk unavailable"):
        trader.execute_entry(opportunity)
    assert venue.posts == []


def test_pending_without_returned_order_id_blocks_new_exposure(tmp_path):
    trader, venue, opportunity = make_trader(tmp_path)
    trader.state.pending_entry = {
        "token_id": "123", "order_id": None, "reason": "legacy_unknown_submission",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    trader.state.save()
    trader._reconcile_pending_entry()
    assert trader.state.pending_entry is not None
    assert trader.execute_entry(opportunity).rejection_reason == "pending_entry_unresolved"
    assert venue.posts == []


def test_partial_timeout_is_not_adopted_until_residual_cancelled(tmp_path, monkeypatch):
    trader, venue, opportunity = make_trader(tmp_path)
    trader.execute_entry(opportunity)
    venue.balance = 6.0
    venue.status = {"status": "live", "size_matched": "6", "original_size": "10", "price": "0.55"}
    monkeypatch.setattr(trader.executor, "cancel_order_detailed", lambda _: CancelResult(success=False))
    trader._reconcile_pending_entry()
    assert trader.state.pending_entry is not None
    assert trader.state.open_positions == []

    def cancel(_):
        venue.status["status"] = "cancelled"
        return CancelResult(success=True)
    monkeypatch.setattr(trader.executor, "cancel_order_detailed", cancel)
    monkeypatch.setattr(trader, "_place_backup_tp_order", lambda *a, **k: None)
    trader.executor._post_cancel_reconcile_seconds = 0
    trader._reconcile_pending_entry()
    assert trader.state.pending_entry is None
    assert trader.state.open_positions[0].shares == 6.0
    assert trader.state.open_positions[0].cost_basis == pytest.approx(3.30)
    assert trader.state.last_entered_market_id == opportunity["tick"].market_id
    assert len(venue.posts) == 1


def test_stale_cancel_rechecks_late_fill_instead_of_discarding_it(tmp_path, monkeypatch):
    trader, venue, opportunity = make_trader(tmp_path)
    trader.execute_entry(opportunity)
    trader.config.pending_entry_max_age_seconds = 1
    trader.state.pending_entry["created_at"] = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    venue.status = {"status": "live", "size_matched": "0", "price": "0.55"}
    def cancel(_):
        venue.balance = 6.0
        venue.status = {"status": "cancelled", "size_matched": "6", "price": "0.55"}
        return True
    monkeypatch.setattr(trader.executor, "cancel_order", cancel)
    monkeypatch.setattr(trader, "_place_backup_tp_order", lambda *a, **k: None)
    trader.executor._post_cancel_reconcile_seconds = 0
    trader._reconcile_pending_entry()
    assert trader.state.pending_entry is None
    assert trader.state.open_positions[0].shares == 6.0
    assert len(venue.posts) == 1


@pytest.mark.parametrize("slow_stage", ["sign", "persist"])
@pytest.mark.parametrize("remaining, expected_posts", [(180.25, 0), (181.25, 1)])
def test_submission_window_rechecked_after_signing_and_persistence(tmp_path, monkeypatch, slow_stage, remaining, expected_posts):
    trader, venue, opportunity = make_trader(tmp_path)
    base = datetime.now(timezone.utc)
    elapsed = [0.0]
    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return base + timedelta(seconds=elapsed[0])
    monkeypatch.setattr("scripts.trader.single_trader.datetime", Clock)
    monkeypatch.setattr("scripts.trader.contracts.datetime", Clock)
    opportunity["tick"] = replace(
        opportunity["tick"], payoff_kind="chainlink_twap_60s",
        contract_supported=True, fee_schedule={"enabled": False},
        barrier_source="polymarket", barrier_confirmed=True,
        up_token_id="123", down_token_id="456",
        probability_evaluated_at=base, probability_spot_observed_at=base,
        market_end_time=base + timedelta(seconds=remaining),
        time_remaining_seconds=remaining, elapsed_seconds=900 - remaining,
    )
    original_sign = venue.create_order
    def slow_sign(*args, **kwargs):
        signed = original_sign(*args, **kwargs)
        elapsed[0] = 0.75  # Still fresh, but now inside the prohibited final180s.
        return signed
    if slow_stage == "sign":
        monkeypatch.setattr(venue, "create_order", slow_sign)
    else:
        original_save = trader.state.save
        def slow_save():
            original_save()
            elapsed[0] = 0.75
        monkeypatch.setattr(trader.state, "save", slow_save)
    result = trader.execute_entry(opportunity)
    assert not result.entered
    assert len(venue.posts) == expected_posts, result


def test_deployment_pause_blocks_only_new_entries(tmp_path):
    trader, venue, opportunity = make_trader(tmp_path)
    (tmp_path / ".entries_paused").write_text("deployment")
    assert trader.execute_entry(opportunity).rejection_reason == "entries_paused"
    assert venue.posts == []
    # Existing-position settlement remains callable and is not behind this gate.
    from tests.test_live_trader_safety import _position
    position = _position()
    trader.state.add_position(position)
    trader._get_settlement_service().settle_position_at_final(position, 82900, "fixture")
    assert not trader.state.open_positions
    assert trader.state.closed_positions[-1].result == TradeResult.LOSS_EXPIRY


def test_modern_small_fill_remains_a_position_until_authoritative_resolution(tmp_path, monkeypatch):
    executor = _LiveExec(OrderResult(success=True, order_id="small-filled", shares_filled=1,
                                   avg_fill_price=0.55, total_cost=0.55))
    executor.positions["123"] = 1.0
    trader = _trader(tmp_path, executor, clob_health_gate_enabled=False, availability_ledger_enabled=False)
    now = datetime.now(timezone.utc)
    contract = {"payoff_kind": "chainlink_twap_60s", "averaging_window_seconds": 60,
                "condition_id": "fixture-condition", "outcome_token_ids": {"UP": "123", "DOWN": "456"},
                "fee_schedule": {"enabled": False}, "supported": True}
    tick = replace(make_tick(prob_above=0.99, prob_below=0.01),
                   payoff_kind="chainlink_twap_60s", contract_supported=True,
                   fee_schedule={"enabled": False}, contract_metadata=contract,
                   up_token_id="123", down_token_id="456",
                   probability_evaluated_at=now, probability_spot_observed_at=now,
                   barrier_source="polymarket", barrier_confirmed=True,
                   market_end_time=now + timedelta(seconds=600))
    trader.state.current_market = {"market_id": tick.market_id, "reference_price": tick.barrier}
    monkeypatch.setattr(trader, "_record_clob_order_outcome", lambda result: None)
    attempt = trader.execute_entry({"direction": "UP", "token_id": "123", "tick": tick,
                                    "model_prob": 0.99, "polymarket_prob": 0.55})
    assert attempt.position is not None
    position = attempt.position
    assert position.shares == 1.0 and position.pending_settlement
    assert trader.state.pending_entry is None
    assert not position.tp_order_id
    service = trader._get_settlement_service()
    monkeypatch.setattr(service, "resolve_position_payout", lambda *args: (1.0, "fixture_resolved_payout"))
    assert service.attempt_settlement(position)
    assert not trader.state.open_positions
    trade = trader.state.closed_positions[-1]
    assert trade.proceeds == 1.0 and trade.pnl == pytest.approx(0.45)
    assert trade.final_btc_price is None


@pytest.mark.parametrize("requested_token, expected_posts", [("wrong-contract", 0), ("123", 1)])
def test_modern_order_token_is_bound_to_the_verified_tick(tmp_path, requested_token, expected_posts):
    trader, venue, opportunity = make_trader(tmp_path)
    now = datetime.now(timezone.utc)
    opportunity["token_id"] = requested_token
    opportunity["tick"] = replace(
        opportunity["tick"], payoff_kind="chainlink_twap_60s", contract_supported=True,
        fee_schedule={"enabled": False}, barrier_source="polymarket", barrier_confirmed=True,
        up_token_id="123", down_token_id="456", probability_evaluated_at=now,
        probability_spot_observed_at=now, market_end_time=now + timedelta(seconds=600),
    )
    result = trader.execute_entry(opportunity)
    assert len(venue.posts) == expected_posts
    if not expected_posts:
        assert result.rejection_reason == "token_contract_mismatch"
