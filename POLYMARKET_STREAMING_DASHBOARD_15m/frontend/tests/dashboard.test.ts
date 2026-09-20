import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";
import { useDash } from "../src/lib/store";
import { WsManager } from "../src/lib/ws";
import type { BootstrapPayload, TradeEvent, WsEnvelope } from "../src/lib/types";

const at = "2026-09-05T10:00:00Z";
const old = "2026-08-01T10:00:00Z";
const instance = (capital: number) => ({
  instance_id: 1271, capital, starting_capital: 100, total_pnl: -1,
  total_pnl_pct: -1, pnl_mean_to_sd: null, max_drawdown: 1,
  max_drawdown_pct: 1, wins: 0, losses: 1, win_rate: 0, trades_count: 1,
});
const event = (name: string, source: "live" | "dry_run" = "live"): TradeEvent => ({
  instance_id: 1271, run_source: source, timestamp: at, event: name,
  direction: "UP", entry_price: .5, shares: 10, pnl: 7,
});
function receive(type: string, data: unknown) {
  useDash.getState().applyEnvelope({ type, data, id: "test", server_time: at } as WsEnvelope);
}

beforeEach(() => useDash.setState(useDash.getInitialState(), true));

test("live close never synthesizes account equity from P&L; observed update replaces it", () => {
  useDash.setState({ mode: "live", instance: instance(119), equity: [100, 99], equitySeries: [{ t: at, v: 119 }] });
  receive("trade.append", event("TP_FILLED"));
  assert.deepEqual(useDash.getState().equitySeries, [{ t: at, v: 119 }]);
  receive("instance.update", { source: "live", instance: instance(120), position: { open: null }, equity: [100, 99], equity_series: [{ t: at, v: 120 }] });
  assert.equal(useDash.getState().equitySeries[0].v, 120);
  assert.equal(useDash.getState().instance!.total_pnl, -1);
});

test("correction uses full backend history and current position, not replayed recent closes", () => {
  useDash.setState({ mode: "live", equitySeries: [{ t: old, v: 999 }], trades: [event("TP_FILLED")], markers: [{ t: at, kind: "WIN" }] });
  const position = { open: { direction: "DOWN", entry_price: .6, shares: 8, tp_target: .8, sl_target: .4, entered_at: at, market_id: null, notional: 4.8 } };
  receive("trades.reset", {
    source: "live", events: [event("VOIDED_PHANTOM_TP")],
    snapshot: { source: "live", instance: instance(119), position, equity: [100, 99], equity_series: [{ t: old, v: 100 }, { t: at, v: 119 }] },
  });
  assert.deepEqual(useDash.getState().equitySeries, [{ t: old, v: 100 }, { t: at, v: 119 }]);
  assert.deepEqual(useDash.getState().position, position);
  assert.equal(useDash.getState().trades[0].event, "VOIDED_PHANTOM_TP");
  assert.deepEqual(useDash.getState().markers, []);
});

test("opposite-source state and correction events cannot overwrite the selected account", () => {
  useDash.setState({ mode: "live", instance: instance(119) });
  receive("instance.update", { source: "dry_run", instance: instance(500), position: { open: null }, equity: [500] });
  receive("trades.reset", { source: "dry_run", events: [event("TP_FILLED", "dry_run")] });
  receive("trade.append", event("TP_FILLED", "dry_run"));
  assert.equal(useDash.getState().instance!.capital, 119);
  assert.deepEqual(useDash.getState().trades, []);
});

test("WebSocket recovery reselects the view and accepts only its fresh bootstrap", (context) => {
  const sockets: FakeSocket[] = [];
  let reconnect: (() => void) | undefined;
  class FakeSocket {
    static OPEN = 1;
    readyState = 1;
    sent: string[] = [];
    onopen?: () => void;
    onclose?: () => void;
    onmessage?: (ev: { data: string }) => void;
    constructor(_url: string) { sockets.push(this); }
    send(text: string) { this.sent.push(text); }
    close() { this.onclose?.(); }
  }
  context.mock.method(globalThis, "setTimeout", ((fn: () => void) => { reconnect = fn; return 0; }) as typeof setTimeout);
  const previous = globalThis.WebSocket;
  Object.defineProperty(globalThis, "WebSocket", { value: FakeSocket, configurable: true, writable: true });
  const manager = new WsManager("ws://example/ws");
  const received: string[] = [];
  manager.subscribe((env) => { if (env.type === "bootstrap") received.push(env.data.mode); });
  try {
    manager.selectView(1271, "dry_run");
    manager.connect();
    sockets[0].onopen!();
    sockets[0].onclose!();
    reconnect!();
    sockets[1].onopen!();
    assert.deepEqual(JSON.parse(sockets[1].sent[0]), { action: "select_view", instance_id: 1271, source: "dry_run" });
    for (const mode of ["live", "dry_run"]) {
      sockets[1].onmessage!({ data: JSON.stringify({ type: "bootstrap", data: { mode, instance: instance(119) } }) });
    }
    assert.deepEqual(received, ["dry_run"]);
  } finally {
    manager.close();
    Object.defineProperty(globalThis, "WebSocket", { value: previous, configurable: true, writable: true });
  }
});
