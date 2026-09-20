# Dashboard guide

The bot trades Polymarket outcome tokens. The current BTC 15-minute contract
uses a Chainlink 60-second average at resolution. The model forecasts that
payoff approximately; a quoted model probability is not a guaranteed win rate.
Resolved Polymarket token payouts determine expiry accounting.

## Market and model

The probability panel distinguishes market-implied prices, available diagnostic
models and the probability selected by the trader. The selected source label
identifies the 60-second-average approximation. Terminal B-L diagnostics and the
average-payoff forecast are different quantities, so disagreement is informative.
`n/a` means no usable estimate is available.

The edge panel compares model probability with the executable price and required
threshold. A favorable edge alone does not authorize an order: contract and fee
metadata, timestamps, market window, liquidity and live execution checks must also
pass. Live entry uses a bounded limit with a short wait and residual cancellation;
an uncertain order remains under reconciliation.

The price chart shows UP/DOWN quotes and model estimates. The market card shows
BTC spot, the contract barrier and time left. The configured first 300 seconds and
final 180 seconds block new entries; they do not prohibit management of existing
positions or pending settlement.

## Position and account

The position card shows side, shares, cost, entry, TP/SL and post-exit grace. Live
liveness is separate from position state: stopping a worker does not erase its
account data or switch to paper.

- **Cash** is the live state balance available in collateral.
- **Realized PnL** is the precise sum recorded by the trader for completed trades.
  Deposits and balance adjustments are not trading profit.
- **Account equity** uses current cash while no position is open. Its plotted
  history preserves dated observations from the trader's equity export; when a
  position is open, the most recent recorded valuation may be older than the UI.
- **P&L mean/SD** is mean dollar PnL divided by sample standard deviation per
  trade. It has no annualization factor. Drawdown requires complete history;
  unavailable history is shown explicitly.

Paper equity is starting capital plus realized PnL. Paper rows are alternative
parameter experiments, not separately funded portfolios to add together. The
best leaderboard row is not an out-of-sample performance claim.

Today's statistics use the Europe/Paris trading day. Event names describe exit
mechanisms; their net PnL is the evidence for a gain or loss. The trade feed is
newest-first. A corrected trade file replaces cached history and position/account
values together.

## Status and recovery

Worker liveness follows that worker's own lock heartbeat. Spot age, model age,
calibration activity, book availability and WebSocket connection are independent
status signals. A green backend health indicator alone does not prove that a new
order is permitted or that a strategy has an edge.

The default view follows configured `MODE`. Clicking the title control opts into
saved dry/live alternation; a WebSocket reconnect restores the chosen view. Slow
clients reconnect for a fresh accounting snapshot. Use the
[runbook](../OPERATIONS.md) for service control and deliberate live stops.
