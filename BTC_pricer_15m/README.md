# BTC_pricer_15m

Polymarket BTC 15-minute research and execution stack. It shares market data and
probability calculations between a 1296-instance paper grid and one live trader.
Use the [operations runbook](../OPERATIONS.md) for startup, persistent stops,
recovery, Docker tests and deployment. Configuration is [config.yaml](config.yaml).

## Model and market contract

Deribit options calibrate an SSVI surface. Calibration schema v5 records the
payoff, contract/fee metadata and timestamps of probability evaluation and its
spot observation. The current Chainlink 60-second-average forecast uses an
SSVI-derived effective-volatility approximation. It is not an exact model of
Chainlink sampling; closed-form terminal digitals and average-payoff Monte Carlo
are different quantities.
The average-payoff calculation uses 300,000 antithetic paths and a maximum
five-second step, with uncertainty estimated from pair averages.

[`contracts.py`](scripts/trader/contracts.py) validates explicit resolution
sources, condition/outcome-token identity and fee schedules. Unsupported metadata
or stale entry inputs block new exposure. Current entries require fresh probability
and spot observations, including a check after signing immediately before posting.
TWAP pricing waits briefly for a new source observation after fitting; stale or
missing updates skip the calculation. The two-second entry limit is unchanged.
See the runbook for the timing budgets and producer/consumer freshness diagnostics.
The first 300 and final 180 seconds of a market are blocked by configuration.

Expiry accounting uses the resolved Polymarket token payout. A winning token
pays one collateral unit and a losing token zero. Without a measured final BTC
value, the settlement export keeps `settlement_price` null; adjacent strikes and
spot feeds are not substitutes for a resolved contract.
When CLOB winner metadata lags, explicit outcome indices bind Polygon
ConditionalTokens payouts to the correct tokens. Settlement reads run in the
background; a pending payout never pins discovery to an expired market.

Paper executions walk bid/ask depth and apply each market's V2 fee schedule per
price level. For supported taker-only exponent-1 schedules the collateral fee is
`shares * rate * p * (1-p)`, conservatively rounded upward to 1e-5. Live accounting
uses execution receipts and net amounts, not this estimate. Paper liquidity and
slippage assumptions still require empirical validation.

## Experiment and results

The current paper experiment is `twap_v5_20260905`: the configured alpha/floor/TP
axes plus stop-loss values produce 1296 combinations. Old snapshots with another
experiment identity are rejected. Preserve old paper outputs separately and start
a deliberate new baseline; never reset live history for a grid migration.

`results/state_snapshot.json`, `trades.csv` and `leaderboard.csv` describe paper
execution. Live uses `15m_live_state.json`, `15m_live_trades.csv`,
`15m_live_equity.csv` and SQLite persistence. Calibration travels through Redis
with legacy inbox-file transport fallback. Reading old formats does not make an
old payoff model eligible to trade current contracts.

The leaderboard ranks realized PnL. `pnl_mean_to_sd` is dollar-PnL mean/sample SD
per trade, without annualization. Drawdown comes from complete realized history;
unknown history yields unavailable statistics. Report `as_of` and `written_at`
distinguish state time from write time. The best parameter row is a research
selection, not out-of-sample evidence of edge.

Availability evidence is recorded in the scoped `availability_ledger.csv` and
health logs. The offline `scripts.trader.cli.clean_unavailable_dry_trades` tool
is report-only unless `--apply` is supplied; inspect evidence before corrections.
Its append-only `ENTRY_VOID` markers remove the paired paper entry/close from
replayed accounting, including the dashboard. Inferred or open-ended outage
intervals are not automatic proof that a historical fill was impossible.

The dashboard separates live cash, precise realized PnL and dated account-equity
observations. Compare live with its exact parameter mirror or `--live-shadow`;
never compare against an arbitrary paper winner or combine their capital.

## Execution and recovery

Live order intent is persisted before submission. A timeout or uncertain response
must be reconciled by the submitted order identity before another exposure is
attempted. Cancellation and residual fills require confirmation. Inspect pending
intent, venue orders, balances and receipt provenance when recovering; a healthy
process is not proof that its orders or accounting are correct.

Use the existing reconciliation tools in read-only mode before applying a repair.
Preserve their audit records and authenticate the account evidence. The root
[runbook](../OPERATIONS.md) documents the independent operator-intent and watchdog
safety holds, including explicit acknowledgment for a reviewed safety stop.
