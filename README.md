# Polymarket BTC 15-Minute Strategy

Bitcoin 15-minute Polymarket trading research with Deribit-calibrated SSVI
probability forecasts, paper strategy grids, live execution, and a real-time
dashboard.

## How it works

Deribit option prices calibrate a Stochastic Volatility Inspired (SSVI) surface.
A shared calculator uses Monte Carlo paths to estimate the probability that the
trailing 60-second average BTC price exceeds the market's strike. Paper strategies
and the live trader consume the same timestamped forecasts through Redis.

The stack includes market-data collection, a configurable paper grid, live order
reconciliation, persistence, watchdog recovery, and a FastAPI/React dashboard.
Canonical model and strategy settings live in
[`BTC_pricer_15m/config.yaml`](BTC_pricer_15m/config.yaml).

The average-payoff model is an approximation, not an exact reproduction of
Chainlink's sampling procedure. Stale inputs and unsupported contract or fee
metadata block entries. Settlement uses resolved token payouts. Paper results,
option-fit quality, and healthy workers do not establish a profitable strategy.

## Getting started

Install Docker Desktop with Linux containers and Python 3.11+ for the management
wrapper. From the repository root, create only missing environment files and
start the paper services:

```powershell
if (!(Test-Path -LiteralPath .\BTC_pricer_15m\.env)) {
    Copy-Item .\BTC_pricer_15m\.env.example .\BTC_pricer_15m\.env
}
if (!(Test-Path -LiteralPath .\POLYMARKET_STREAMING_DASHBOARD_15m\.env)) {
    Copy-Item .\POLYMARKET_STREAMING_DASHBOARD_15m\.env.example .\POLYMARKET_STREAMING_DASHBOARD_15m\.env
}
python .\POLYMARKET_STREAMING_DASHBOARD_15m\manage.py start
```

Open the dashboard at [localhost:5175](http://127.0.0.1:5175).
Keep credentials in local `.env` files and runtime/account data out of Git.

Root `manage.py start` and `manage.py restart` include the real-money live trader.
For deliberate live operation, configure the trading `.env` with the intended
wallet, signature type, funder/address, and current Polymarket credentials. The
Docker image supplies `py_clob_client_v2`. Set dashboard `MODE=live` to display
the live account; its template defaults to paper view. Read the
[operations runbook](OPERATIONS.md) for persistent stop intent and safety latches.

## Documentation

| Guide | Contents |
|---|---|
| [Operations](OPERATIONS.md) | Service control, recovery, Docker tests, and runtime verification |
| [Model and trading engine](BTC_pricer_15m/README.md) | Payoff assumptions, experiments, execution, and accounting |
| [Dashboard](POLYMARKET_STREAMING_DASHBOARD_15m/README.md) | Configuration, data sources, permissions, and validation |
| [Dashboard UI guide](POLYMARKET_STREAMING_DASHBOARD_15m/DASHBOARD_GUIDE.md) | Reading model, market, and account displays |

Builds and tests run through Docker using the commands in the operations runbook.
Use isolated test results and a disposable Redis broker for integration checks.
