# Agent Notes

## Scope and sources

- `BTC_pricer_15m/`: calc, grid, live, market data and persistence.
- `POLYMARKET_STREAMING_DASHBOARD_15m/`: dashboard and restricted model-log adapter.
- [OPERATIONS.md](OPERATIONS.md) is the runbook. `BTC_pricer_15m/config.yaml` is
  canonical configuration; `BTC_pricer_15m/scripts/stack_services.py` defines all eight services.
- Preserve concurrent edits, live history and old experiment evidence. Never
  persist credentials or treat a historical account balance as current authority.

## Docker verification

Run builds, tests, smoke checks and runtime verification through Docker from
`BTC_pricer_15m/`. Do not use native Windows pytest/npm runs as verification.
After every code change, rebuild and run the full relevant suite; changes across
projects/shared runtime configuration require both suites:

```powershell
docker compose --profile local-live build
docker compose run --rm --no-deps -v /app/results calc python -m pytest
docker compose run --rm --no-deps dashboard python -m pytest
docker compose run --rm --no-deps dashboard python -m pyflakes app tests ../manage.py ../live_manager.py ../management_tests
```

Calc pytest includes mandatory pyflakes undefined-name checks. Dashboard pytest
includes its management tests; its Docker build runs frontend behavior tests and
TypeScript/Vite compilation. The anonymous `/app/results` volume isolates calc
tests from production history. Use isolated results/config paths for diagnostic
fixtures. Tests of mocks alone do not establish scientific correctness.

Before considering a code change complete, rebuild/recreate the full authorized
stack with `local-live`, then verify actual runtime progress. Root
`python .\manage.py restart --build` explicitly includes live. Preserve stopped
intent and unresolved safety latches; never acknowledge a fault to bypass a gate.

## Runtime and dashboard

Root `manage.py` manages `redis spot calc grid live docker-logs dashboard watchdog`
and tries to start Docker Desktop on Windows. Dashboard is
[127.0.0.1:5175](http://127.0.0.1:5175); backend container port is 8798.

`.live_desired.json` is persistent live intent. The watchdog safety trigger is
an independent latch requiring `--acknowledge-safety-stop` after diagnosis.
The 12-hour stack maintenance marker does not expire live intent or safety.
Use managers to stop live; a raw container stop does not record operator intent.
The reactive watchdog records a hold before stopping live. The recovery watchdog
respects both holds and dependency health; it does not repair running unhealthy
workers or create missing containers.

In `MODE=live`, retain live account data regardless of liveness. Paper selection
uses `MODE=dry_run` or explicit view selection. Each trader's own lock heartbeat
proves loop progress; shared model freshness is a separate status. Collectors own
change detection and event publication; HTTP/WebSocket snapshots read their cache.
Dashboard never receives a Docker socket; its internal logs adapter only exposes
fixed-container model probability lines.

## Scientific/accounting boundaries

Calibration schema v5 carries payoff, market fee/contract metadata and input
observation times. The 60-second-average SSVI forecast is approximate, not exact
oracle replication. Unknown contracts/fees and stale probabilities block entries.
Resolved token payouts determine expiry PnL; do not fabricate a final BTC price
or use next-market strikes as settlement ground truth.

The current grid has 1296 combinations and experiment `twap_v5_20260905`. Do not
mix legacy terminal-payoff results into it. Compare live with its exact parameter
mirror or live-shadow; resolve the mirror from current config rather than a
historical instance number. Current entry blocks are first 300 / final 180 seconds.

Keep cash, realized trading PnL and dated account equity distinct. Reconcile against
authenticated account evidence before modifying live records. `pnl_mean_to_sd`
is an unannualized per-trade statistic; complete historical drawdown and current
worker health are separate from profitability or scientific validity.
