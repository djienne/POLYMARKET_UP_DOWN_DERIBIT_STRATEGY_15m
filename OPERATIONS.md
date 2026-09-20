# Operations

Run the normal commands from the repository root. Python launches Docker; builds,
tests, smoke checks and runtime verification run inside Docker.

```powershell
python .\manage.py start
python .\manage.py stop
python .\manage.py restart --build
python .\manage.py status
python .\manage.py doctor
python .\manage.py logs live --tail 120
```

Root `start` and `restart` explicitly include real-money `live` and the
`local-live` profile. `stop` records stopped live intent before stopping the
supervisor and remaining services. The dashboard is [localhost:5175](http://127.0.0.1:5175);
container port 8798 is its backend, not a separately published host port.

If the host URL fails despite a healthy container, compare `/api/health` inside
the container and from a sibling service with the same request from Windows.
An internal HTTP 200 does not verify Docker Desktop's published port. Recreate
the dashboard to restore that mapping before considering an engine-wide restart,
which would also interrupt other projects. Keep the localhost-only binding.

## Services and stop intent

[`scripts/stack_services.py`](BTC_pricer_15m/scripts/stack_services.py) defines
the eight services and their dependency order:

| Service | Purpose |
|---|---|
| `redis` | Calibration, spot, market-context and mirror-signal IPC |
| `spot` | BTC observations with source timestamps and freshness |
| `calc` | Contract-aware probability estimates and market lifecycle |
| `grid` | 1296 paper parameter combinations in the current experiment |
| `live` | One real-money trader; enabled through `local-live` |
| `docker-logs` | Internal adapter exposing only fixed-grid model log lines |
| `dashboard` | FastAPI and built React UI; results/config are read-only |
| `watchdog` | Recovery of stopped containers in dependency order |

The watchdog has `restart: always`; other workers use `unless-stopped`.
It can start existing stopped containers, not recreate missing containers.
Running-but-unhealthy services require diagnosis; a container restart is not a
scientific validity check.

Two independent holds protect live execution:

- `results/.live_desired.json` records operator intent. A live stop persists
  until an explicit start, including across Docker restarts.
- `results/diagnostics/live_watchdog_last_trigger.txt` is a safety latch. It
  persists until explicitly acknowledged after reviewing the diagnosis.

The separate `results/.maintenance_stop` pauses stack recovery for 12 hours.
Its expiry does **not** clear either live hold. Do not use a raw `docker stop`
as a substitute for recording desired live state.

To stop only live, or resume after reviewing a safety diagnosis, from the root:

```powershell
python .\POLYMARKET_STREAMING_DASHBOARD_15m\manage.py live stop
python .\POLYMARKET_STREAMING_DASHBOARD_15m\manage.py live local --acknowledge-safety-stop
```

Root `start` and `restart` also accept `--acknowledge-safety-stop`. An ordinary
start refuses an unacknowledged latch. The reactive host watchdog
`BTC_pricer_15m/scripts/live_watchdog.ps1` records the hold before stopping live
and captures diagnostics. The container watchdog cannot undo that hold.

The dashboard-folder manager has a different scope: its ordinary `start`/`stop`
manage the non-live services; its `live` subcommand explicitly controls live.
Use the root manager for complete-stack operations.

## Model and experiment boundaries

`BTC_pricer_15m/config.yaml` is canonical. Calibration schema v5 includes payoff,
contract/fee metadata and probability observation/evaluation times. The current
60-second Chainlink-average forecast is an **approximation** using SSVI-derived
volatility; it does not reproduce an unpublished oracle sampling procedure.
A model probability is not a measured probability or evidence of trading edge.

TWAP pricing waits at most one second after fitting for a new spot observation
whose source age is at most 1.4 seconds. Missing observations skip pricing;
the entry guard still requires both pricing and source observation ages to be
within two seconds. Slow book requests share the remaining freshness budget.
Calibration timing reports source-to-receive delay, pricing spot age,
pricing-to-publication duration and publication spot age. Grid/live logs report
entry-input validity counts and consumer spot age during permitted entry windows.
Use these together: fresh publications alone do not prove fresh entry decisions.

Each market must have supported resolution semantics, matching outcome tokens,
a known fee schedule and fresh valid entry inputs. Old v1-v4 calibration files
remain readable during migration; they do not authorize a v5 entry.
Polymarket's resolved token payout is the settlement authority. A token winner
alone does not establish a final BTC price: `settlement_price` stays null when
that measurement is unavailable.

Paper fills apply the market's V2 fee schedule at each price level. Live
accounting uses actual execution amounts. Do not apply one historical global
fee percentage or infer live fees from the paper estimate.

The 1296-instance paper experiment is `twap_v5_20260905`. A legacy snapshot is
rejected by its experiment identity. Archive old **paper** outputs deliberately
before a new baseline; preserve live history and historical research evidence.
Compare live with the parameter-matched mirror or `--live-shadow`, not the
leaderboard winner. Current no-entry windows are the first 300 and final 180
seconds; recheck canonical config when changing them.

## Verification after changes

From `BTC_pricer_15m/`, rebuild before testing the current image:

```powershell
docker compose --profile local-live build
docker compose run --rm --no-deps -v /app/results calc python -m pytest
docker compose run --rm --no-deps dashboard python -m pytest
docker compose run --rm --no-deps dashboard python -m pyflakes app tests ../manage.py ../live_manager.py ../management_tests
```

The calc command replaces the live results bind mount with an isolated anonymous volume.
This also prevents fixture setup from scanning production settlement history.
The Redis lease and calc-entrypoint integration checks require `TEST_REDIS_URL`
pointing to a disposable Redis broker. Supply it with `compose run -e
TEST_REDIS_URL=redis://<test-container>:6379/0`; never point tests at production Redis.

Calc's full suite includes the mandatory undefined-name check in
`tests/test_pyflakes_smoke.py`. Dashboard pytest includes management tests
outside its backend directory. Its image build runs frontend behavior tests
and TypeScript/Vite compilation. Test fixtures must use isolated results paths;
do not run experiments against live state.

Recreate all authorized services after code/config changes with root
`python .\manage.py restart --build`. Preserve deliberate stops and unresolved
safety latches; do not acknowledge a fault merely to complete a deployment.
Check service health, advancing worker heartbeats, input freshness, rejection
reasons and actual fills/account balances after restart. Dashboard HTTP health
alone does not establish trading readiness or strategy profitability.

## Interpreting and repairing outputs

All active outputs live under `BTC_pricer_15m/results/`. Grid state/trades and
leaderboard describe paper execution; live JSON/CSV and SQLite describe the
live account. Leaderboard `as_of` is snapshot time and `written_at` is report
write time. `pnl_mean_to_sd` is per-trade dollar-PnL mean/sample SD, not an
annualized Sharpe; incomplete historical statistics are unavailable.

In `MODE=live`, stale liveness never selects paper results. Trader liveness uses
its own touched lock; fresh calc/grid output cannot mask a dead live worker.
Collectors alone consume file changes. Atomic CSV corrections trigger a fresh
client accounting snapshot; slow clients reconnect rather than lose trades.

Live cash, precise realized trading PnL and dated account-equity observations
are separate quantities. Cash flows, rounding and reconciliation can make them
differ. The flat-account equity headline uses current state cash; the chart
preserves recorded dates and values. Use authenticated account evidence and the
existing reconciliation tools before applying a financial correction. Never
reset or rewrite live PnL to make a chart agree with an old balance.
