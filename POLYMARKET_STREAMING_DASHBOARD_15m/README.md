# Polymarket Streaming Dashboard

FastAPI + React dashboard for the sibling BTC 15-minute trading stack. Production
runs in Docker at [127.0.0.1:5175](http://127.0.0.1:5175); backend port 8798 is
internal. Use the [operations runbook](../OPERATIONS.md) for service control,
rebuilds, persistent stops and verification. The [UI guide](DASHBOARD_GUIDE.md)
explains model and accounting displays.

The UI fits a 16:9 streaming frame and shows market/model probabilities, entry
thresholds, position state, trading windows, worker/model status and performance.

## Configuration and view selection

Copy missing configuration from [.env.example](.env.example). Compose supplies
container results/config paths, Redis and the internal log-adapter URL. Its
`env_file` reads this folder's `.env`; changes need a container recreation.

- `MODE=live` selects live state and trades even when the live worker is stale.
- `MODE=dry_run` selects the grid. The strategy mirror is resolved from canonical
  live parameters, with `DEFAULT_INSTANCE_ID` only a fallback.
- Dry/live alternation is opt-in and saved in the browser. Reconnection restores
  the selected source and instance.
- `DASHBOARD_API_TOKEN` optionally protects data HTTP routes and WebSocket access.
  Keep the default localhost binding. A browser token can be stored as
  `dashboardApiToken` in local storage; do not bake a secret into a public bundle.

## Data and permissions

Collectors own change detection and publish cached snapshots. HTTP/WebSocket
consumers never consume another client's pending update. Atomic CSV corrections
produce an accounting reset; congestion closes a slow WebSocket so reconnection
loads a complete snapshot.

The dashboard reads results and configuration through read-only mounts. It has
no Docker CLI or socket. The separate internal `docker-logs` service receives the
socket and exposes only bounded, fixed-grid model-probability log lines through
`GET /logs`; it cannot forward container management requests.

Live cash and precise realized PnL come from `15m_live_state.json`. Recorded
account-equity history comes from `15m_live_equity.csv`, preserving timestamps and
cash adjustments. Paper equity accumulates realized PnL. Neither cash changes nor
an old chart value establish a trading profit or authorize financial repair.

## Validation

Use the Docker commands in the runbook. Dashboard pytest includes backend tests
and `management_tests` copied from this folder's `tests/`. The image includes
pyflakes, pytest and pytest-asyncio. Its frontend build runs Node's test runner
through `tsx`, then TypeScript and Vite; native npm/pytest is not a verification
path for this workspace.

Verify the host URL as well as container health: Docker's internal health check
does not exercise Windows port forwarding. The runbook describes how to isolate
an unreachable host port from an application failure.

The Dockerfile has `frontend-build`, `docker-logs` and `dashboard` targets. The
runtime target receives shared manager helpers through Compose's named
`runtime_helpers` build context, so operations code is not duplicated here.
Legacy native-manager switches remain compatibility tools; use Docker for normal
operation and validation.
