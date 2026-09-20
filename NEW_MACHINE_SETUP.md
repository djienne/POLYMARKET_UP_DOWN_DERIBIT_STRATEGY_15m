# New machine setup

Install Docker Desktop with Linux containers and a host Python 3.11+ for the
operations wrapper. Node, npm, Conda and native scientific libraries are not
required for the Docker workflow. Keep both project folders under this root.

Create only missing environment files:

```powershell
if (!(Test-Path -LiteralPath .\BTC_pricer_15m\.env)) {
    Copy-Item .\BTC_pricer_15m\.env.example .\BTC_pricer_15m\.env
}
if (!(Test-Path -LiteralPath .\POLYMARKET_STREAMING_DASHBOARD_15m\.env)) {
    Copy-Item .\POLYMARKET_STREAMING_DASHBOARD_15m\.env.example .\POLYMARKET_STREAMING_DASHBOARD_15m\.env
}
```

Paper research does not require a funded live account. To build and start only
non-live services from the root:

```powershell
python .\POLYMARKET_STREAMING_DASHBOARD_15m\manage.py start
```

Open [127.0.0.1:5175](http://127.0.0.1:5175). The dashboard template selects paper
view. Follow [OPERATIONS.md](OPERATIONS.md) for full Docker validation, persistent
stops and the current experiment boundary before reusing existing result files.

For deliberate live operation, configure the private key, funder/address and
current Polymarket credentials in the trading `.env`, using the intended wallet
and signature type. The trading image installs `py_clob_client_v2`; do not install
or derive credentials with the retired v1 client. Keep secrets out of terminal
output, committed files and dashboard frontend bundles.

Set dashboard `MODE=live` when live account display is desired. Root
`python .\manage.py start --build` explicitly starts all eight services including
real-money live. It refuses an unacknowledged safety latch. See the runbook for
review and explicit resume; copying an old results directory is not authorization
to resume its live trading.
