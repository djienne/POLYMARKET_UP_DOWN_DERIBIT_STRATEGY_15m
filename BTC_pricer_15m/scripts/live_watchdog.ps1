param(
    [int]$SinceSeconds = 10,
    [string]$Service = "live",
    [string]$Profile = "local-live",
    [string]$DiagnosticsRoot = "results/diagnostics",
    [int]$LogTail = 600,
    [string]$ContainerName = "btc_pricer_15m_live",
    [switch]$NoParity,
    [int]$ParityInstance = -1,
    [double]$ParityIntervalSeconds = 15.0,
    [double]$ParityLookbackMinutes = 45.0,
    [double]$ParityEntryGraceSeconds = 20.0,
    [double]$ParityCloseGraceSeconds = 60.0,
    [double]$ParityCloseTimeTolSeconds = 90.0,
    [double]$ParityTimeTolSeconds = 30.0
)

$ErrorActionPreference = "Stop"
$stackDirectory = Split-Path -Parent $PSScriptRoot
$argsList = @(
    "-m", "scripts.live_watchdog",
    "--since-seconds", "$SinceSeconds",
    "--service", $Service,
    "--profile", $Profile,
    "--diagnostics-root", $DiagnosticsRoot,
    "--log-tail", "$LogTail",
    "--container-name", $ContainerName,
    "--parity-interval-seconds", "$ParityIntervalSeconds",
    "--parity-lookback-minutes", "$ParityLookbackMinutes",
    "--parity-entry-grace-seconds", "$ParityEntryGraceSeconds",
    "--parity-close-grace-seconds", "$ParityCloseGraceSeconds",
    "--parity-close-time-tol-seconds", "$ParityCloseTimeTolSeconds",
    "--parity-time-tol-seconds", "$ParityTimeTolSeconds"
)
if ($NoParity) {
    $argsList += "--no-parity-enabled"
}
if ($ParityInstance -ge 0) {
    $argsList += @("--parity-instance", "$ParityInstance")
}
Push-Location -LiteralPath $stackDirectory
try {
    & python @argsList
    $watchdogExitCode = $LASTEXITCODE
} finally {
    Pop-Location
}
exit $watchdogExitCode
