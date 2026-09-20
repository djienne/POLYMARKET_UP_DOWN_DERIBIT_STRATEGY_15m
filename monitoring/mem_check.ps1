# mem_check.ps1 — sample container memory, append to CSV, report per-container trend.
# Usage: powershell -File mem_check.ps1
# Detects gradual increase (memory leak) by linear-regression slope (MiB/hour)
# over all samples collected this session, per container.

$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$csv  = Join-Path $here "mem_samples.csv"

# Containers to track: the 15m stack plus the two at-risk siblings on other stacks.
$targets = @(
  # 15m stack
  "btc_pricer_15m_redis","btc_pricer_15m_spot","btc_pricer_15m_calc",
  "btc_pricer_15m_grid","btc_pricer_15m_live","polymarket_dashboard_15m",
  # 5m stack
  "btc_pricer_5m_redis","btc_pricer_5m_spot","btc_pricer_5m_calc",
  "btc_pricer_5m_grid","btc_pricer_5m_live","polymarket_dashboard_5m",
  # 1d stack (well-provisioned; watch for slow creep)
  "polymarket-1d-redis","polymarket-1d-calc","polymarket-1d-paper",
  "polymarket-1d-dashboard","polymarket-1d-live",
  # INTERMARKET family (fleet -- monitored)
  "INTERMARKET","INTERMARKET_DXY","INTERMARKET_M2","INTERMARKET_NDX","INTERMARKET_SP500"
)

function To-MiB([string]$s) {
  $s = $s.Trim()
  if ($s -match '([0-9.]+)\s*([KMG]i?B)') {
    $n = [double]$matches[1]; $u = $matches[2]
    switch -Regex ($u) {
      'GiB|GB' { return $n * 1024 }
      'MiB|MB' { return $n }
      'KiB|KB' { return $n / 1024 }
      default  { return $n }
    }
  }
  return $null
}

$ts = (Get-Date).ToString("s")
# Query ALL running containers and filter to our targets. Passing an explicit
# name list to `docker stats` errors out if ANY name is absent (e.g. the
# profile-gated *_live containers when not running), which would drop the whole
# sample. Filtering the full list is robust to missing containers.
$all = docker stats --no-stream --format "{{.Name}};{{.MemUsage}};{{.MemPerc}}"
$raw = $all | Where-Object { $targets -contains ($_.Split(';')[0]) }

if (-not (Test-Path $csv)) { "timestamp;container;mem_mib;limit_mib;pct" | Out-File -FilePath $csv -Encoding utf8 }

foreach ($line in $raw) {
  $p = $line.Split(';')
  if ($p.Count -lt 3) { continue }
  $name = $p[0]
  $usage = $p[1].Split('/')
  $mem = To-MiB $usage[0]
  $lim = if ($usage.Count -gt 1) { To-MiB $usage[1] } else { $null }
  $pct = $p[2].TrimEnd('%')
  "$ts;$name;$mem;$lim;$pct" | Out-File -FilePath $csv -Append -Encoding utf8
}

# --- Trend analysis ---
$rows = Import-Csv -Path $csv -Delimiter ';'
$byC = $rows | Group-Object container
"=== MEM TREND (this session, n samples per container) ==="
"{0,-26} {1,7} {2,8} {3,8} {4,9} {5,10} {6}" -f "container","n","first","last","delta","slope/h","verdict"
foreach ($g in $byC) {
  $s = $g.Group | Sort-Object timestamp
  $n = $s.Count
  $first = [double]$s[0].mem_mib
  $last  = [double]$s[-1].mem_mib
  $lim   = [double]$s[-1].limit_mib
  $delta = [math]::Round($last - $first,1)
  # linear regression slope in MiB per hour
  $t0 = [datetime]$s[0].timestamp
  $xs = @(); $ys = @()
  foreach ($r in $s) { $xs += (([datetime]$r.timestamp) - $t0).TotalHours; $ys += [double]$r.mem_mib }
  $slope = 0.0
  if ($n -ge 2) {
    $mx = ($xs | Measure-Object -Average).Average
    $my = ($ys | Measure-Object -Average).Average
    $num = 0.0; $den = 0.0
    for ($i=0; $i -lt $n; $i++) { $num += ($xs[$i]-$mx)*($ys[$i]-$my); $den += ($xs[$i]-$mx)*($xs[$i]-$mx) }
    if ($den -gt 0) { $slope = $num/$den }
  }
  $slopeR = [math]::Round($slope,1)
  $spanH = if ($n -ge 2) { (([datetime]$s[-1].timestamp) - $t0).TotalHours } else { 0 }
  # A real leak is a SUSTAINED climb of REAL memory over REAL time. Require all of:
  #   >=3 samples, >=0.5h span, >150 MiB absolute growth, >20 MiB/h rate.
  # This rejects the noise that fooled a raw-slope check: redis AOF/stream
  # oscillation (~10 MiB wiggle) and one-time uncapping steps (e.g. DXY 512->613,
  # where the old capped baseline produces a huge phantom slope but small delta).
  $nearCap = ($lim -gt 0 -and ($last/$lim) -ge 0.90)
  $verdict = "flat/ok"
  if ($n -ge 3 -and $spanH -ge 0.5 -and $delta -gt 150 -and $slope -gt 20) { $verdict = "RISING (possible leak)" }
  if ($nearCap) { $verdict = "NEAR CAP ($([math]::Round($last/$lim*100,0))%)" }
  "{0,-26} {1,7} {2,8} {3,8} {4,9} {5,10} {6}" -f $g.Name,$n,[math]::Round($first,0),[math]::Round($last,0),$delta,$slopeR,$verdict
}
