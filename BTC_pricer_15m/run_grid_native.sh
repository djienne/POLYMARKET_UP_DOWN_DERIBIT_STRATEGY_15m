#!/usr/bin/env bash
# Supervisor wrapper for the native 15m grid trader.
# Restarts the python process if it exits, with a 5s backoff. Mirrors
# Docker's `restart: unless-stopped` semantics. Stops gracefully on SIGTERM.
set -u

cd "$(dirname "$0")"
mkdir -p logs

PY=/c/Users/david/miniconda3/envs/btc_pricer/python.exe

export OMP_NUM_THREADS=4
export NUMBA_NUM_THREADS=4
export OPENBLAS_NUM_THREADS=4
export MKL_NUM_THREADS=4

trap 'echo "[supervisor] received SIGTERM, exiting"; kill ${TRADER_PID:-0} 2>/dev/null; exit 0' TERM INT

while true; do
  echo "[supervisor] $(date -u +'%Y-%m-%dT%H:%M:%SZ') starting trader"
  "$PY" -u -m scripts.trader.main --mode grid -v &
  TRADER_PID=$!
  wait "$TRADER_PID"
  rc=$?
  echo "[supervisor] $(date -u +'%Y-%m-%dT%H:%M:%SZ') trader exited rc=$rc; restarting in 5s"
  rm -f results/trader.lock 2>/dev/null
  sleep 5
done
