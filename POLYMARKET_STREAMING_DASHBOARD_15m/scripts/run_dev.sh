#!/usr/bin/env bash
# Launches backend (FastAPI on :8798) and frontend (Vite on :5175) together.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"

cleanup() {
  jobs -p | xargs -r kill 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "[dev] starting backend uvicorn :8798"
(cd "${ROOT}/backend" && python -m uvicorn app.main:app --host 127.0.0.1 --port 8798 --reload) &

echo "[dev] starting vite :5175"
(cd "${ROOT}/frontend" && npm run dev) &

wait
