#!/usr/bin/env bash
# Production-style launch: build frontend once, then serve everything from uvicorn on :8798.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"

echo "[prod] building frontend"
(cd "${ROOT}/frontend" && npm run build)

echo "[prod] launching uvicorn :8798 (serves /api, /ws, and static dist/)"
cd "${ROOT}/backend"
exec python -m uvicorn app.main:app --host 127.0.0.1 --port 8798
