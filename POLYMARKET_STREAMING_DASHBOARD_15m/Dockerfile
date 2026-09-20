# Stage 1: Build frontend
FROM node:24-alpine AS frontend-build
WORKDIR /build
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ .
RUN npm test && npm run build

# Separate, narrowly scoped service; only this target receives the Docker socket.
FROM python:3.11-slim AS docker-logs
RUN pip install --no-cache-dir "httpx>=0.27"
COPY backend/app/docker_logs_proxy.py /app/docker_logs_proxy.py
EXPOSE 8799
CMD ["python", "/app/docker_logs_proxy.py"]

# Stage 2: Python runtime + built frontend
FROM python:3.11-slim AS dashboard

WORKDIR /app

RUN pip install --no-cache-dir \
    "fastapi>=0.115" \
    "uvicorn[standard]>=0.30" \
    "pydantic>=2.7" \
    "pydantic-settings>=2.3" \
    "python-dotenv>=1.0" \
    "httpx>=0.27" \
    "redis>=5.0" \
    "aiohttp>=3.9" \
    "aiohttp-retry>=2.8" \
    "websockets>=13.0" \
    "pytest>=8.0" \
    "pytest-asyncio>=0.23" \
    "pyflakes==3.4.0"

COPY backend/ ./backend/
COPY --from=frontend-build /build/dist ./frontend/dist
COPY manage.py live_manager.py ./
COPY --from=runtime_helpers /scripts/stack_services.py /scripts/live_runtime_state.py /BTC_pricer_15m/scripts/
COPY tests/ ./management_tests/

EXPOSE 8798

WORKDIR /app/backend
ENV PYTHONPATH=/app:/app/backend
RUN python -m pyflakes app tests ../manage.py ../live_manager.py ../management_tests
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8798"]
