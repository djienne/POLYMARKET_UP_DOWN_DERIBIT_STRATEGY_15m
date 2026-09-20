FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements-docker.txt ./

RUN apt-get update \
    && apt-get install -y --no-install-recommends openssh-client g++ \
    && python -m pip install --upgrade pip \
    && pip install -r requirements-docker.txt \
    && apt-get purge -y --auto-remove g++ \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN mkdir -p results

CMD ["python", "-u", "-m", "scripts.trader.main", "--mode", "grid", "-v"]
