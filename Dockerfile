FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
COPY README.md ./
RUN pip install --no-cache-dir .

# Runs the worker; point it at the server via TEMPORAL_ADDRESS.
ENV TEMPORAL_ADDRESS=temporal:7233
ENTRYPOINT ["order-worker"]
