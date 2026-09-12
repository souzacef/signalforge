FROM ghcr.io/astral-sh/uv:0.12.13 AS uv

FROM python:3.13-slim-bookworm AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_NO_CACHE=1

WORKDIR /app

COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY src ./src
RUN uv sync --locked --no-dev --no-editable

FROM python:3.13-slim-bookworm AS runtime

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 signalforge \
    && useradd --uid 10001 --gid signalforge --no-create-home \
        --home-dir /app --shell /usr/sbin/nologin signalforge

WORKDIR /app

COPY --from=builder --chown=signalforge:signalforge /app/.venv ./.venv
COPY --chown=signalforge:signalforge alembic.ini ./
COPY --chown=signalforge:signalforge alembic ./alembic

USER signalforge

EXPOSE 8000

CMD ["uvicorn", "signalforge.main:app", "--host", "0.0.0.0", "--port", "8000"]
