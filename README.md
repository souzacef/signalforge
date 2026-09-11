# SignalForge

SignalForge is a portfolio project for exploring incident automation and
AI-assisted operations. Its current backend increment can persist, create, and
retrieve incidents through a small Python API backed by PostgreSQL.

The application begins as a modular monolith. This keeps deployment and local
development straightforward while domain boundaries are still emerging, without
preventing modules from being separated later when real operational needs justify
it.

Event-driven messaging, AI/RAG, observability, Angular, Kubernetes, Helm,
Terraform, and AWS are planned directions. They are not implemented in this
phase.

## Prerequisites

- Python 3.13 (managed automatically by `uv` when configured to do so)
- [uv](https://docs.astral.sh/uv/)
- Podman with a Compose provider, or Docker with Compose

## Local setup

Create the local environment file and install the locked dependencies:

```sh
cp .env.example .env
cd backend
uv sync --locked
```

The application reads `.env` from either the current directory or its parent.
Environment variables take precedence over values in the file.

Start PostgreSQL from the repository root with either runtime:

```sh
podman compose up -d
podman compose ps
```

```sh
docker compose up -d
docker compose ps
```

From `backend/`, start the API:

```sh
uv run uvicorn signalforge.main:app --app-dir src --reload
```

The API exposes:

- `GET /health/live` — process liveness only; it never checks PostgreSQL.
- `GET /health/ready` — returns `200` when PostgreSQL answers a minimal query,
  otherwise `503` without exposing connection details.
- `POST /api/v1/incidents` — creates an incident with initial status `open`.
- `GET /api/v1/incidents/{incident_id}` — retrieves an incident by UUID.

Interactive API documentation is available at <http://127.0.0.1:8000/docs>.

Create an incident:

```sh
curl -X POST http://127.0.0.1:8000/api/v1/incidents \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "manual",
    "title": "Checkout API error rate increased",
    "description": "5xx error rate exceeded threshold",
    "severity": "high",
    "occurred_at": "2026-09-11T18:30:00Z"
  }'
```

## Database migrations

Alembic manages the PostgreSQL schema. Apply migrations from `backend/` before
starting the API or running the full test suite:

```sh
uv run alembic upgrade head
```

Create future revisions after changing model metadata:

```sh
uv run alembic revision --autogenerate -m "describe the change"
```

## Quality checks

Run all tests (including the PostgreSQL integration test) while PostgreSQL is
healthy:

```sh
uv run pytest
```

Run only fast tests without PostgreSQL:

```sh
uv run pytest -m "not integration"
```

Formatting, linting, and type checking:

```sh
uv run ruff format --check .
uv run ruff check .
uv run mypy src
```

To apply formatting, run `uv run ruff format .`.

Stop the local database from the repository root with either
`podman compose down` or `docker compose down`. The named volume preserves its
data; add `--volumes` only when you intentionally want to delete local database
data.
