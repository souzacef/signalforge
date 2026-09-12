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
openssl rand -hex 32
cd backend
uv sync --locked
```

Copy the generated value into `SIGNALFORGE_JWT_SECRET` in `.env`. Do not commit
the populated file.

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
- `POST /api/v1/incidents` — creates an incident with initial status `open`;
  requires an `operator` or `admin` bearer token.
- `GET /api/v1/incidents/{incident_id}` — retrieves an incident by UUID;
  requires a `viewer`, `operator`, or `admin` bearer token.
- `POST /api/v1/incidents/{incident_id}/acknowledge` — acknowledges an open
  incident; requires an `operator` or `admin` bearer token.
- `POST /api/v1/incidents/{incident_id}/resolve` — resolves an acknowledged
  incident; requires an `operator` or `admin` bearer token.
- `POST /api/v1/auth/token` — authenticates an email and password.
- `GET /api/v1/auth/me` — returns the authenticated user.

Interactive API documentation is available at <http://127.0.0.1:8000/docs>.

Create an incident:

```sh
curl -X POST http://127.0.0.1:8000/api/v1/incidents \
  -H 'Authorization: Bearer your-access-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "manual",
    "title": "Checkout API error rate increased",
    "description": "5xx error rate exceeded threshold",
    "severity": "high",
    "occurred_at": "2026-09-11T18:30:00Z"
  }'
```

## Local authentication

Passwords are hashed with Argon2 and access tokens are signed JWTs containing
only the user UUID and issued-at/expiration timestamps. Tokens expire after 30
minutes by default; configure `SIGNALFORGE_ACCESS_TOKEN_EXPIRE_MINUTES` to
change that duration.

There is no public registration endpoint. After applying migrations, bootstrap
a user from `backend/`; the command prompts for the password twice without
accepting it as a command-line argument:

```sh
uv run python -m signalforge.users.create_user \
  --email admin@example.com \
  --role admin
```

Log in using the first-party OAuth2 password form. SignalForge uses an email
address in the standard form field named `username`:

```sh
curl -X POST http://127.0.0.1:8000/api/v1/auth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'username=admin@example.com' \
  --data-urlencode 'password=your-password'
```

Use the returned access token to resolve the current user:

```sh
curl http://127.0.0.1:8000/api/v1/auth/me \
  -H 'Authorization: Bearer your-access-token'
```

Incident endpoints require authentication and enforce these explicit role
permissions:

| Role | Read | Create | Acknowledge | Resolve |
| --- | --- | --- | --- | --- |
| `viewer` | Yes | No | No | No |
| `operator` | Yes | Yes | Yes | Yes |
| `admin` | Yes | Yes | Yes | Yes |

Access tokens do not contain role claims. Each authenticated request loads the
current User from PostgreSQL, so role and active-state changes apply without
waiting for the token to expire. A missing or invalid bearer token is an
authentication failure (`401` with `WWW-Authenticate: Bearer`); an authenticated
user without an allowed role receives `403`.

The deterministic Incident lifecycle is `OPEN -> ACKNOWLEDGED -> RESOLVED`.
Direct arbitrary status updates and reopening are not exposed. Each transition
stores its UTC timestamp and the authenticated actor's UUID; invalid state
transitions return `409 Conflict`. A lifecycle history or timeline and broader
administrator management are not implemented yet.

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
