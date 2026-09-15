# SignalForge

SignalForge is an event-driven incident intake, triage, and controlled remediation platform built around durable asynchronous workflows, deterministic operational decisions, advisory AI enrichment, and human-in-the-loop execution safety.

It is a portfolio project, but the system is implemented end to end rather than presented as a collection of disconnected demos: authenticated incident APIs, transactional event publication, RabbitMQ workers, deterministic triage, optional Gemini enrichment, controlled remediation, observability, and an Angular operator console packaged for a production-style local demo.

[![Backend CI](https://github.com/souzacef/signalforge/actions/workflows/backend.yml/badge.svg?branch=main)](https://github.com/souzacef/signalforge/actions/workflows/backend.yml)
[![Frontend CI](https://github.com/souzacef/signalforge/actions/workflows/frontend.yml/badge.svg?branch=main)](https://github.com/souzacef/signalforge/actions/workflows/frontend.yml)

## Engineering highlights

- **Transactional outbox:** incident creation and its durable event intent commit in the same PostgreSQL transaction, avoiding a database/broker dual-write gap.
- **At-least-once-safe processing:** RabbitMQ delivery may duplicate; durable consumer receipts and idempotent persistence make redelivery safe without claiming exactly-once semantics.
- **Deterministic triage first:** severity-to-priority mapping is authoritative and reproducible; AI cannot change priority, incident state, or human-review requirements.
- **Advisory AI:** optional Gemini enrichment is persisted separately and can fail without making incident intake or triage unavailable.
- **Human-in-the-loop remediation:** proposal, approval/rejection, and execution request are distinct steps; a proposer cannot approve their own proposal.
- **Ambiguous-effect safety:** remediation distinguishes `outcome_unknown` from failure and success instead of blindly retrying a potentially completed external side effect.
- **Observability:** structured logs, bounded-label Prometheus metrics, and OpenTelemetry traces span the API and asynchronous workers, with a local Grafana/Tempo stack.
- **Operator console:** Angular provides RBAC-aware incident and remediation workflows, responsive accessibility polish, and production packaging behind an Nginx same-origin `/api` proxy.

## Technology stack

| Area | Implemented technology |
| --- | --- |
| Backend | Python 3.13, FastAPI, async SQLAlchemy, Alembic, Pydantic settings |
| Persistence | PostgreSQL 18 |
| Messaging | RabbitMQ direct exchange, durable queues, publisher confirms |
| AI | Gemini structured advisory enrichment |
| Frontend | Angular, TypeScript, Angular Material, RxJS |
| Observability | OpenTelemetry, Prometheus, Tempo, Grafana, structured JSON logs |
| Testing & quality | pytest, PostgreSQL/RabbitMQ integration tests, Ruff, mypy, Vitest, Angular production build checks |
| Packaging & CI | Podman/Docker Compose, multi-stage frontend/backend images, Nginx, GitHub Actions |

## Architecture at a glance

```mermaid
flowchart LR
    UI["Angular operator console"] --> API["FastAPI<br/>auth + RBAC + domain APIs"]
    Client["API client"] --> API

    API --> DB[(PostgreSQL)]
    DB -->|"transactional outbox"| Dispatcher["Outbox dispatcher"]
    Dispatcher --> MQ[(RabbitMQ)]

    MQ --> Consumer["Incident consumer"]
    Consumer -->|"persist receipt + triage"| DB
    Consumer --> Triage["Deterministic triage<br/>authoritative"]

    MQ --> Enrichment["Enrichment worker"]
    Enrichment --> Gemini[Gemini]
    Gemini -->|"advisory result"| Enrichment
    Enrichment --> DB

    API --> Review["Human review<br/>proposal + approve / reject"]
    Review -->|"separate admin execute request"| DB
    MQ --> Remediation["Remediation worker"]
    Remediation -->|"allowlisted target only"| Actuator["Trusted restart endpoint"]
    Remediation -->|"durable outcome"| DB
```

The asynchronous path uses a PostgreSQL transactional outbox, RabbitMQ, and idempotent workers. Deterministic triage remains authoritative; AI enrichment is advisory. Remediation requires human review and a separate execution request before the opt-in worker can call an allowlisted endpoint.

For the deeper design, failure semantics, and intentional non-goals, see [docs/architecture.md](docs/architecture.md).

## Current scope

**Implemented**

- JWT authentication and database-backed RBAC for `viewer`, `operator`, and `admin`
- incident creation, filtered pagination, detail, and lifecycle `open -> acknowledged -> resolved`
- transactional outbox with lease-based dispatch and RabbitMQ publisher confirms
- durable idempotent incident consumption and deterministic triage
- optional Gemini advisory enrichment with persisted snapshots
- remediation proposals, proposer/approver separation, approval/rejection, and controlled admin execution requests
- durable remediation outcomes: `requested`, `in_progress`, `succeeded`, `failed`, and `outcome_unknown`
- structured logging, Prometheus metrics, distributed tracing, Tempo, and Grafana
- Angular operations console with overview, incidents, remediation, accessibility/responsive polish, and production Nginx packaging
- GitHub Actions quality gates for backend, frontend, and packaged frontend runtime smoke tests

**Possible future work**

- RAG-backed runbook retrieval that remains advisory to deterministic state
- public cloud deployment and environment-specific infrastructure
- Kubernetes/Helm/Terraform only if a real deployment need justifies them
- stronger production alerting and centralized log aggregation

These are future directions, not current capabilities.

## Documentation

- [Architecture and design boundaries](docs/architecture.md)
- [5–8 minute portfolio demo walkthrough](docs/demo.md)
- [Frontend-specific development and packaging notes](frontend/README.md)

## Run the demo locally

The packaged demo uses Podman Compose or Docker Compose and keeps the browser on one origin through Nginx. Migrations and user bootstrap are deliberately explicit.

```sh
cp .env.example .env
openssl rand -hex 32
# Copy the generated value into SIGNALFORGE_JWT_SECRET in .env.

podman compose --profile demo build backend frontend
podman compose up -d postgres
podman compose run --rm backend alembic upgrade head
podman compose run --rm backend python -m signalforge.users.create_user \
  --email admin@example.com \
  --role admin
podman compose --profile demo up -d
```

Open <http://127.0.0.1:8080>. Replace `podman compose` with `docker compose` for the equivalent Docker workflow. The frontend uses relative `/api/...` requests, which Nginx proxies to FastAPI inside the Compose network, so normal UI use needs no CORS workaround.

The base demo does not require AI, remediation execution, or observability. Add optional profiles only when needed:

- `--profile ai` for Gemini enrichment
- `--profile remediation` for the controlled remediation worker and a trusted restart-endpoint allowlist
- `--profile observability` for Collector, Tempo, Prometheus, and Grafana

See [Container workflow](#container-workflow) for the full runtime contract and [docs/demo.md](docs/demo.md) for the recommended presentation sequence.

## Contents

- [Prerequisites](#prerequisites)
- [Local setup](#local-setup)
- [Frontend development](#frontend-development)
- [Container workflow](#container-workflow)
- [Remediation proposal API](#remediation-proposal-api)
- [Event pipeline and AI enrichment](#event-pipeline-and-ai-enrichment)
- [Operational logging](#operational-logging)
- [Metrics](#metrics)
- [Distributed tracing](#distributed-tracing)
- [Local observability](#local-observability)
- [Local authentication](#local-authentication)
- [Database migrations](#database-migrations)
- [Quality checks](#quality-checks)
- [License](#license)

## Prerequisites

- Python 3.13 (managed automatically by `uv` when configured to do so)
- [uv](https://docs.astral.sh/uv/)
- Podman with a Compose provider, or Docker with Compose

For direct frontend development, use Node.js 24.15.0 or a later 24.x release and npm 11.13.0 or a later 11.x release.

## Local setup

Create the local environment file and install the locked backend dependencies:

```sh
cp .env.example .env
openssl rand -hex 32
cd backend
uv sync --locked
```

Copy the generated value into `SIGNALFORGE_JWT_SECRET` in `.env`. Do not commit the populated file. Environment variables take precedence over values in `.env`.

Start PostgreSQL from the repository root:

```sh
podman compose up -d postgres
podman compose ps
```

Docker users can replace `podman compose` with `docker compose` throughout this document.

From `backend/`, apply database migrations explicitly:

```sh
uv run alembic upgrade head
```

There is no public registration endpoint. Bootstrap a user when needed; the command prompts for the password twice and never accepts it as a command-line argument:

```sh
uv run python -m signalforge.users.create_user \
  --email admin@example.com \
  --role admin
```

Start the API for direct host development:

```sh
uv run uvicorn signalforge.main:app --app-dir src --reload
```

The API is available at <http://127.0.0.1:8000>, with interactive documentation at <http://127.0.0.1:8000/docs>.

## Frontend development

The Angular application lives in `frontend/`. It provides authentication/session restoration, Operations Overview, Incident queue/detail/lifecycle actions, deterministic triage display, advisory enrichment display, and the remediation review/execution workflow.

With the backend running at <http://127.0.0.1:8000>:

```sh
cd frontend
npm ci
npm start
```

Open <http://localhost:4200>. The Angular development proxy forwards relative `/api/...` requests to the local backend. The application itself does not hardcode a browser-facing backend origin.

Run the frontend quality gates with:

```sh
npm run test:ci
npm run build
```

The production image uses the normal Angular production artifact and serves it through Nginx with SPA fallback, `/api` reverse proxying, and `/healthz` liveness.

## Container workflow

The backend and frontend images are reproducible runtimes without source bind mounts or development servers. The default Compose stack remains backend-oriented; the Angular frontend is opt-in through `--profile demo`.

First-time containerized demo setup from the repository root:

```sh
cp .env.example .env
openssl rand -hex 32
# Copy the generated value into SIGNALFORGE_JWT_SECRET in .env.

podman compose --profile demo build backend frontend
podman compose up -d postgres
podman compose run --rm backend alembic upgrade head
podman compose run --rm backend python -m signalforge.users.create_user \
  --email admin@example.com \
  --role admin
podman compose --profile demo up -d
podman compose ps
```

On subsequent starts:

```sh
podman compose --profile demo up -d
```

The packaged console is available at <http://127.0.0.1:8080> by default, or the loopback port configured through `FRONTEND_PORT`. FastAPI remains separately available on port 8000 by default for Swagger, direct debugging, and backend health endpoints.

`GET /healthz` on the frontend origin checks only Nginx liveness. Backend readiness remains `GET /health/ready` on FastAPI. Migrations are never run automatically by frontend, backend, PostgreSQL, or worker startup. Compose also does not create a default user.

The default five-service core stack is:

1. PostgreSQL
2. FastAPI backend
3. RabbitMQ
4. outbox dispatcher
5. incident consumer

Optional profiles are independent and composable:

- `demo` adds the Angular/Nginx frontend
- `ai` adds advisory Gemini enrichment
- `remediation` adds the controlled execution worker
- `observability` adds the local telemetry stack

For example:

```sh
podman compose --profile demo --profile ai up -d
```

RabbitMQ accepts AMQP connections on <amqp://127.0.0.1:5672> and exposes its management UI at <http://127.0.0.1:15672> by default. Local example credentials are development-only.

Worker availability is not part of API readiness. If RabbitMQ or a worker is unavailable, committed incidents and outbox events remain durable until asynchronous processing resumes.

This Compose topology is a local demo/development contract, not internet-facing production infrastructure.

## API and incident lifecycle

The core API includes:

- `POST /api/v1/auth/token` — authenticate email/password
- `GET /api/v1/auth/me` — resolve the authenticated user
- `POST /api/v1/incidents` — create an incident; `operator` or `admin`
- `GET /api/v1/incidents` — filtered, paginated incident list; all authenticated roles
- `GET /api/v1/incidents/{incident_id}` — incident detail
- `POST /api/v1/incidents/{incident_id}/acknowledge` — acknowledge an open incident; `operator` or `admin`
- `POST /api/v1/incidents/{incident_id}/resolve` — resolve an acknowledged incident; `operator` or `admin`
- `GET /api/v1/incidents/{incident_id}/triage` — persisted deterministic triage
- `GET /api/v1/triage` — filtered, paginated triage list
- `GET /api/v1/incidents/{incident_id}/enrichments` — persisted advisory enrichment snapshots
- `GET /api/v1/enrichments` — global advisory enrichment list

Incident list filters support `status`, `severity`, `source`, `occurred_from`, and `occurred_to`, plus `limit`/`offset` pagination. Results are ordered by `occurred_at` descending and then `id` descending.

The incident lifecycle is deliberately narrow and server-authoritative:

```text
open -> acknowledged -> resolved
```

Arbitrary status editing and reopening are not exposed. Invalid transitions return `409 Conflict`, and transitions persist the authenticated actor and timestamp.

## Remediation proposal API

The supported remediation action is currently `restart_service`. A proposal contains an Incident ID, a logical target, and a reason. The authenticated user is always recorded as proposer; attribution fields cannot be supplied by the client.

Role policy:

| Role | Read | Create | Reject / withdraw | Approve | Request execution |
| --- | --- | --- | --- | --- | --- |
| `viewer` | Yes | No | No | No | No |
| `operator` | Yes | Yes | Own pending proposal only | No | No |
| `admin` | Yes | Yes | Any pending proposal | Another user's pending proposal | Approved proposal |

Relevant endpoints:

- `POST /api/v1/remediation-proposals`
- `GET /api/v1/remediation-proposals`
- `GET /api/v1/remediation-proposals/{proposal_id}`
- `POST /api/v1/remediation-proposals/{proposal_id}/approve`
- `POST /api/v1/remediation-proposals/{proposal_id}/reject`
- `POST /api/v1/remediation-proposals/{proposal_id}/execute`
- `GET /api/v1/remediation-proposals/{proposal_id}/execution`

Approval only records the human decision. It does **not** restart a service, publish an execution event, or invoke AI. An admin must separately request execution. That request atomically commits one `requested` execution row and one `remediation.execution.requested` outbox event, then returns `202 Accepted`.

The opt-in remediation worker consumes the durable RabbitMQ execution queue and resolves the logical target through an operator-configured allowlist of fixed HTTP(S) endpoints. Proposal targets are never interpreted as arbitrary URLs or commands. The allowlist defaults to empty and the worker fails closed.

```env
SIGNALFORGE_REMEDIATION_RESTART_ENDPOINTS={"checkout-api":"http://checkout-control:8080/internal/restart"}
```

The value above is a format example only. Use intentionally trusted local/test endpoints when demonstrating execution.

The worker never automatically repeats the actuator call. A timeout, transport ambiguity, cancellation, or stale in-progress attempt can become `outcome_unknown` because the external side effect may have happened even when SignalForge cannot prove the result. No database transaction is held during actuator HTTP I/O.

Run the worker only after configuring a non-empty trusted allowlist:

```sh
podman compose --profile remediation up -d remediation-worker
```

The normal default stack starts no remediation worker and causes no remediation side effects.

## Event pipeline and AI enrichment

Successful incident creation atomically commits both the Incident and an immutable `incident.created` outbox intent in PostgreSQL. Publication is outside the API request path. The Incident table remains the source of truth; the outbox is delivery infrastructure, not event sourcing.

The dispatcher uses lease-based ownership, mandatory routing, publisher confirms, bounded publication timeouts, and short database transactions. It never holds PostgreSQL locks while waiting on RabbitMQ. Ambiguous publication can produce duplicates, so the transport contract is at-least-once rather than exactly-once.

The incident consumer validates `incident.created` v1 messages and atomically records a durable processing receipt plus deterministic triage. Severity maps directly to priority:

| Severity | Priority | Human review |
| --- | --- | --- |
| `critical` | P1 | Required |
| `high` | P2 | No |
| `medium` | P3 | No |
| `low` | P4 | No |

The transaction commits before broker ACK. A redelivery after ACK loss is therefore recognized as a duplicate and cannot recreate or mutate triage.

First-time triage also creates a durable `triage.enrichment.requested` outbox event. The optional enrichment worker consumes those requests and calls the configured Gemini model with schema-constrained structured output. Deterministic priority and review requirements are fixed context, not AI outputs.

The provider call occurs without holding a database transaction. A successful result is persisted atomically with the enrichment worker's durable receipt before ACK. Full prompts, raw model responses, API keys, and secrets are not persisted.

AI remains advisory. It cannot change priority, review requirements, incident state, or trigger remediation. API readiness and deterministic triage remain independent of Gemini and the enrichment worker.

Run the optional worker with:

```sh
podman compose --profile ai up -d
```

The end-to-end path is:

```text
API
  -> PostgreSQL incident + transactional outbox
  -> dispatcher
  -> RabbitMQ
  -> incident consumer
  -> deterministic triage + durable receipt
  -> enrichment outbox
  -> dispatcher
  -> RabbitMQ
  -> enrichment worker
  -> Gemini
  -> persisted advisory enrichment
```

For the reasoning behind these boundaries, see [docs/architecture.md](docs/architecture.md).

## Operational logging

The API, dispatcher, incident consumer, enrichment worker, and remediation worker emit structured JSON logs with explicit service identity and a small allowlisted field set.

The API accepts a canonical UUID in `X-Request-ID`, generates one when missing or invalid, returns the effective value in the response, and includes it in completion logs. RabbitMQ handling logs correlate bounded event metadata after validated decoding.

Authorization values, credentials, connection URLs, request/message bodies, Gemini prompts and responses, and raw known exception strings are intentionally omitted.

## Metrics

The API exposes Prometheus-format metrics at <http://127.0.0.1:8000/metrics>. HTTP request counters and duration histograms use method, route template, and bounded status/result labels rather than IDs or payload data.

Standalone workers expose process-local metrics on loopback in Compose:

- <http://127.0.0.1:9101/metrics> — dispatcher
- <http://127.0.0.1:9102/metrics> — incident consumer
- <http://127.0.0.1:9103/metrics> — enrichment worker when `ai` is enabled
- <http://127.0.0.1:9104/metrics> — remediation worker when `remediation` is enabled

Metric labels are code-owned and bounded. Incident IDs, target names, endpoint URLs, users, payloads, and raw error strings are not used as labels.

## Distributed tracing

OpenTelemetry instrumentation covers the API, dispatcher, incident consumer, enrichment worker, and remediation worker. Tracing is opt-in and uses OTLP/HTTP.

The incident/enrichment lineage is:

```text
API SERVER
  -> durable incident.created context
  -> RabbitMQ PRODUCER
  -> Incident CONSUMER PROCESS
  -> durable triage.enrichment.requested context
  -> RabbitMQ PRODUCER
  -> Enrichment CONSUMER PROCESS
  -> Gemini CLIENT
```

Remediation follows a separate lineage beginning only after the explicit admin execute request:

```text
Admin execute API SERVER
  -> durable remediation.execution.requested context
  -> RabbitMQ PRODUCER
  -> Remediation CONSUMER PROCESS
  -> actuator HTTP CLIENT, only when an actual allowlisted call occurs
```

Prompts, model responses, credentials, endpoint URLs, target names, and business identifiers are intentionally excluded from sensitive span attributes. At-least-once delivery also applies to telemetry: retries and redeliveries can create additional spans.

## Local observability

The optional `observability` Compose profile adds OpenTelemetry Collector, Tempo, Prometheus, and Grafana without changing business correctness or readiness.

```sh
SIGNALFORGE_TRACING_ENABLED=true \
  podman compose --profile observability up -d
```

Combine profiles when useful, for example:

```sh
SIGNALFORGE_TRACING_ENABLED=true \
  podman compose --profile observability --profile ai up -d
```

Local endpoints:

- SignalForge API: <http://127.0.0.1:8000>
- Grafana: <http://127.0.0.1:3000>
- Prometheus: <http://127.0.0.1:9090>
- Collector OTLP/HTTP: <http://127.0.0.1:4318>

Grafana provisions Prometheus as the default metrics source, Tempo for trace exploration, and a **SignalForge Overview** dashboard. Structured logs remain on container standard output; centralized logs and production alerting are not part of the current scope.

## Local authentication

Passwords are hashed with Argon2. Access tokens are signed JWTs containing the user UUID and issued-at/expiration timestamps, with a 30-minute default lifetime.

Tokens deliberately do not contain role claims. Every authenticated request loads the current user from PostgreSQL, so role or active-state changes apply without waiting for token expiry.

Authenticate with the OAuth2 password form using the email address in the `username` field:

```sh
curl -X POST http://127.0.0.1:8000/api/v1/auth/token \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'username=admin@example.com' \
  --data-urlencode 'password=your-password'
```

Read the current user with:

```sh
curl http://127.0.0.1:8000/api/v1/auth/me \
  -H 'Authorization: Bearer your-access-token'
```

Authentication failures return `401` with `WWW-Authenticate: Bearer`; authenticated users without the required role receive `403`.

## Database migrations

Alembic manages the PostgreSQL schema. Apply migrations explicitly from `backend/`:

```sh
uv run alembic upgrade head
```

Migration startup is intentionally not hidden inside API or worker entrypoints.

## Quality checks

Backend CI uses a dedicated PostgreSQL database named `signalforge_test` and a dedicated RabbitMQ test vhost. The test suite rejects unsafe database fallbacks before opening a connection.

A representative local backend test invocation is:

```sh
SIGNALFORGE_DATABASE_URL=postgresql+asyncpg://signalforge:password@localhost:5432/signalforge_test \
  uv run pytest
```

Static quality checks:

```sh
uv run ruff format --check .
uv run ruff check .
uv run mypy src
```

Frontend checks from `frontend/`:

```sh
npm ci
npm run test:ci
npm run build
```

GitHub Actions also builds the production frontend container and smoke-tests Nginx `/healthz` plus Angular SPA deep links.

Stop local Compose services with `podman compose down` or `docker compose down`. Named volumes preserve PostgreSQL and RabbitMQ data; add `--volumes` only when intentionally deleting local data.

## License

SignalForge is released under the [MIT License](LICENSE).
