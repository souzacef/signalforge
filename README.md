# SignalForge

SignalForge is a portfolio project for exploring incident automation and
AI-assisted operations. Its current backend provides an authenticated Incident
API backed by PostgreSQL, with role-based access control, deterministic lifecycle
transitions, filtered paginated listing, and durable asynchronous publication and
idempotent consumption of new-Incident events through RabbitMQ.

The application begins as a modular monolith. This keeps deployment and local
development straightforward while domain boundaries are still emerging, without
preventing modules from being separated later when real operational needs justify
it.

RAG, metrics, distributed tracing, observability dashboards, Angular, Kubernetes,
Helm, Terraform, and AWS are planned directions. They are not implemented in
this phase.

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

Start only PostgreSQL from the repository root with either runtime:

```sh
podman compose up -d postgres
podman compose ps
```

```sh
docker compose up -d postgres
docker compose ps
```

From `backend/`, apply the database migrations:

```sh
uv run alembic upgrade head
```

There is no public registration endpoint. Bootstrap a user when needed; the
command prompts for the password twice without accepting it as a command-line
argument:

```sh
uv run python -m signalforge.users.create_user \
  --email admin@example.com \
  --role admin
```

Then start the API:

```sh
uv run uvicorn signalforge.main:app --app-dir src --reload
```

This direct host-development workflow remains supported and uses Uvicorn reload
for local iteration.

## Container workflow

The backend image is a reproducible application runtime without source bind
mounts or development reload. Before using Compose, set a generated
`SIGNALFORGE_JWT_SECRET` in `.env`; Compose rejects an unset or empty value.

The commands below use Podman. Replace `podman compose` with `docker compose`
for the equivalent Docker workflow:

```sh
podman compose build backend
podman compose up -d postgres
podman compose run --rm backend alembic upgrade head
podman compose up -d
podman compose ps
```

Migrations are an explicit one-shot command and are never run by any service at
startup. The same application image supplies the default API command, the
Alembic CLI, and the standalone dispatcher, consumer, and enrichment-worker
commands.

The API is available at <http://127.0.0.1:8000> by default. Set
`BACKEND_PORT` to change the published host port. Compose connects the backend
to PostgreSQL through the `postgres` service hostname, while direct host
development continues to use the URL from `.env`.

The default local five-service core stack consists of PostgreSQL, the FastAPI
backend, RabbitMQ, the standalone dispatcher, and the standalone consumer. An
optional sixth enrichment-worker service is available through the `ai` Compose
profile. RabbitMQ accepts AMQP connections on
<amqp://127.0.0.1:5672> and exposes its management UI at
<http://127.0.0.1:15672> by default. `RABBITMQ_PORT` and
`RABBITMQ_MANAGEMENT_PORT` override those loopback-only host ports. The example
broker credentials are local-development defaults, not production-safe secrets;
the broker vhost is `signalforge`.

The workers run independently from FastAPI and are not part of API readiness. If
RabbitMQ or a worker is unavailable, Incident creation still commits the Incident
and its outbox event. Pending outbox events are published when the dispatcher and
RabbitMQ return; published queue messages remain durable until a consumer handles
them. Worker runtimes reconnect after expected RabbitMQ outages without requiring
an automatic container restart policy.

This is a local container runtime contract, not production deployment
infrastructure.

The API exposes:

- `GET /health/live` — process liveness only; it never checks PostgreSQL.
- `GET /health/ready` — returns `200` when PostgreSQL answers a minimal query,
  otherwise `503` without exposing connection details.
- `POST /api/v1/incidents` — creates an incident with initial status `open`;
  requires an `operator` or `admin` bearer token.
- `GET /api/v1/incidents` — lists and filters incidents with offset pagination;
  available to all authenticated roles.
- `GET /api/v1/incidents/{incident_id}` — retrieves an incident by UUID;
  requires a `viewer`, `operator`, or `admin` bearer token.
- `POST /api/v1/incidents/{incident_id}/acknowledge` — acknowledges an open
  incident; requires an `operator` or `admin` bearer token.
- `POST /api/v1/incidents/{incident_id}/resolve` — resolves an acknowledged
  incident; requires an `operator` or `admin` bearer token.
- `GET /api/v1/incidents/{incident_id}/enrichments` — lists persisted advisory
  enrichment snapshots for one Incident; available to all authenticated roles.
- `GET /api/v1/enrichments` — lists persisted advisory enrichment snapshots
  globally; available to all authenticated roles.
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

List incidents with any combination of `status`, `severity`, `source`,
`occurred_from`, and `occurred_to` filters, plus `limit` and `offset`
pagination parameters:

```sh
curl 'http://127.0.0.1:8000/api/v1/incidents?status=open&limit=20&offset=0' \
  -H 'Authorization: Bearer your-access-token'
```

Filters combine with `AND`; source matching is exact after trimming surrounding
query whitespace. Results are ordered by `occurred_at` descending, then `id`
descending. The response `total` is the number of matching incidents before
pagination. Full-text search is not implemented.

Successful new Incident creation atomically commits the Incident and one durable
`incident.created` v1 outbox intent in PostgreSQL. The intent stores an immutable
creation snapshot; publication is not in the API request path. Historical
incidents are not backfilled, and lifecycle transitions
do not emit events yet. The Incident table remains the source of truth; the outbox
is delivery infrastructure, not event sourcing.

Pending outbox intents support PostgreSQL lease-based ownership and retry
scheduling. Each committed claim increments the attempt count, even if its owner
stops before using it; rolled-back claims do not count. Claim, settlement, and
release helpers leave commit/rollback to the caller's short database transaction.
Release preserves the due time, making already-due work immediately eligible.
RabbitMQ publisher/topology support reconstructs the stored `incident.created` v1
envelope and sends persistent JSON messages with mandatory routing and publisher
confirms. In a caller-provisioned vhost (normally `signalforge`), it declares the
durable direct exchange `signalforge.events`, durable classic queue
`signalforge.incident-events`, and `incident.created` binding.

The publisher does not query or settle PostgreSQL state. Its robust connection is
reused across publications; a timeout or ambiguous transport failure retires the
publisher, requiring the caller to create a replacement. Publication has an
explicit timeout covering readiness and confirmation; failure cleanup can add up
to two bounded resource-close budgets (five seconds each by default).

One-shot dispatch orchestration can claim a batch, commit those claims, and process
each event sequentially through preflight ownership, confirmed publication, and
conditional settlement. Each preflight and settlement uses a fresh short database
transaction, so PostgreSQL locks are never held while awaiting RabbitMQ. Retries
preserve the durable event ID. An invalid stored event is retained with a visible
safe error code and a 24-hour retry delay for operator intervention. An ambiguous
publication stops the batch and conditionally releases its unstarted claims.

The standalone dispatcher can be run independently of FastAPI after setting
`SIGNALFORGE_RABBITMQ_URL`:

```sh
uv run python -m signalforge.outbox.dispatcher
```

It performs bounded polling, reconnects to RabbitMQ with bounded exponential
backoff and jitter, and handles `SIGTERM`/`SIGINT` through graceful shutdown. An
active batch may drain for the configured timeout; forced cancellation then relies
on claim-lease expiration for recovery. A publication interrupted this way may
have an ambiguous broker outcome, unprocessed events may remain leased until
expiration, and a later attempt can create a duplicate if publication succeeded
before cancellation.

Delivery is at-least-once: ambiguous outcomes and publish/settlement crash windows
can cause duplicates. Claims fence database settlement, not external delivery,
and do not provide exactly-once delivery. Root Compose runs the dispatcher as a
separate process from the same application image as FastAPI.

The consumer validates `incident.created` v1 messages and, on first processing,
atomically records both a durable consumer receipt and a deterministic incident
triage snapshot. Severity maps directly to priority (`critical` -> P1, `high` ->
P2, `medium` -> P3, `low` -> P4); critical incidents require human review and
the other severities do not. The transaction commits before ACK, so broker
redelivery after ACK loss is safely treated as a duplicate and neither creates
nor mutates triage. Malformed and unsupported messages are terminally rejected
and discarded because no DLQ exists yet; transient database failures are
requeued.

Authenticated viewers, operators, and administrators can read persisted triage
through `GET /api/v1/incidents/{incident_id}/triage` and `GET /api/v1/triage`.
The list endpoint uses limit/offset pagination and supports exact `priority`,
`original_severity`, `requires_human_review`, and `source` filters plus
inclusive `created_from` and `created_to` bounds. Results are ordered by
`created_at` descending and then `incident_id` descending.

These endpoints remain read-only and deterministic-only. They report the source,
original severity, priority, review requirement, event ID, and creation time
persisted from the event snapshot; values are not recalculated from current
Incident state. Advisory AI enrichment remains a separate resource.

First-time deterministic triage also creates a durable
`triage.enrichment.requested` v1 outbox event. Its own event ID is linked to the
triggering `incident.created` event through `trigger_event_id`, and its payload
captures only the Incident event snapshot plus the deterministic triage decision.
The processing receipt, triage row, and enrichment request commit in one database
transaction; duplicate Incident delivery creates no additional request.

The existing confirmed dispatcher validates and publishes these requests through
the direct `signalforge.events` exchange to the durable
`signalforge.triage-enrichment` queue. D3b1 adds a one-message processor foundation
that calls configurable Gemini model `SIGNALFORGE_GEMINI_MODEL` (default
`gemini-3.8-flash`) with schema-constrained structured output; its API key is read
from `SIGNALFORGE_GEMINI_API_KEY`. The incident snapshot is the only model input,
and deterministic priority and review requirements are fixed context, not AI
outputs. Full prompts, raw provider responses, and secrets are not persisted.

The processor performs the model call without holding a database transaction, then
atomically commits the `triage-enrichment-consumer` receipt and one advisory
`triage_enrichments` result before ACK. A committed redelivery is detected by a
cheap receipt preflight and does not call Gemini again. Concurrent duplicates may
both invoke Gemini before the database race is decided, but exactly one durable
result persists; model invocation is not exactly-once. Provider or persistence
failures cannot affect Incident creation, deterministic triage, API readiness, or
the existing `incident.created` ACK path.

Malformed or unusable AI responses are retriable and are NACKed for requeue
without a receipt or result. Deterministic provider request or configuration
rejection is terminal at the one-message handler layer. The long-running worker
applies bounded, interruptible backoff between transient attempts, but there is no
durable bounded retry count or enrichment DLQ yet; transient messages may be
retried repeatedly.

Run the standalone enrichment worker independently of FastAPI after migrations
have been applied and the RabbitMQ URL and Gemini API key have been set:

```sh
uv run python -m signalforge.enrichment.runtime
```

The worker consumes only `signalforge.triage-enrichment`, uses one Gemini provider
instance across messages and broker reconnects, and defaults to sequential
processing with prefetch one. It reconnects to RabbitMQ with bounded backoff and
uses a separate bounded backoff after transient provider or database failures.
`SIGTERM` and `SIGINT` stop new work, allow the active delivery up to the configured
drain timeout, then force cancellation and close broker and database resources.
It never runs migrations automatically.

The Compose service is deliberately opt-in. Put a real
`SIGNALFORGE_GEMINI_API_KEY` in `.env`, then add the worker to the core stack with:

```sh
podman compose --profile ai up -d
```

Normal `podman compose up -d` does not start the enrichment worker and does not
require Gemini credentials. Starting the `ai` profile without a key fails only the
enrichment worker settings validation; the deterministic core remains available.
AI output remains advisory. It cannot change priority, review requirements,
Incident state, or trigger remediation.

Persisted enrichment results are readable through
`GET /api/v1/incidents/{incident_id}/enrichments` and
`GET /api/v1/enrichments` without invoking Gemini. Both endpoints return
historical advisory snapshots ordered by creation time and request event ID
descending, with `limit` and `offset` pagination. The global list supports exact
`incident_id`, `category`, `provider`, and `model` filters plus inclusive
`created_from` and `created_to` bounds; the Incident-scoped list supports the same
filters except `incident_id`. Text filters are trimmed and exact.

Multiple results per Incident are supported, and an existing Incident with no
completed result returns an empty collection. The HTTP request path never calls
Gemini or inspects worker, broker, outbox, or processing state. API availability
remains independent of Gemini and the enrichment worker; deterministic triage
remains separate and authoritative.

Run the standalone consumer independently of FastAPI after migrations have been
applied and `SIGNALFORGE_RABBITMQ_URL` has been set:

```sh
uv run python -m signalforge.consumers.runtime
```

The runtime consumes the durable `signalforge.incident-events` queue sequentially
with a default prefetch count of one and delegates each delivery to the durable,
idempotent handler above. PostgreSQL and RabbitMQ outages are retried with bounded
exponential backoff and jitter. Poison messages are safely logged and consumption
continues; `SIGTERM` and `SIGINT` stop new work and allow an active delivery a
bounded drain period. Root Compose runs the consumer as a fifth service from the
same non-root application image used by the API and dispatcher.

The complete transport path is:

```text
API -> transactional outbox -> dispatcher -> RabbitMQ -> consumer -> processed_events + incident_triage
incident_triage -> transactional outbox -> dispatcher -> RabbitMQ -> enrichment-worker -> triage_enrichments
```

This combines a transactional producer outbox, at-least-once RabbitMQ delivery,
and durable consumer idempotency. Duplicate delivery is expected and safe; it is
not exactly-once delivery. Malformed or unsupported messages are currently
rejected without requeue and discarded because no DLQ exists yet.
Triage remains the authoritative deterministic baseline derived from the event
snapshot. The optional enrichment worker persists advisory output but adds no
AI-driven priority or remediation.

Publication remains outside the API request path, and API readiness remains
PostgreSQL-only.

## Operational logging

The API, dispatcher, Incident consumer, and enrichment worker emit one structured
JSON object per log line to standard output or standard error for external
container collection. Each process sets an explicit service identity, and records
use stable event names plus a small allowlisted field set rather than dumping
arbitrary objects.

The API accepts a canonical UUID in `X-Request-ID`, generates a UUID when the
header is absent or invalid, returns the effective ID in the response, and includes
it in the request-completion log. RabbitMQ delivery logs correlate existing event
metadata and, after validated decoding, Incident and trigger identifiers. Context
is task-local and reset after every request or delivery.

Authorization values, credentials, connection URLs, request and message bodies,
Gemini prompts and responses, and raw known exception strings are intentionally
omitted. Metrics, distributed tracing, and a local dashboard or log aggregation
stack remain future Phase 4 work; this logging slice does not make telemetry an API
readiness dependency.

## Local authentication

Passwords are hashed with Argon2 and access tokens are signed JWTs containing
only the user UUID and issued-at/expiration timestamps. Tokens expire after 30
minutes by default; configure `SIGNALFORGE_ACCESS_TOKEN_EXPIRE_MINUTES` to
change that duration.

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

Run all tests (including the PostgreSQL integration tests) against a dedicated
database named `signalforge_test`. Set `SIGNALFORGE_DATABASE_URL` explicitly;
the test suite rejects dotenv fallbacks and other database names before opening
a connection:

```sh
SIGNALFORGE_DATABASE_URL=postgresql+asyncpg://signalforge:password@localhost:5432/signalforge_test \
  uv run pytest
```

The same safety guard applies when selecting only fast tests:

```sh
SIGNALFORGE_DATABASE_URL=postgresql+asyncpg://signalforge:password@localhost:5432/signalforge_test \
  uv run pytest -m "not integration"
```

### RabbitMQ publisher tests

Broker tests require a dedicated RabbitMQ `4.3.5-management-alpine` broker in
addition to the explicit `signalforge_test` PostgreSQL URL above. They create and
delete a unique `signalforge_test_...` vhost per test, using management permissions.
Without `SIGNALFORGE_TEST_RABBITMQ_URL`, these broker tests are explicitly skipped;
if configured, connection or configuration failures fail the tests.

For disposable local validation (not permanent Compose wiring):

```sh
export RABBITMQ_DEFAULT_USER=signalforge_test
export RABBITMQ_DEFAULT_PASS="$(openssl rand -hex 24)"
export RABBITMQ_DEFAULT_VHOST=signalforge_test_local
podman run -d --name signalforge-rabbitmq-test --hostname signalforge-rabbitmq-test \
  -p 127.0.0.1:5673:5672 -p 127.0.0.1:15673:15672 \
  -e RABBITMQ_DEFAULT_USER -e RABBITMQ_DEFAULT_PASS -e RABBITMQ_DEFAULT_VHOST \
  -v signalforge-rabbitmq-test-data:/var/lib/rabbitmq \
  rabbitmq:4.3.5-management-alpine
podman exec signalforge-rabbitmq-test rabbitmq-diagnostics -q check_running
```

Wait until the readiness command succeeds, then from `backend/`, retaining the
explicit PostgreSQL test URL in the environment:

```sh
export SIGNALFORGE_TEST_RABBITMQ_URL="amqp://${RABBITMQ_DEFAULT_USER}:${RABBITMQ_DEFAULT_PASS}@127.0.0.1:5673/${RABBITMQ_DEFAULT_VHOST}"
export SIGNALFORGE_TEST_RABBITMQ_MANAGEMENT_URL=http://127.0.0.1:15673
uv run pytest
```

Remove only these disposable broker resources after validation; this deletes their
test data. Do not use these commands against a developer broker:

```sh
podman rm -f signalforge-rabbitmq-test
podman volume rm signalforge-rabbitmq-test-data
unset SIGNALFORGE_TEST_RABBITMQ_URL SIGNALFORGE_TEST_RABBITMQ_MANAGEMENT_URL
unset RABBITMQ_DEFAULT_USER RABBITMQ_DEFAULT_PASS RABBITMQ_DEFAULT_VHOST
```

Formatting, linting, and type checking:

```sh
uv run ruff format --check .
uv run ruff check .
uv run mypy src
```

To apply formatting, run `uv run ruff format .`.

Stop the local stack from the repository root with either `podman compose down`
or `docker compose down`. Named volumes preserve PostgreSQL and RabbitMQ data;
add `--volumes` only when you intentionally want to delete local data.
