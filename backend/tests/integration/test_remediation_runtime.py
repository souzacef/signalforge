from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote
from uuid import UUID, uuid4

import aio_pika
import pytest
from aio_pika import DeliveryMode, Message
from httpx import AsyncClient
from pamqp.commands import Basic
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker
from yarl import URL

from signalforge.consumers.models import ProcessedEvent
from signalforge.core.config import RemediationExecutionSettings
from signalforge.db.session import engine
from signalforge.incidents.models import Incident, IncidentSeverity
from signalforge.outbox.rabbitmq import (
    REMEDIATION_EXECUTION_QUEUE_NAME,
    REMEDIATION_EXECUTION_ROUTING_KEY,
    declare_topology,
)
from signalforge.remediation import consumer, runtime
from signalforge.remediation.events import (
    RemediationExecutionRequested,
    RemediationExecutionRequestedPayload,
)
from signalforge.remediation.execution import (
    RemediationCommand,
    RemediationExecutionOutcome,
    RemediationExecutionResult,
)
from signalforge.remediation.models import (
    RemediationActionKind,
    RemediationExecution,
    RemediationExecutionAttempt,
    RemediationExecutionAttemptStatus,
    RemediationExecutionStatus,
    RemediationProposal,
    RemediationProposalStatus,
)
from signalforge.remediation.runtime import (
    RemediationBroker,
    RemediationWorkerSettings,
    run_worker,
)
from signalforge.users.models import User, UserRole

pytestmark = [pytest.mark.integration, pytest.mark.anyio]
sessions = async_sessionmaker(engine, expire_on_commit=False)
type AsyncPredicate = Callable[[], Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class Broker:
    url: URL
    api: AsyncClient
    vhost: str


@dataclass(frozen=True, slots=True)
class Seeded:
    event: RemediationExecutionRequested
    execution_id: UUID
    proposal_id: UUID
    incident_id: UUID
    user_ids: tuple[UUID, UUID]


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[RemediationCommand] = []
        self.called = asyncio.Event()

    async def execute(self, command: RemediationCommand) -> RemediationExecutionResult:
        self.calls.append(command)
        self.called.set()
        return RemediationExecutionResult(
            proposal_id=command.proposal_id,
            action_kind=command.action_kind,
            target=command.target,
            outcome=RemediationExecutionOutcome.SUCCEEDED,
        )


async def provision_vhost(broker: Broker) -> None:
    (await broker.api.put(f"/api/vhosts/{broker.vhost}", json={})).raise_for_status()
    (
        await broker.api.put(
            f"/api/permissions/{broker.vhost}/{quote(broker.url.user or '', safe='')}",
            json={"configure": ".*", "write": ".*", "read": ".*"},
        )
    ).raise_for_status()


@pytest.fixture
async def broker() -> AsyncIterator[Broker]:
    raw_url = os.environ.get("SIGNALFORGE_TEST_RABBITMQ_URL")
    if raw_url is None:
        pytest.skip("Set SIGNALFORGE_TEST_RABBITMQ_URL to run real RabbitMQ tests")
    url = URL(raw_url)
    if url.scheme not in {"amqp", "amqps"} or not url.path.startswith(
        "/signalforge_test_"
    ):
        pytest.fail("RabbitMQ test URL must name a signalforge_test_ vhost")
    management_url = os.environ.get("SIGNALFORGE_TEST_RABBITMQ_MANAGEMENT_URL")
    if management_url is None or not url.user or not url.password:
        pytest.fail("Provide test RabbitMQ credentials and its management URL")
    vhost = f"signalforge_test_{uuid4().hex}"
    async with AsyncClient(
        base_url=management_url,
        auth=(url.user, url.password),
        timeout=10,
    ) as api:
        runtime_broker = Broker(url.with_path(f"/{vhost}"), api, vhost)
        response = await api.get("/api/overview")
        response.raise_for_status()
        assert response.json()["rabbitmq_version"] == "4.3.5"
        await provision_vhost(runtime_broker)
        try:
            yield runtime_broker
        finally:
            response = await api.delete(f"/api/vhosts/{vhost}")
            assert response.status_code in {204, 404}


@pytest.fixture
async def seeded() -> AsyncIterator[Seeded]:
    incident = Incident(
        source="runtime-integration",
        title="Checkout errors",
        severity=IncidentSeverity.HIGH,
        occurred_at=datetime.now(UTC),
    )
    proposer = User(
        email=f"runtime-proposer-{uuid4()}@example.com",
        password_hash="test-only-hash",
        role=UserRole.OPERATOR,
    )
    requester = User(
        email=f"runtime-requester-{uuid4()}@example.com",
        password_hash="test-only-hash",
        role=UserRole.ADMIN,
    )
    async with sessions.begin() as session:
        session.add_all([incident, proposer, requester])
        await session.flush()
        proposal = RemediationProposal(
            incident_id=incident.id,
            action_kind=RemediationActionKind.RESTART_SERVICE,
            target="checkout-api",
            reason="Restart is the approved runbook response",
            status=RemediationProposalStatus.APPROVED,
            proposed_by_user_id=proposer.id,
            approved_by_user_id=requester.id,
            approved_at=datetime.now(UTC),
        )
        session.add(proposal)
        await session.flush()
        execution = RemediationExecution(
            proposal_id=proposal.id,
            action_kind=proposal.action_kind,
            target=proposal.target,
            requested_by_user_id=requester.id,
        )
        session.add(execution)
        await session.flush()
        event = RemediationExecutionRequested(
            event_id=uuid4(),
            occurred_at=execution.requested_at,
            aggregate_id=execution.id,
            payload=RemediationExecutionRequestedPayload(
                proposal_id=execution.proposal_id,
                action_kind=execution.action_kind,
                target=execution.target,
            ),
        )
        seed = Seeded(
            event=event,
            execution_id=execution.id,
            proposal_id=proposal.id,
            incident_id=incident.id,
            user_ids=(proposer.id, requester.id),
        )
    try:
        yield seed
    finally:
        async with sessions.begin() as session:
            await session.execute(
                delete(ProcessedEvent).where(ProcessedEvent.event_id == event.event_id)
            )
            await session.execute(
                delete(RemediationExecutionAttempt).where(
                    RemediationExecutionAttempt.execution_id == execution.id
                )
            )
            await session.execute(
                delete(RemediationExecution).where(
                    RemediationExecution.id == execution.id
                )
            )
            await session.execute(
                delete(RemediationProposal).where(RemediationProposal.id == proposal.id)
            )
            await session.execute(delete(Incident).where(Incident.id == incident.id))
            await session.execute(delete(User).where(User.id.in_(seed.user_ids)))


def worker_settings(database_url: str, rabbitmq_url: str) -> RemediationWorkerSettings:
    return RemediationWorkerSettings.model_validate(
        {
            "database_url": database_url,
            "rabbitmq_url": rabbitmq_url,
            "remediation_worker_reconnect_base_seconds": 0.05,
            "remediation_worker_reconnect_max_seconds": 0.1,
            "remediation_worker_processing_retry_base_seconds": 0.05,
            "remediation_worker_processing_retry_max_seconds": 0.1,
            "remediation_worker_shutdown_drain_timeout_seconds": 0.2,
        }
    )


def execution_settings() -> RemediationExecutionSettings:
    return RemediationExecutionSettings.model_validate(
        {
            "restart_endpoints": {
                "checkout-api": "http://control-plane.internal/restart/checkout"
            },
            "request_timeout_seconds": 0.01,
            "attempt_lease_seconds": 0.1,
        }
    )


async def eventually(predicate: AsyncPredicate, *, timeout: float = 10) -> None:  # noqa: ASYNC109
    async with asyncio.timeout(timeout):
        while not await predicate():  # noqa: ASYNC110 - poll external services
            await asyncio.sleep(0.02)


async def publish(broker: Broker, event: RemediationExecutionRequested) -> None:
    connection = await aio_pika.connect(str(broker.url), timeout=5)
    try:
        channel = await connection.channel()
        exchange = await declare_topology(channel, timeout=5)
        confirmation = await exchange.publish(
            Message(
                event.model_dump_json().encode(),
                message_id=str(event.event_id),
                type=REMEDIATION_EXECUTION_ROUTING_KEY,
                content_type="application/json",
                content_encoding="utf-8",
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key=REMEDIATION_EXECUTION_ROUTING_KEY,
            mandatory=True,
        )
        assert isinstance(confirmation, Basic.Ack)
    finally:
        await connection.close()


async def queue_state(broker: Broker) -> dict[str, Any] | None:
    response = await broker.api.get(
        "/api/queues/"
        f"{quote(broker.vhost, safe='')}/"
        f"{quote(REMEDIATION_EXECUTION_QUEUE_NAME, safe='')}"
    )
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return response.json()


async def execution_succeeded(seed: Seeded) -> bool:
    async with sessions() as session:
        execution = await session.get(RemediationExecution, seed.execution_id)
        attempt = await session.scalar(
            select(RemediationExecutionAttempt).where(
                RemediationExecutionAttempt.execution_id == seed.execution_id
            )
        )
        receipt_count = await session.scalar(
            select(func.count())
            .select_from(ProcessedEvent)
            .where(
                ProcessedEvent.consumer_name == consumer.CONSUMER_NAME,
                ProcessedEvent.event_id == seed.event.event_id,
            )
        )
    return (
        execution is not None
        and execution.status is RemediationExecutionStatus.SUCCEEDED
        and attempt is not None
        and attempt.status is RemediationExecutionAttemptStatus.SUCCEEDED
        and receipt_count == 1
    )


async def test_real_worker_processes_one_execution_and_closes_resources(
    broker: Broker,
    seeded: Seeded,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executor = RecordingExecutor()
    connected: list[RemediationBroker] = []
    disposed: list[AsyncEngine] = []
    real_connect = runtime._connect_broker
    real_dispose = AsyncEngine.dispose

    async def recording_connect(value: RemediationWorkerSettings) -> RemediationBroker:
        resource = await real_connect(value)
        connected.append(resource)
        return resource

    async def recording_dispose(self: AsyncEngine, close: bool = True) -> None:
        disposed.append(self)
        await real_dispose(self, close=close)

    monkeypatch.setattr(runtime, "_connect_broker", recording_connect)
    monkeypatch.setattr(AsyncEngine, "dispose", recording_dispose)
    stop = asyncio.Event()
    task = asyncio.create_task(
        run_worker(
            worker_settings(
                os.environ["SIGNALFORGE_DATABASE_URL"],
                str(broker.url),
            ),
            execution_settings(),
            executor=executor,
            stop_event=stop,
            jitter_source=lambda: 0,
        )
    )
    try:
        await publish(broker, seeded.event)
        await asyncio.wait_for(executor.called.wait(), timeout=5)
        await eventually(lambda: execution_succeeded(seeded))

        async def queue_drained() -> bool:
            state = await queue_state(broker)
            return state is not None and state.get("messages") == 0

        await eventually(queue_drained)
        assert not task.done()
        assert executor.calls == [
            RemediationCommand(
                proposal_id=seeded.proposal_id,
                action_kind=RemediationActionKind.RESTART_SERVICE,
                target="checkout-api",
            )
        ]
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=2)

    assert len(connected) == 1
    assert connected[0].connection.is_closed
    assert connected[0].channel.is_closed
    assert len(disposed) == 1
