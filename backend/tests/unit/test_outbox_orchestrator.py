from dataclasses import FrozenInstanceError
from datetime import timedelta
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from sqlalchemy.exc import SQLAlchemyError

from signalforge.outbox import orchestrator
from signalforge.outbox.dispatching import DispatchErrorCode
from signalforge.outbox.orchestrator import (
    DispatchBatchResult,
    DispatchOutcome,
    EventDispatchResult,
    dispatch_batch,
)


def test_batch_result_and_event_results_are_immutable_and_sanitized() -> None:
    event = EventDispatchResult(
        uuid4(),
        DispatchOutcome.RETRY_SCHEDULED,
        error_code=DispatchErrorCode.PUBLISH_TIMEOUT,
    )
    result = DispatchBatchResult(
        claimed=1,
        published=0,
        retry_scheduled=1,
        ownership_lost=0,
        released=0,
        failed=0,
        publisher_retired=True,
        events=(event,),
    )
    assert result.events == (event,)
    assert event.error_code == "publish_timeout"
    assert not hasattr(event, "exception")
    with pytest.raises(FrozenInstanceError):
        result.failed = 1


class FailingCommit:
    async def __aenter__(self) -> object:
        return object()

    async def __aexit__(self, *args: object) -> None:
        raise SQLAlchemyError("claim commit failed")


class FailingFactory:
    def begin(self) -> FailingCommit:
        return FailingCommit()


@pytest.mark.anyio
async def test_claim_commit_failure_propagates_before_publication(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = Mock(is_retired=False)
    publisher.publish = AsyncMock()
    claim = AsyncMock(return_value=())
    monkeypatch.setattr(orchestrator, "claim_pending", claim)
    with pytest.raises(SQLAlchemyError, match="claim commit failed"):
        await dispatch_batch(
            FailingFactory(),
            publisher,
            batch_limit=1,
            lease_duration=timedelta(seconds=3),
            publish_timeout=1,
            settlement_budget=timedelta(seconds=1),
        )
    publisher.publish.assert_not_awaited()


@pytest.mark.anyio
async def test_retired_publisher_is_rejected_before_claim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = Mock(is_retired=True)
    claim = AsyncMock()
    monkeypatch.setattr(orchestrator, "claim_pending", claim)
    with pytest.raises(RuntimeError, match="publisher is retired"):
        await dispatch_batch(
            Mock(),
            publisher,
            batch_limit=1,
            lease_duration=timedelta(seconds=3),
            publish_timeout=1,
            settlement_budget=timedelta(seconds=1),
        )
    claim.assert_not_awaited()
