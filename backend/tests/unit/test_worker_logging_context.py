import asyncio
import json
import logging
from typing import Any, cast
from uuid import uuid4

import pytest
from aio_pika import IncomingMessage
from aio_pika.abc import AbstractIncomingMessage

from signalforge.consumers import runtime
from signalforge.consumers.incident_created import InvalidEventError, ProcessingResult
from signalforge.consumers.runtime import ConsumerSettings
from signalforge.observability.logging import JsonFormatter, current_log_context

pytestmark = pytest.mark.anyio


class FakeMessage:
    def __init__(self, event_id: str) -> None:
        self.body = b"{}"
        self.message_id = event_id
        self.type = "incident.created"


class JsonCaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.items: list[dict[str, Any]] = []
        self.setFormatter(JsonFormatter())

    def emit(self, record: logging.LogRecord) -> None:
        self.items.append(json.loads(self.format(record)))


async def test_delivery_context_resets_after_terminal_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event_ids = [str(uuid4()), str(uuid4())]
    messages = [FakeMessage(event_id) for event_id in event_ids]
    test_logger = logging.getLogger("signalforge.tests.worker_delivery")
    handler = JsonCaptureHandler()
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.INFO)
    test_logger.propagate = False

    async def handle(
        message: IncomingMessage,
        session_factory: object,
    ) -> ProcessingResult:
        test_logger.info("delivery observed", extra={"event": "delivery_observed"})
        if message.message_id == event_ids[0]:
            raise InvalidEventError("invalid_json")
        return ProcessingResult.PROCESSED

    monkeypatch.setattr(runtime, "handle_message", handle)
    settings = ConsumerSettings.model_validate(
        {
            "database_url": (
                "postgresql+asyncpg://user:password@localhost/signalforge_test"
            ),
            "rabbitmq_url": "amqp://user:password@localhost/test",
        }
    )

    try:
        with pytest.raises(InvalidEventError):
            await runtime._run_handler_or_drain(
                cast(AbstractIncomingMessage, messages[0]),
                cast(Any, object()),
                settings,
                asyncio.Event(),
            )
        await runtime._run_handler_or_drain(
            cast(AbstractIncomingMessage, messages[1]),
            cast(Any, object()),
            settings,
            asyncio.Event(),
        )
    finally:
        test_logger.removeHandler(handler)

    assert [item["event_id"] for item in handler.items] == event_ids
    second_log = handler.items[1]
    assert second_log["message"] == "delivery observed"
    assert event_ids[0] not in json.dumps(second_log)
    assert current_log_context() == {}
