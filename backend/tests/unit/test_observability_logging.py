import asyncio
import json
import logging
from datetime import datetime
from typing import Any
from uuid import uuid4

import pytest

from signalforge.observability.logging import (
    JsonFormatter,
    bind_log_context,
    current_log_context,
)

pytestmark = pytest.mark.anyio


class JsonCaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.items: list[dict[str, Any]] = []
        self.rendered: list[str] = []
        self.setFormatter(JsonFormatter())

    def emit(self, record: logging.LogRecord) -> None:
        rendered = self.format(record)
        self.rendered.append(rendered)
        self.items.append(json.loads(rendered))


async def test_json_log_has_stable_schema_and_task_local_context() -> None:
    logger = logging.getLogger("signalforge.tests.observability")
    handler = JsonCaptureHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    request_id = str(uuid4())
    event_id = str(uuid4())

    try:
        with bind_log_context(request_id=request_id, event_id=event_id):
            logger.info("message processed", extra={"event": "message_processed"})
    finally:
        logger.removeHandler(handler)

    item = handler.items[0]
    assert item["level"] == "INFO"
    assert item["logger"] == logger.name
    assert item["event"] == "message_processed"
    assert item["service"] == "signalforge-api"
    assert item["message"] == "message processed"
    assert item["request_id"] == request_id
    assert item["event_id"] == event_id
    assert datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00")).tzinfo
    assert current_log_context() == {}


async def test_concurrent_contexts_do_not_cross_contaminate() -> None:
    logger = logging.getLogger("signalforge.tests.context")
    handler = JsonCaptureHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    pairs = [(str(uuid4()), str(uuid4())) for _ in range(2)]

    async def emit(request_id: str, event_id: str) -> None:
        with bind_log_context(request_id=request_id, event_id=event_id):
            await asyncio.sleep(0)
            logger.info("task completed", extra={"event": "task_completed"})

    try:
        await asyncio.gather(*(emit(*pair) for pair in pairs))
    finally:
        logger.removeHandler(handler)

    observed = {(item["request_id"], item["event_id"]) for item in handler.items}
    assert observed == set(pairs)
    assert current_log_context() == {}


async def test_allowlist_omits_secret_bearing_values() -> None:
    secrets = [
        "postgresql://user:SUPER_SECRET@host/db",
        "amqp://user:SUPER_SECRET@host/vhost",
        "gemini-secret-value",
        "fake-jwt-secret",
    ]
    logger = logging.getLogger("signalforge.tests.secrets")
    handler = JsonCaptureHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.WARNING)
    logger.propagate = False

    try:
        logger.warning(
            "known failure without interpolated details",
            *secrets,
            extra={
                "event": "provider_transient_failure",
                "database_url": secrets[0],
                "rabbitmq_url": secrets[1],
                "api_key": secrets[2],
                "authorization": secrets[3],
                "reason": "unavailable",
            },
        )
    finally:
        logger.removeHandler(handler)

    output = "\n".join(handler.rendered)
    assert all(secret not in output for secret in secrets)
    assert handler.items[0]["reason"] == "unavailable"
