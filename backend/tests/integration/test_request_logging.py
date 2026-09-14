import asyncio
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from signalforge.observability.logging import JsonFormatter

pytestmark = pytest.mark.anyio


class JsonCaptureHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.items: list[dict[str, Any]] = []
        self.setFormatter(JsonFormatter())

    def emit(self, record: logging.LogRecord) -> None:
        self.items.append(json.loads(self.format(record)))


@contextmanager
def capture_request_logs() -> Iterator[JsonCaptureHandler]:
    logger = logging.getLogger("signalforge.main")
    handler = JsonCaptureHandler()
    previous_level = logger.level
    previous_propagate = logger.propagate
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        yield handler
    finally:
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
        logger.propagate = previous_propagate


async def test_concurrent_requests_keep_request_ids_isolated(
    client: AsyncClient,
) -> None:
    request_ids = [str(uuid4()), str(uuid4())]

    with capture_request_logs() as captured:
        responses = await asyncio.gather(
            *(
                client.get("/health/live", headers={"X-Request-ID": request_id})
                for request_id in request_ids
            )
        )

    assert [response.status_code for response in responses] == [200, 200]
    assert [response.headers["X-Request-ID"] for response in responses] == request_ids
    completion_logs = [
        item for item in captured.items if item["event"] == "http_request_completed"
    ]
    assert len(completion_logs) == 2
    assert {item["request_id"] for item in completion_logs} == set(request_ids)
    assert all(item["path"] == "/health/live" for item in completion_logs)


async def test_missing_and_invalid_request_ids_are_safely_replaced(
    client: AsyncClient,
) -> None:
    invalid = "attacker-controlled-request-id"

    with capture_request_logs() as captured:
        generated_response, replaced_response = await asyncio.gather(
            client.get("/health/live"),
            client.get("/health/live", headers={"X-Request-ID": invalid}),
        )

    generated = generated_response.headers["X-Request-ID"]
    replaced = replaced_response.headers["X-Request-ID"]
    assert str(UUID(generated)) == generated
    assert str(UUID(replaced)) == replaced
    assert replaced != invalid
    completion_ids = {
        item["request_id"]
        for item in captured.items
        if item["event"] == "http_request_completed"
    }
    assert completion_ids == {generated, replaced}
    assert invalid not in json.dumps(captured.items)


async def test_unexpected_500_keeps_default_response_and_request_id(
    app: FastAPI,
) -> None:
    @app.get("/test-unexpected-failure")
    async def fail() -> None:
        raise RuntimeError("private-exception-value")

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    with capture_request_logs() as captured:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/test-unexpected-failure")

    assert response.status_code == 500
    assert response.text == "Internal Server Error"
    request_id = response.headers["X-Request-ID"]
    assert str(UUID(request_id)) == request_id
    completion = next(
        item for item in captured.items if item["event"] == "http_request_completed"
    )
    assert completion["request_id"] == request_id
    assert completion["status_code"] == 500
    assert "private-exception-value" not in json.dumps(captured.items)
