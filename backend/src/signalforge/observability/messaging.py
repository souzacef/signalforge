"""Narrow OpenTelemetry messaging semantic-convention helpers."""

from __future__ import annotations

from typing import Final

from opentelemetry.trace import Span, Status, StatusCode

MESSAGING_SYSTEM: Final = "messaging.system"
MESSAGING_OPERATION_NAME: Final = "messaging.operation.name"
MESSAGING_OPERATION_TYPE: Final = "messaging.operation.type"
MESSAGING_DESTINATION_NAME: Final = "messaging.destination.name"
MESSAGING_RABBITMQ_ROUTING_KEY: Final = "messaging.rabbitmq.destination.routing_key"
MESSAGING_MESSAGE_ID: Final = "messaging.message.id"
ERROR_TYPE: Final = "error.type"


def messaging_attributes(
    *,
    operation_name: str,
    operation_type: str,
    destination_name: str,
    routing_key: str,
    message_id: str | None = None,
) -> dict[str, str]:
    attributes = {
        MESSAGING_SYSTEM: "rabbitmq",
        MESSAGING_OPERATION_NAME: operation_name,
        MESSAGING_OPERATION_TYPE: operation_type,
        MESSAGING_DESTINATION_NAME: destination_name,
        MESSAGING_RABBITMQ_ROUTING_KEY: routing_key,
    }
    if message_id is not None:
        attributes[MESSAGING_MESSAGE_ID] = message_id
    return attributes


def mark_span_error(span: Span, error: BaseException) -> None:
    """Record only a bounded exception-class identity, never exception text."""
    identity = f"{type(error).__module__}.{type(error).__qualname__}"
    span.set_status(Status(StatusCode.ERROR))
    span.set_attribute(ERROR_TYPE, identity[:128])
