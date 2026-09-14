"""Structured, allowlisted JSON logging with task-local correlation context."""

import json
import logging
import re
import sys
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Final
from uuid import UUID

_context: ContextVar[Mapping[str, str | int | float] | None] = ContextVar(
    "signalforge_log_context", default=None
)
_service: ContextVar[str] = ContextVar("signalforge_log_service", default="signalforge")
_EVENT_PATTERN: Final = re.compile(r"^[a-z][a-z0-9_]*$")
_SAFE_LABEL: Final = re.compile(r"^[a-zA-Z0-9_.-]{1,100}$")
_SAFE_ROUTE: Final = re.compile(r"^/[a-zA-Z0-9_/{}/.-]{0,199}$")
_CONTEXT_FIELDS: Final = frozenset(
    {
        "request_id",
        "incident_id",
        "event_id",
        "trigger_event_id",
        "consumer_name",
        "event_type",
        "event_version",
    }
)
_EXTRA_FIELDS: Final = frozenset(
    {
        "method",
        "path",
        "status_code",
        "duration_ms",
        "result",
        "reason",
        "attempt",
        "delay_seconds",
        "queue",
        "routing_key",
        "signal",
        "claimed",
        "published",
        "retry_scheduled",
        "ownership_lost",
        "released",
        "failed",
        "publisher_retired",
        "error_code",
        "exception_type",
    }
)
_NUMERIC_FIELDS: Final = frozenset(
    {
        "event_version",
        "status_code",
        "duration_ms",
        "attempt",
        "delay_seconds",
        "claimed",
        "published",
        "retry_scheduled",
        "ownership_lost",
        "released",
        "failed",
    }
)
_UUID_FIELDS: Final = frozenset(
    {"request_id", "incident_id", "event_id", "trigger_event_id"}
)


def _safe_field(name: str, value: object) -> str | int | float | bool | None:
    if name in _UUID_FIELDS:
        try:
            return str(UUID(str(value)))
        except (TypeError, ValueError, AttributeError):
            return None
    if name in _NUMERIC_FIELDS:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return value
    if name == "publisher_retired":
        return value if isinstance(value, bool) else None
    if name == "path":
        return (
            value if isinstance(value, str) and _SAFE_ROUTE.fullmatch(value) else None
        )
    if isinstance(value, str) and _SAFE_LABEL.fullmatch(value):
        return value
    return None


@contextmanager
def bind_log_context(**fields: object) -> Iterator[None]:
    """Bind only safe, known correlation fields and restore the prior context."""
    valid = {
        key: normalized
        for key, value in fields.items()
        if key in _CONTEXT_FIELDS
        if (normalized := _safe_field(key, value)) is not None
    }
    token = _context.set({**(_context.get() or {}), **valid})
    try:
        yield
    finally:
        _context.reset(token)


def current_log_context() -> dict[str, str | int | float]:
    return dict(_context.get() or {})


def delivery_log_context(message: object, *, consumer_name: str) -> dict[str, object]:
    """Build safe correlation candidates from existing AMQP metadata."""
    return {
        "consumer_name": consumer_name,
        "event_id": getattr(message, "message_id", None),
        "event_type": getattr(message, "type", None),
    }


class JsonFormatter(logging.Formatter):
    """Serialize only the stable schema and explicitly allowlisted safe fields."""

    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "event", None)
        if not isinstance(event, str) or not _EVENT_PATTERN.fullmatch(event):
            event = "log_record"
        data: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC)
            .isoformat()
            .replace("+00:00", "Z"),
            "level": record.levelname,
            "logger": record.name,
            "event": event,
            "service": _service.get(),
            "message": str(record.msg) if isinstance(record.msg, str) else "log record",
        }
        data.update(_context.get() or {})
        for key in _EXTRA_FIELDS | _CONTEXT_FIELDS:
            if key in data:
                continue
            value = _safe_field(key, getattr(record, key, None))
            if value is not None:
                data[key] = value
        return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


def configure_logging(service_name: str) -> None:
    """Set explicit process identity; use stdout JSON unless a test owns logging."""
    if not _SAFE_LABEL.fullmatch(service_name):
        raise ValueError("invalid logging service name")
    _service.set(service_name)
    root = logging.getLogger()
    if root.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(logging.INFO)
