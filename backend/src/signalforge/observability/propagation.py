"""Bounded W3C Trace Context propagation with no baggage support."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from opentelemetry.context import Context
from opentelemetry.trace import get_current_span
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

TRACE_CONTEXT_MAX_LENGTH: Final = 512
_TRACEPARENT: Final = "traceparent"
_TRACESTATE: Final = "tracestate"
_TRACESTATE_KEY = re.compile(
    r"[a-z][_0-9a-z\-*\/]{0,255}"
    r"|[a-z0-9][_0-9a-z\-*\/]{0,240}@[a-z][_0-9a-z\-*\/]{0,13}"
)
_TRACESTATE_VALUE = re.compile(
    r"[\x20-\x2b\x2d-\x3c\x3e-\x7e]{0,255}"
    r"[\x21-\x2b\x2d-\x3c\x3e-\x7e]"
)
_PROPAGATOR = TraceContextTextMapPropagator()


@dataclass(frozen=True, slots=True)
class TraceContextCarrier:
    """The only propagation values allowed across a durable boundary."""

    traceparent: str | None = None
    tracestate: str | None = None

    def as_dict(self) -> dict[str, str]:
        carrier: dict[str, str] = {}
        if self.traceparent is not None:
            carrier[_TRACEPARENT] = self.traceparent
        if self.tracestate is not None:
            carrier[_TRACESTATE] = self.tracestate
        return carrier


def _normalize_value(value: object) -> str | None:
    if isinstance(value, bytes):
        try:
            normalized = value.decode("ascii")
        except UnicodeDecodeError:
            return None
    elif isinstance(value, str):
        try:
            value.encode("ascii")
        except UnicodeEncodeError:
            return None
        normalized = value
    else:
        return None
    if not normalized or len(normalized) > TRACE_CONTEXT_MAX_LENGTH:
        return None
    return normalized


def _valid_tracestate_syntax(value: str) -> bool:
    """Reject malformed values before the SDK parser can log their contents."""
    members = re.split(r"[ \t]*,[ \t]*", value)
    if len(members) > 32:
        return False
    keys: set[str] = set()
    for member in members:
        if "=" not in member:
            return False
        key, item = member.split("=", 1)
        item = item.rstrip(" \t")
        if (
            _TRACESTATE_KEY.fullmatch(key) is None
            or _TRACESTATE_VALUE.fullmatch(item) is None
            or key in keys
        ):
            return False
        keys.add(key)
    return True


def normalize_trace_carrier(
    carrier: Mapping[str, object] | None = None,
    *,
    traceparent: object = None,
    tracestate: object = None,
) -> TraceContextCarrier:
    """Copy only bounded ASCII W3C fields from storage or message headers."""
    if carrier is not None:
        try:
            traceparent = carrier.get(_TRACEPARENT)
            tracestate = carrier.get(_TRACESTATE)
        except (AttributeError, TypeError, ValueError):
            return TraceContextCarrier()
    normalized_parent = _normalize_value(traceparent)
    normalized_state = _normalize_value(tracestate)
    if normalized_state is not None and not _valid_tracestate_syntax(normalized_state):
        normalized_state = None
    return TraceContextCarrier(normalized_parent, normalized_state)


def inject_trace_context(context: Context | None = None) -> TraceContextCarrier:
    """Inject one valid SpanContext through the official W3C propagator."""
    carrier: dict[str, str] = {}
    _PROPAGATOR.inject(carrier, context=context)
    return normalize_trace_carrier(carrier)


def capture_current_trace_context() -> TraceContextCarrier:
    """Capture the current valid SpanContext without starting a span."""
    if not get_current_span().get_span_context().is_valid:
        return TraceContextCarrier()
    return inject_trace_context()


def extract_trace_context(
    carrier: Mapping[str, object] | None = None,
    *,
    traceparent: object = None,
    tracestate: object = None,
) -> Context:
    """Extract a bounded W3C parent, returning an explicit root when invalid."""
    normalized = normalize_trace_carrier(
        carrier, traceparent=traceparent, tracestate=tracestate
    )
    if normalized.traceparent is None:
        return Context()
    extracted = _PROPAGATOR.extract(normalized.as_dict(), context=Context())
    if not get_current_span(extracted).get_span_context().is_valid:
        return Context()
    return extracted
