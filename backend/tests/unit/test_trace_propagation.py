from __future__ import annotations

import logging

from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import TraceFlags, get_current_span

from signalforge.observability.propagation import (
    TRACE_CONTEXT_MAX_LENGTH,
    capture_current_trace_context,
    extract_trace_context,
    inject_trace_context,
    normalize_trace_carrier,
)


def test_capture_and_extract_preserve_sampled_trace_and_tracestate() -> None:
    incoming = extract_trace_context(
        {
            "traceparent": ("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
            "tracestate": "vendor=value",
            "baggage": "secret=must-not-propagate",
        }
    )
    incoming_span_context = get_current_span(incoming).get_span_context()

    assert incoming_span_context.is_valid
    assert incoming_span_context.is_remote
    assert incoming_span_context.trace_flags.sampled
    assert incoming_span_context.trace_state.to_header() == "vendor=value"

    provider = TracerProvider()
    tracer = provider.get_tracer(__name__)
    try:
        with tracer.start_as_current_span("capture", context=incoming):
            captured = capture_current_trace_context()
        assert captured.tracestate == "vendor=value"
        assert captured.traceparent is not None
        assert captured.traceparent.startswith("00-0af7651916cd43dd8448eb211c80319c-")
        assert captured.traceparent.endswith("-01")
        assert set(captured.as_dict()) == {"traceparent", "tracestate"}
    finally:
        provider.shutdown()


def test_unsampled_context_round_trip_preserves_trace_flags() -> None:
    parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"
    extracted = extract_trace_context(traceparent=parent)
    span_context = get_current_span(extracted).get_span_context()

    assert span_context.is_valid
    assert span_context.trace_flags == TraceFlags(0)
    assert inject_trace_context(extracted).traceparent == parent


def test_missing_malformed_oversized_and_unsupported_values_return_root(
    caplog,
) -> None:
    secret = "raw-tracing-value-must-not-be-logged"
    malformed = [
        None,
        {"traceparent": "malformed-" + secret},
        {"traceparent": "x" * (TRACE_CONTEXT_MAX_LENGTH + 1)},
        {"traceparent": b"\xff"},
        {"traceparent": object()},
        {
            "traceparent": ("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
            "tracestate": "invalid " + secret,
        },
    ]

    with caplog.at_level(logging.WARNING):
        contexts = [extract_trace_context(value) for value in malformed]

    assert all(
        not get_current_span(context).get_span_context().is_valid
        for context in contexts[:-1]
    )
    # Invalid tracestate is safely discarded while a valid traceparent survives.
    assert get_current_span(contexts[-1]).get_span_context().is_valid
    assert secret not in caplog.text


def test_normalization_accepts_amqp_ascii_bytes_and_only_known_keys() -> None:
    traceparent = b"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
    carrier = normalize_trace_carrier(
        {
            "traceparent": traceparent,
            "tracestate": b"vendor=value",
            "authorization": "secret",
            "baggage": "secret=value",
        }
    )

    assert carrier.traceparent == traceparent.decode()
    assert carrier.tracestate == "vendor=value"
    assert carrier.as_dict() == {
        "traceparent": traceparent.decode(),
        "tracestate": "vendor=value",
    }


def test_no_valid_current_span_produces_empty_carrier() -> None:
    assert capture_current_trace_context().as_dict() == {}
    assert not get_current_span(Context()).get_span_context().is_valid
