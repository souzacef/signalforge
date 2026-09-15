"""Focused checks for process-local, bounded pipeline outcomes."""

from uuid import uuid4

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from signalforge.observability.metrics import (
    REMEDIATION_EXECUTOR_DURATION_BUCKETS,
    EnrichmentMetrics,
    IncidentConsumerMetrics,
    MessageMetricResult,
    OutboxDispatchMetricResult,
    OutboxMetrics,
    ProviderMetricResult,
    ProviderMetrics,
    RemediationExecutorMetricResult,
    RemediationExecutorMetrics,
    RemediationMessageMetricResult,
    RemediationWorkerMetrics,
)
from signalforge.outbox.dispatcher import _log_batch
from signalforge.outbox.dispatching import DispatchErrorCode
from signalforge.outbox.orchestrator import (
    DispatchBatchResult,
    DispatchOutcome,
    EventDispatchResult,
)


def test_outbox_batch_records_each_real_outcome_with_bounded_labels() -> None:
    metrics = OutboxMetrics()
    secret = "secret-event-id-and-error-text"
    events = (
        EventDispatchResult(uuid4(), DispatchOutcome.PUBLISHED, "incident.created"),
        EventDispatchResult(
            uuid4(), DispatchOutcome.RETRY_SCHEDULED, "triage.enrichment.requested"
        ),
        EventDispatchResult(
            uuid4(), DispatchOutcome.PUBLISHED, "remediation.execution.requested"
        ),
        EventDispatchResult(
            uuid4(),
            DispatchOutcome.RETRY_SCHEDULED,
            "incident.created",
            error_code=DispatchErrorCode.INVALID_EVENT,
        ),
        EventDispatchResult(
            uuid4(), DispatchOutcome.OWNERSHIP_LOST, "incident.created"
        ),
        EventDispatchResult(
            uuid4(),
            DispatchOutcome.DATABASE_FAILED,
            "incident.created",
            publication_confirmed=True,
        ),
        EventDispatchResult(
            uuid4(), DispatchOutcome.DATABASE_FAILED, "incident.created"
        ),
        EventDispatchResult(uuid4(), DispatchOutcome.RELEASED, secret),
    )
    _log_batch(DispatchBatchResult(8, 2, 2, 1, 1, 2, False, events), metrics)
    expected = [
        ("incident.created", "published"),
        ("triage.enrichment.requested", "retry_scheduled"),
        ("remediation.execution.requested", "published"),
        ("incident.created", "invalid"),
        ("incident.created", "ownership_lost"),
        ("incident.created", "ambiguous"),
        ("incident.created", "database_failed"),
        ("unknown", "released"),
    ]
    for event_type, result in expected:
        assert (
            metrics.registry.get_sample_value(
                "signalforge_outbox_dispatch_total",
                {"event_type": event_type, "result": result},
            )
            == 1
        )
    exposition = generate_latest(metrics.registry).decode()
    assert secret not in exposition
    assert all(str(event.event_id) not in exposition for event in events)
    assert set(OutboxDispatchMetricResult) == {
        OutboxDispatchMetricResult(result) for _, result in expected
    }


def test_message_metrics_are_isolated_and_have_only_bounded_results() -> None:
    incident = IncidentConsumerMetrics()
    other_incident = IncidentConsumerMetrics()
    enrichment = EnrichmentMetrics()
    for result in MessageMetricResult:
        incident.record(result)
        enrichment.record(result)
    for result in MessageMetricResult:
        labels = {"result": result.value}
        assert (
            incident.registry.get_sample_value(
                "signalforge_incident_consumer_messages_total", labels
            )
            == 1
        )
        assert (
            enrichment.registry.get_sample_value(
                "signalforge_enrichment_messages_total", labels
            )
            == 1
        )
        assert (
            other_incident.registry.get_sample_value(
                "signalforge_incident_consumer_messages_total", labels
            )
            is None
        )
    assert incident.registry is not enrichment.registry
    assert incident.registry is not other_incident.registry


def test_provider_counter_and_duration_share_only_controlled_labels() -> None:
    registry = CollectorRegistry()
    metrics = ProviderMetrics(registry)
    for result in ProviderMetricResult:
        metrics.record(
            provider="gemini", model="operator-model", result=result, duration=0.1
        )
        labels = {
            "provider": "gemini",
            "model": "operator-model",
            "result": result.value,
        }
        assert (
            registry.get_sample_value(
                "signalforge_enrichment_provider_calls_total", labels
            )
            == 1
        )
        assert (
            registry.get_sample_value(
                "signalforge_enrichment_provider_duration_seconds_count", labels
            )
            == 1
        )
    assert metrics.registry is registry


def test_remediation_metrics_have_exact_names_and_bounded_result_labels() -> None:
    worker = RemediationWorkerMetrics()
    executor = RemediationExecutorMetrics(worker.registry)

    for result in RemediationMessageMetricResult:
        worker.record(result)
    for result in RemediationExecutorMetricResult:
        executor.record(result=result, duration=0.125)

    assert worker.messages._name == "signalforge_remediation_messages"
    assert worker.messages._labelnames == ("result",)
    assert executor.calls._name == "signalforge_remediation_executor_calls"
    assert executor.calls._labelnames == ("result",)
    assert (
        executor.duration._name == "signalforge_remediation_executor_duration_seconds"
    )
    assert executor.duration._labelnames == ("result",)
    assert REMEDIATION_EXECUTOR_DURATION_BUCKETS == (
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
        10.0,
        15.0,
        20.0,
        30.0,
    )
    bucket_bounds = {
        float(sample.labels["le"])
        for sample in executor.duration.collect()[0].samples
        if sample.name == "signalforge_remediation_executor_duration_seconds_bucket"
        and sample.labels["result"] == "success"
    }
    assert bucket_bounds == {
        *REMEDIATION_EXECUTOR_DURATION_BUCKETS,
        float("inf"),
    }
    assert set(result.value for result in RemediationMessageMetricResult) == {
        "processed",
        "duplicate",
        "rejected",
        "requeued",
        "in_progress",
    }
    assert set(result.value for result in RemediationExecutorMetricResult) == {
        "success",
        "target_not_allowed",
        "unsupported_action",
        "http_rejected",
        "http_server_error",
        "timeout",
        "transport",
        "interrupted",
        "unexpected",
    }
    for result in RemediationMessageMetricResult:
        assert (
            worker.registry.get_sample_value(
                "signalforge_remediation_messages_total", {"result": result.value}
            )
            == 1
        )
    for result in RemediationExecutorMetricResult:
        labels = {"result": result.value}
        assert (
            worker.registry.get_sample_value(
                "signalforge_remediation_executor_calls_total", labels
            )
            == 1
        )
        assert (
            worker.registry.get_sample_value(
                "signalforge_remediation_executor_duration_seconds_count", labels
            )
            == 1
        )
    assert executor.registry is worker.registry


def test_remediation_metric_update_failures_are_observational(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    worker = RemediationWorkerMetrics()
    executor = RemediationExecutorMetrics(worker.registry)

    def fail(*args: object, **kwargs: object) -> None:
        raise ValueError("metrics unavailable")

    monkeypatch.setattr(worker.messages, "labels", fail)
    monkeypatch.setattr(executor.calls, "labels", fail)
    monkeypatch.setattr(executor.duration, "labels", fail)

    worker.record(RemediationMessageMetricResult.PROCESSED)
    executor.record(
        result=RemediationExecutorMetricResult.SUCCESS,
        duration=0.1,
    )
