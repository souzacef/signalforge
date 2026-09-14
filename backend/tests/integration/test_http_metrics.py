import asyncio
from uuid import uuid4

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from prometheus_client import CONTENT_TYPE_LATEST

from signalforge.observability.metrics import UNMATCHED_ROUTE, HttpMetrics

pytestmark = pytest.mark.anyio


async def test_metrics_endpoint_exposes_only_application_metrics(
    app: FastAPI,
    client: AsyncClient,
) -> None:
    response = await client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["Content-Type"] == CONTENT_TYPE_LATEST
    assert "signalforge_http_requests_total" in response.text
    assert "signalforge_http_request_duration_seconds" in response.text
    assert "python_gc_objects_collected_total" not in response.text
    assert "process_cpu_seconds_total" not in response.text
    assert (
        app.state.http_metrics.registry.get_sample_value(
            "signalforge_http_requests_total",
            {"method": "GET", "route": "/metrics", "status_code": "200"},
        )
        is None
    )


async def test_http_counter_and_duration_observe_expected_response(
    app: FastAPI,
    client: AsyncClient,
) -> None:
    response = await client.get("/health/live")
    registry = app.state.http_metrics.registry

    assert response.status_code == 200
    assert (
        registry.get_sample_value(
            "signalforge_http_requests_total",
            {"method": "GET", "route": "/health/live", "status_code": "200"},
        )
        == 1
    )
    assert (
        registry.get_sample_value(
            "signalforge_http_request_duration_seconds_count",
            {"method": "GET", "route": "/health/live"},
        )
        == 1
    )
    duration_sum = registry.get_sample_value(
        "signalforge_http_request_duration_seconds_sum",
        {"method": "GET", "route": "/health/live"},
    )
    assert duration_sum is not None and duration_sum >= 0
    assert (
        registry.get_sample_value(
            "signalforge_http_request_duration_seconds_bucket",
            {"method": "GET", "route": "/health/live", "le": "+Inf"},
        )
        == 1
    )


async def test_dynamic_incident_paths_share_one_route_template(
    app: FastAPI,
    client: AsyncClient,
) -> None:
    incident_ids = [str(uuid4()), str(uuid4())]
    responses = await asyncio.gather(
        *(
            client.get(f"/api/v1/incidents/{incident_id}")
            for incident_id in incident_ids
        )
    )
    registry = app.state.http_metrics.registry

    assert [response.status_code for response in responses] == [401, 401]
    assert (
        registry.get_sample_value(
            "signalforge_http_requests_total",
            {
                "method": "GET",
                "route": "/api/v1/incidents/{incident_id}",
                "status_code": "401",
            },
        )
        == 2
    )
    route_labels = {
        sample.labels["route"]
        for metric in registry.collect()
        for sample in metric.samples
        if "route" in sample.labels
    }
    assert "/api/v1/incidents/{incident_id}" in route_labels
    assert not set(incident_ids) & route_labels


async def test_query_values_never_enter_route_labels(
    app: FastAPI,
    client: AsyncClient,
) -> None:
    values = ("critical", "low")
    for value in values:
        response = await client.get(f"/api/v1/incidents?severity={value}")
        assert response.status_code == 401

    route_labels = {
        sample.labels["route"]
        for metric in app.state.http_metrics.registry.collect()
        for sample in metric.samples
        if "route" in sample.labels
    }
    assert route_labels == {"/api/v1/incidents"}
    assert all(value not in route for route in route_labels for value in values)


async def test_unmatched_paths_share_stable_label(
    app: FastAPI,
    client: AsyncClient,
) -> None:
    paths = ("/missing/one", "/different/missing/two")
    for path in paths:
        response = await client.get(path)
        assert response.status_code == 404

    registry = app.state.http_metrics.registry
    assert (
        registry.get_sample_value(
            "signalforge_http_requests_total",
            {"method": "GET", "route": UNMATCHED_ROUTE, "status_code": "404"},
        )
        == 2
    )
    rendered_labels = str(
        [sample.labels for metric in registry.collect() for sample in metric.samples]
    )
    assert all(path not in rendered_labels for path in paths)


async def test_unexpected_500_is_counted_without_changing_response(
    app: FastAPI,
) -> None:
    @app.get("/test-unexpected-failure")
    async def fail() -> None:
        raise RuntimeError("private-exception-value")

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/test-unexpected-failure")

    registry = app.state.http_metrics.registry
    assert response.status_code == 500
    assert response.text == "Internal Server Error"
    assert response.headers["X-Request-ID"]
    assert (
        registry.get_sample_value(
            "signalforge_http_requests_total",
            {
                "method": "GET",
                "route": "/test-unexpected-failure",
                "status_code": "500",
            },
        )
        == 1
    )
    assert (
        registry.get_sample_value(
            "signalforge_http_request_duration_seconds_count",
            {"method": "GET", "route": "/test-unexpected-failure"},
        )
        == 1
    )


async def test_metrics_scrapes_do_not_change_http_metrics(
    app: FastAPI, client: AsyncClient
) -> None:
    first = await client.get("/metrics")
    second = await client.get("/metrics")

    assert first.text == second.text
    assert not any(
        sample.labels.get("route") == "/metrics"
        for metric in app.state.http_metrics.registry.collect()
        for sample in metric.samples
    )


async def test_http_metrics_instances_have_isolated_registries() -> None:
    first = HttpMetrics()
    second = HttpMetrics()
    first.observe(method="get", route="/health/live", status_code=200, duration=0)

    labels = {"method": "GET", "route": "/health/live", "status_code": "200"}
    assert (
        first.registry.get_sample_value("signalforge_http_requests_total", labels) == 1
    )
    assert (
        second.registry.get_sample_value("signalforge_http_requests_total", labels)
        is None
    )
