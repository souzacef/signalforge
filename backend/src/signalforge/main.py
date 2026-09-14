import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import perf_counter
from uuid import UUID, uuid4

from fastapi import FastAPI, Request, Response
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.metrics import NoOpMeterProvider
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import PlainTextResponse

from signalforge.api.health import router as health_router
from signalforge.auth.router import router as auth_router
from signalforge.core.config import get_settings, get_tracing_settings
from signalforge.enrichment.router import router as enrichment_router
from signalforge.incidents.router import router as incidents_router
from signalforge.observability.logging import bind_log_context, configure_logging
from signalforge.observability.metrics import UNMATCHED_ROUTE, HttpMetrics
from signalforge.observability.tracing import (
    API_SERVICE_NAME,
    TracingRuntime,
    create_tracing_runtime,
)
from signalforge.triage.router import router as triage_router

logger = logging.getLogger(__name__)
_TRACING_EXCLUDED_URLS = (
    r"/metrics(?:\?.*)?$,/health/live(?:\?.*)?$,/health/ready(?:\?.*)?$"
)


def _request_id(value: str | None) -> str:
    if value is not None and len(value) == 36:
        try:
            request_id = str(UUID(value))
        except ValueError:
            pass
        else:
            if request_id == value.lower():
                return request_id
    return str(uuid4())


def _route_path(request: Request) -> str:
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    return path if isinstance(path, str) else "/unmatched"


def create_app(
    http_metrics: HttpMetrics | None = None,
    tracing: TracingRuntime | None = None,
) -> FastAPI:
    """Create and configure the SignalForge API application."""
    configure_logging("signalforge-api")
    settings = get_settings()
    tracing_runtime = tracing or create_tracing_runtime(
        get_tracing_settings(), service_name=API_SERVICE_NAME
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            try:
                tracing_runtime.shutdown()
            except Exception as error:
                logger.error(
                    "tracing shutdown failed",
                    extra={
                        "event": "tracing_shutdown_failed",
                        "exception_type": type(error).__name__,
                    },
                )

    application = FastAPI(title=settings.app_name, lifespan=lifespan)
    metrics = http_metrics or HttpMetrics()
    application.state.http_metrics = metrics
    application.state.tracing = tracing_runtime
    # The application completion event replaces Uvicorn's duplicate access line.
    logging.getLogger("uvicorn.access").disabled = True

    @application.get("/metrics", include_in_schema=False)
    async def prometheus_metrics() -> Response:
        return Response(
            content=generate_latest(metrics.registry),
            media_type=CONTENT_TYPE_LATEST,
        )

    @application.exception_handler(Exception)
    async def unhandled_exception(request: Request, error: Exception) -> Response:
        return PlainTextResponse(
            "Internal Server Error",
            status_code=500,
            headers={"X-Request-ID": request.state.request_id},
        )

    @application.middleware("http")
    async def correlate_request(
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        request_id = _request_id(request.headers.get("X-Request-ID"))
        request.state.request_id = request_id
        started = perf_counter()
        status_code = 500
        with bind_log_context(request_id=request_id):
            try:
                response = await call_next(request)
                status_code = response.status_code
                response.headers["X-Request-ID"] = request_id
                return response
            except Exception as error:
                logger.error(
                    "unexpected HTTP request failure",
                    extra={
                        "event": "http_request_failed",
                        "exception_type": type(error).__name__,
                    },
                )
                raise
            finally:
                duration = perf_counter() - started
                route = _route_path(request)
                if request.method != "GET" or route != "/metrics":
                    metrics.observe(
                        method=request.method,
                        route=(UNMATCHED_ROUTE if route == "/unmatched" else route),
                        status_code=status_code,
                        duration=duration,
                    )
                logger.info(
                    "HTTP request completed",
                    extra={
                        "event": "http_request_completed",
                        "method": request.method,
                        "path": _route_path(request),
                        "status_code": status_code,
                        "duration_ms": round(duration * 1000, 3),
                    },
                )

    application.include_router(health_router)
    application.include_router(auth_router)
    application.include_router(incidents_router)
    application.include_router(triage_router)
    application.include_router(enrichment_router)
    if tracing_runtime.provider is not None:
        FastAPIInstrumentor.instrument_app(
            application,
            tracer_provider=tracing_runtime.provider,
            meter_provider=NoOpMeterProvider(),
            excluded_urls=_TRACING_EXCLUDED_URLS,
            exclude_spans=["send", "receive"],
        )
    return application


app = create_app()
