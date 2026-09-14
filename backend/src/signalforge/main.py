import logging
from time import perf_counter
from uuid import UUID, uuid4

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import PlainTextResponse

from signalforge.api.health import router as health_router
from signalforge.auth.router import router as auth_router
from signalforge.core.config import get_settings
from signalforge.enrichment.router import router as enrichment_router
from signalforge.incidents.router import router as incidents_router
from signalforge.observability.logging import bind_log_context, configure_logging
from signalforge.triage.router import router as triage_router

logger = logging.getLogger(__name__)


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


def create_app() -> FastAPI:
    """Create and configure the SignalForge API application."""
    configure_logging("signalforge-api")
    settings = get_settings()
    application = FastAPI(title=settings.app_name)
    # The application completion event replaces Uvicorn's duplicate access line.
    logging.getLogger("uvicorn.access").disabled = True

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
                logger.info(
                    "HTTP request completed",
                    extra={
                        "event": "http_request_completed",
                        "method": request.method,
                        "path": _route_path(request),
                        "status_code": status_code,
                        "duration_ms": round((perf_counter() - started) * 1000, 3),
                    },
                )

    application.include_router(health_router)
    application.include_router(auth_router)
    application.include_router(incidents_router)
    application.include_router(triage_router)
    application.include_router(enrichment_router)
    return application


app = create_app()
