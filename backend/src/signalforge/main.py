from fastapi import FastAPI

from signalforge.api.health import router as health_router
from signalforge.auth.router import router as auth_router
from signalforge.core.config import get_settings
from signalforge.incidents.router import router as incidents_router


def create_app() -> FastAPI:
    """Create and configure the SignalForge API application."""
    settings = get_settings()
    application = FastAPI(title=settings.app_name)
    application.include_router(health_router)
    application.include_router(auth_router)
    application.include_router(incidents_router)
    return application


app = create_app()
