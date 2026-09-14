from functools import lru_cache
from typing import Annotated

from pydantic import Field, PostgresDsn, SecretStr, StringConstraints
from pydantic_settings import BaseSettings, SettingsConfigDict

MetricsHost = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
MetricsPort = Annotated[int, Field(ge=1, le=65535)]


class DatabaseSettings(BaseSettings):
    """Shared database configuration without API-only requirements."""

    model_config = SettingsConfigDict(
        env_prefix="SIGNALFORGE_",
        env_file=(".env", "../.env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: PostgresDsn


class Settings(DatabaseSettings):
    """API runtime settings loaded from the environment or a local dotenv file."""

    app_name: str = "SignalForge"
    app_environment: str = "development"
    jwt_secret: Annotated[SecretStr, Field(min_length=32)]
    access_token_expire_minutes: Annotated[int, Field(gt=0)] = 30


class EnrichmentSettings(BaseSettings):
    """Gemini settings loaded only by the enrichment processor."""

    model_config = SettingsConfigDict(
        env_prefix="SIGNALFORGE_",
        env_file=(".env", "../.env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    gemini_api_key: Annotated[SecretStr, Field(min_length=1)]
    gemini_model: Annotated[
        str,
        StringConstraints(strip_whitespace=True, min_length=1, max_length=100),
    ] = "gemini-3.8-flash"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


@lru_cache
def get_enrichment_settings() -> EnrichmentSettings:
    return EnrichmentSettings()  # type: ignore[call-arg]
