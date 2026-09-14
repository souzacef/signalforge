from functools import lru_cache
from typing import Annotated, Self

from pydantic import (
    Field,
    HttpUrl,
    PostgresDsn,
    SecretStr,
    StringConstraints,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from signalforge.remediation.targets import ServiceTarget

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


class TracingSettings(BaseSettings):
    """API tracing settings that do not require a collector by default."""

    model_config = SettingsConfigDict(
        env_prefix="SIGNALFORGE_",
        env_file=(".env", "../.env"),
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )

    tracing_enabled: bool = False
    otlp_traces_endpoint: HttpUrl | None = None
    tracing_sample_ratio: Annotated[
        float,
        Field(ge=0, le=1, allow_inf_nan=False),
    ] = 1.0

    @model_validator(mode="after")
    def require_export_endpoint_when_enabled(self) -> Self:
        if self.tracing_enabled and self.otlp_traces_endpoint is None:
            raise ValueError("otlp_traces_endpoint is required when tracing is enabled")
        return self


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


class RemediationExecutionSettings(BaseSettings):
    """Settings loaded only by trusted remediation execution code."""

    model_config = SettingsConfigDict(
        env_prefix="SIGNALFORGE_REMEDIATION_",
        env_file=(".env", "../.env"),
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
    )

    restart_endpoints: dict[ServiceTarget, HttpUrl] = Field(default_factory=dict)
    request_timeout_seconds: Annotated[float, Field(gt=0, le=30)] = 5.0

    @field_validator("restart_endpoints")
    @classmethod
    def reject_endpoint_url_extras(
        cls,
        endpoints: dict[str, HttpUrl],
    ) -> dict[str, HttpUrl]:
        for endpoint in endpoints.values():
            if endpoint.username is not None or endpoint.password is not None:
                raise ValueError("restart endpoint must not contain credentials")
            if endpoint.query is not None:
                raise ValueError("restart endpoint must not contain a query string")
            if endpoint.fragment is not None:
                raise ValueError("restart endpoint must not contain a fragment")
        return endpoints


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


@lru_cache
def get_enrichment_settings() -> EnrichmentSettings:
    return EnrichmentSettings()  # type: ignore[call-arg]


@lru_cache
def get_tracing_settings() -> TracingSettings:
    return TracingSettings()


@lru_cache
def get_remediation_execution_settings() -> RemediationExecutionSettings:
    return RemediationExecutionSettings()
