from functools import lru_cache
from typing import Annotated

from pydantic import Field, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


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


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
