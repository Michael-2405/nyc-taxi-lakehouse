from __future__ import annotations

from pathlib import Path

import pendulum
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseModel):
    name: str
    environment: str
    log_level: str
    timezone: str

    @property
    def tz(self) -> pendulum.Timezone:
        return pendulum.timezone(self.timezone)

class LoggingSettings(BaseModel):
    directory: Path
    max_size_mb: int
    backup_count: int


class HttpSettings(BaseModel):
    timeout: int
    max_retries: int
    user_agent: str


class MinioSettings(BaseModel):
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    raw_bucket: str
    log_bucket: str


class Environment(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )

    APP_NAME: str
    APP_ENV: str
    APP_LOG_LEVEL: str
    APP_TIMEZONE: str

    LOG_DIRECTORY: Path
    LOG_MAX_SIZE_MB: int
    LOG_BACKUP_COUNT: int

    HTTP_TIMEOUT: int
    HTTP_MAX_RETRIES: int
    HTTP_USER_AGENT: str

    MINIO_ENDPOINT: str
    MINIO_ACCESS_KEY: str
    MINIO_SECRET_KEY: str
    MINIO_SECURE: bool
    MINIO_RAW_BUCKET: str
    MINIO_LOG_BUCKET: str


_env = Environment() # type: ignore


class Settings:
    def __init__(self) -> None:
        self.app = AppSettings(
            name=_env.APP_NAME,
            environment=_env.APP_ENV,
            log_level=_env.APP_LOG_LEVEL,
            timezone=_env.APP_TIMEZONE,
        )

        self.logging = LoggingSettings(
            directory=_env.LOG_DIRECTORY,
            max_size_mb=_env.LOG_MAX_SIZE_MB,
            backup_count=_env.LOG_BACKUP_COUNT,
        )

        self.http = HttpSettings(
            timeout=_env.HTTP_TIMEOUT,
            max_retries=_env.HTTP_MAX_RETRIES,
            user_agent=_env.HTTP_USER_AGENT
        )

        self.minio = MinioSettings(
            endpoint=_env.MINIO_ENDPOINT,
            access_key=_env.MINIO_ACCESS_KEY,
            secret_key=_env.MINIO_SECRET_KEY,
            secure=_env.MINIO_SECURE,
            raw_bucket=_env.MINIO_RAW_BUCKET,
            log_bucket=_env.MINIO_LOG_BUCKET,
        )


settings = Settings()
