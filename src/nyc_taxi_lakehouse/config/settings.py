from __future__ import annotations

import os
from pathlib import Path

import pendulum
from pydantic import BaseModel
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ENV_FILE_PATH = os.environ.get("ENV_FILE_PATH", str(PROJECT_ROOT / ".env"))

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
    root_user: str
    root_password: str
    secure: bool
    bucket_name: str
    raw_prefix: str
    bronze_prefix: str
    silver_prefix: str
    gold_prefix: str
    reference_prefix: str
    log_bucket: str



class Environment(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ENV_FILE_PATH,
        extra="ignore",
    )

    # APP
    APP_NAME: str
    APP_ENV: str
    APP_LOG_LEVEL: str
    APP_TIMEZONE: str

    # LOGS
    LOG_DIRECTORY: Path
    LOG_MAX_SIZE_MB: int
    LOG_BACKUP_COUNT: int

    # HTTP
    HTTP_TIMEOUT: int
    HTTP_MAX_RETRIES: int
    HTTP_USER_AGENT: str

    # MINIO
    MINIO_ENDPOINT: str
    MINIO_ROOT_USER: str
    MINIO_ROOT_PASSWORD: str
    MINIO_SECURE: bool
    MINIO_BUCKET_NAME: str
    MINIO_RAW_PREFIX: str
    MINIO_BRONZE_PREFIX: str
    MINIO_SILVER_PREFIX: str
    MINIO_GOLD_PREFIX: str
    MINIO_REFERENCE_PREFIX: str
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
            root_user=_env.MINIO_ROOT_USER,
            root_password=_env.MINIO_ROOT_PASSWORD,
            secure=_env.MINIO_SECURE,
            bucket_name=_env.MINIO_BUCKET_NAME,
            raw_prefix=_env.MINIO_RAW_PREFIX,
            bronze_prefix=_env.MINIO_BRONZE_PREFIX,
            silver_prefix=_env.MINIO_SILVER_PREFIX,
            gold_prefix=_env.MINIO_GOLD_PREFIX,
            reference_prefix=_env.MINIO_REFERENCE_PREFIX,
            log_bucket=_env.MINIO_LOG_BUCKET,
        )


settings = Settings()
