from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

import structlog

from nyc_taxi_lakehouse.config.settings import settings

def configure_logging() -> None:
  settings.logging.directory.mkdir(
    parents=True, exist_ok=True
  )

  formatter = logging.Formatter("%(message)s")

  root_logger = logging.getLogger()
  root_logger.handlers.clear()

  root_logger.setLevel(settings.app.log_level)

  console = logging.StreamHandler()
  console.setFormatter(formatter)

  application = RotatingFileHandler(
    settings.logging.directory / "application.log",
    maxBytes=settings.logging.max_size_mb * 1024 * 1024,
    backupCount=settings.logging.backup_count,
    encoding="utf-8"
  )

  application.setFormatter(formatter)

  errors = RotatingFileHandler(
    settings.logging.directory / "errors.log",
    maxBytes=settings.logging.max_size_mb * 1024 * 1024,
    backupCount=settings.logging.backup_count,
    encoding="utf-8"
  )

  errors.setLevel(logging.ERROR)
  errors.setFormatter(formatter)

  root_logger.addHandler(console)
  root_logger.addHandler(application)
  root_logger.addHandler(errors)

  structlog.configure(
    processors=[
      structlog.contextvars.merge_contextvars,
      structlog.processors.TimeStamper(fmt="iso"),
      structlog.processors.add_log_level,
      structlog.processors.StackInfoRenderer(),
      structlog.processors.format_exc_info,
      structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.make_filtering_bound_logger(
      logging.getLevelName(settings.app.log_level)
    ),
    cache_logger_on_first_use=True
  )
