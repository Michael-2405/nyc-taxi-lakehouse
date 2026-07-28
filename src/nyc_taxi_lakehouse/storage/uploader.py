from __future__ import annotations

from pathlib import Path

import structlog
from minio.error import S3Error

from nyc_taxi_lakehouse.storage.client import StorageClient

logger = structlog.get_logger(__name__)
class Uploader:
    def __init__(self) -> None:
        self.storage = StorageClient()

    def upload(
        self,
        file_path: Path,
        bucket_name: str,
        object_name: str,
    ) -> None:

        self.storage.create_bucket_if_not_exists(bucket_name)

        if self.storage.object_exists(bucket_name, object_name):
            logger.info(
                "Object already exists, skipping upload",
                file_path=str(file_path),
                bucket=bucket_name,
                object_name=object_name
            )
            return

        try:
            self.storage.client.fput_object(
                bucket_name=bucket_name,
                object_name=object_name,
                file_path=str(file_path)
            )

            logger.info(
                "Upload completed",
                bucket=bucket_name,
                object_name=object_name
            )

        except S3Error:
            logger.exception(
                "Upload failed",
                bucket=bucket_name,
                object_name=object_name
            )
            raise
