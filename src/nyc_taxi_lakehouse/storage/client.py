from __future__ import annotations

import structlog
from minio import Minio
from minio.error import S3Error

from nyc_taxi_lakehouse.config.settings import settings

logger = structlog.get_logger(__name__)

class StorageClient:
    def __init__(self) -> None:
        self._client = Minio(
            endpoint=settings.minio.endpoint,
            access_key=settings.minio.access_key,
            secret_key=settings.minio.secret_key,
            secure=settings.minio.secure,
        )

    @property
    def client(self) -> Minio:
        return self._client

    def create_bucket_if_not_exists(self, bucket_name: str) -> None:
        try:
            if self._client.bucket_exists(bucket_name):
                logger.info("Bucket already exists", bucket=bucket_name)
                return
            self._client.make_bucket(bucket_name)
            logger.info("Bucket created", bucket=bucket_name)

        except S3Error as exc:
            if exc.code == "BucketAlreadyOwnedByYou":
                logger.info("Bucket already exists (race on create)", bucket=bucket_name)
                return

            logger.exception("Failed to create bucket", bucket=bucket_name)
            raise

    def object_exists(self, bucket_name: str, object_name: str) -> bool:
        try:
            self._client.stat_object(bucket_name, object_name)
            return True

        except S3Error as exc:
            if exc.code == "NoSuchKey":
                return False

            logger.exception("Failed to check object existence", bucket=bucket_name, object_name=object_name)
            raise
