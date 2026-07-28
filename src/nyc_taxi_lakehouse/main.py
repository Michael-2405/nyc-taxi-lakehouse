from pathlib import Path
from sys import exception

import structlog

from nyc_taxi_lakehouse.config.logging import configure_logging
from nyc_taxi_lakehouse.config.settings import settings
from nyc_taxi_lakehouse.download.downloader import Downloader
from nyc_taxi_lakehouse.download.manifest import build_manifest
from nyc_taxi_lakehouse.storage.uploader import Uploader

logger= structlog.get_logger(__name__)

def main() -> None:
  configure_logging()

  logger.info("Pipeline started")

  manifest = build_manifest(start_year=2023, start_month=1, end_year=2025, end_month=12)

  downloader = Downloader()
  uploader = Uploader()

  for dataset in manifest:
    logger.info("Processing dataset", filename=dataset.filename)

    file = downloader.download(dataset, Path("data/downloads"))

    try:
      uploader.upload(file_path=file, bucket_name=settings.minio.raw_bucket, object_name=dataset.object_name)

    except Exception:
      logger.error("Upload failed, keeping local file for retry", file_path=str(file))
      raise

    else:
      file.unlink()
      logger.info("Temporary file removed", file_path=str(file))

  logger.info("Pipeline finished")

if __name__ == "__main__":
  main()
