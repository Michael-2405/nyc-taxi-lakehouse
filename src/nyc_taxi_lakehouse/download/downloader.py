from __future__ import annotations

from fileinput import filename
from pathlib import Path

import structlog

from nyc_taxi_lakehouse.download.client import HttpClient
from nyc_taxi_lakehouse.download.manifest import DatasetFile

logger = structlog.get_logger(__name__)
class Downloader:
    def __init__(self) -> None:
        self.http = HttpClient()

    def download(
            self,
            dataset: DatasetFile,
            destination: Path
    ) -> Path:

        destination.mkdir(parents=True, exist_ok=True)

        output = destination / dataset.filename

        logger.info(
            "Downloading dataset",
            filename=dataset.filename,
            url=dataset.url
        )

        with self.http.client.stream("GET", dataset.url) as response:
            response.raise_for_status()

            with output.open("wb") as file:
                for chunk in response.iter_bytes():
                    file.write(chunk)

        logger.info(
            "Download completed",
            file_path=str(output)
        )

        return output
