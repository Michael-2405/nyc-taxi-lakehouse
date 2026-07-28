from __future__ import annotations

from dataclasses import dataclass

import pendulum


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


@dataclass(slots=True, frozen=True)
class DatasetFile:
    year: int
    month: int

    @property
    def filename(self) -> str:
        return f"yellow_tripdata_{self.year}-{self.month:02d}.parquet"

    @property
    def url(self) -> str:
        return f"{BASE_URL}/{self.filename}"

    @property
    def object_name(self) -> str:
        return (
            f"yellow/"
            f"year={self.year}/"
            f"month={self.month:02d}/"
            f"{self.filename}"
        )


def build_manifest( start_year: int, start_month: int, end_year: int, end_month: int) -> list[DatasetFile]:

    start = pendulum.date(start_year, start_month, 1)
    end = pendulum.date(end_year, end_month, 1)

    manifest: list[DatasetFile] = []

    current = start

    while current <= end:
        manifest.append(
            DatasetFile(
                year=current.year,
                month=current.month,
            )
        )

        current = current.add(months=1)

    return manifest
