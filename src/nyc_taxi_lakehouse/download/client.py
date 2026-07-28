from __future__ import annotations

import httpx

from nyc_taxi_lakehouse.config.settings import settings


class HttpClient:
    def __init__(self) -> None:
        self._client = httpx.Client(
            timeout=settings.http.timeout,
            headers={
                "User-Agent": "nyc-taxi-lakehouse/1.0",
            },
            follow_redirects=True,
        )

    @property
    def client(self) -> httpx.Client:
        return self._client

    def close(self) -> None:
        self._client.close()
