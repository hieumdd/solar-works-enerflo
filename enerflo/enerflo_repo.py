from typing import Any
import os

import requests

BASE_URL = "https://enerflo.io/api/v1"
PAGE_SIZE = 500


def get_client() -> requests.Session:
    client = requests.Session()
    client.headers.update({"api-key": os.getenv("ENERFLO_API_KEY", "")})
    return client


def get(endpoint: str):
    def _get(client: requests.Session):
        def __get(page: int = 1) -> list[dict[str, Any]]:
            with client.get(f"{BASE_URL}/{endpoint}", params={
                "pageSize": PAGE_SIZE,
                "page": page,
            }) as r:
                r.raise_for_status()
                res = r.json()
            data = res["data"]
            return data if not data else data + __get(page + 1)

        return __get

    with get_client() as client:
        return _get(client)
