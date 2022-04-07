from typing import Any
import os

import requests

BASE_URL = "https://enerflo.io/api/v1"


def get_client() -> requests.Session:
    client = requests.Session()
    client.headers.update({"api-key": os.getenv("ENERFLO_API_KEY", "")})
    return client


def get(endpoint: str):
    def _get(client: requests.Session):
        def __get() -> list[dict[str, Any]]:
            with client.get(f"{BASE_URL}/{endpoint}") as r:
                res = r.json()
            return res["data"]

        return __get

    with get_client() as client:
        return _get(client)
