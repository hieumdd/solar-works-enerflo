from typing import Any, Callable
import os
import math
import asyncio

import httpx

BASE_URL = "https://enerflo.io/api/v1"
PAGE_SIZE = 500


async def _get_one(
    client: httpx.AsyncClient,
    url: str,
    callback_fn: Callable[[dict], Any],
    page: int = 1,
) -> dict[str, Any]:
    r = await client.get(
        url,
        params={
            "pageSize": PAGE_SIZE,
            "page": page,
        },
    )
    r.raise_for_status()
    res = r.json()
    return callback_fn(res)


def get(endpoint: str):
    def _get():
        async def __get():
            async with httpx.AsyncClient(
                headers={"api-key": os.getenv("ENERFLO_API_KEY", "")},
                timeout=None,
            ) as client:
                url = f"{BASE_URL}/{endpoint}"
                count = await _get_one(client, url, lambda x: x["dataCount"])
                pages = math.ceil(count / 500)
                tasks = [
                    asyncio.create_task(
                        _get_one(
                            client,
                            url,
                            lambda x: x["data"],
                            page,
                        )
                    )
                    for page in range(1, pages + 1)
                ]
                return await asyncio.gather(*tasks)

        return [i for j in asyncio.run(__get()) for i in j]

    return _get
