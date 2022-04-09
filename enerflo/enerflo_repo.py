from typing import Any, Callable
import os
import math
import asyncio

import httpx

BASE_URL = "https://enerflo.io/api/v1"
PAGE_SIZE = 500


async def _get_one(
    client: httpx.AsyncClient,
    uri: str,
    callback_fn: Callable[[dict], Any],
    page: int = 1,
) -> dict[str, Any]:
    r = await client.get(
        uri,
        params={
            "pageSize": PAGE_SIZE,
            "page": page,
        },
    )
    r.raise_for_status()
    res = r.json()
    return callback_fn(res)


def get(uri: str):
    def _get():
        async def __get():
            async with httpx.AsyncClient(
                base_url=BASE_URL,
                headers={"api-key": os.getenv("ENERFLO_API_KEY", "")},
                timeout=None,
            ) as client:
                count = await _get_one(client, uri, lambda x: x["dataCount"])
                pages = math.ceil(count / 500)
                tasks = [
                    asyncio.create_task(
                        _get_one(
                            client,
                            uri,
                            lambda x: x["data"],
                            page,
                        )
                    )
                    for page in range(1, pages + 1)
                ]
                return await asyncio.gather(*tasks)

        return [i for j in asyncio.run(__get()) for i in j]

    return _get
