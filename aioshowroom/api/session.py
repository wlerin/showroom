import aiohttp
from typing import Dict

# TODO: logging
DEFAULT_HEADERS = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36",
    }
DEFAULT_REQUEST_TIMEOUT = 120


class ClientSession(aiohttp.ClientSession):
    def __init__(self,
                 headers: Dict | None = None,
                 cookies: Dict | None = None,
                 timeout: int | None = None,
                 **kwargs):
        if not headers:
            headers = DEFAULT_HEADERS
        super().__init__(headers=headers,
                         cookies=cookies,
                         timeout=timeout or aiohttp.ClientTimeout(total=DEFAULT_REQUEST_TIMEOUT),
                         **kwargs)
