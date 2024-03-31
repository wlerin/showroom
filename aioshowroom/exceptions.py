import aiohttp
from typing import Text


# TODO: determine what I actually need to store in this error
class ClientResponseError(aiohttp.ClientError):
    def __init__(self, status: int, message: Text, text: Text):
        self.status = status
        self.message = message
        self.text = text
        super().__init__(f"{status}, {message}, body='{text}")
