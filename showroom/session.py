from requests import Session
from requests.exceptions import ConnectionError, ChunkedEncodingError, Timeout, ReadTimeout, HTTPError
from requests.adapters import HTTPAdapter
from fake_useragent import UserAgent
import logging
import time
import json

ua = UserAgent(fallback='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                        '(KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36', )


class WatchSession(Session):
    """
    Wrapper for requests.Session.

    Mainly used to catch temporary errors and set a Timeout

    Overrides requests.Session.get() and increases max pool size

    Raises:
        May raise TimeoutError, ConnectionError, or ChunkedEncodingError
        if retries are exceeded.
    """

    # TODO: set pool_maxsize based on config
    def __init__(self, pool_maxsize=100):
        super().__init__()
        https_adapter = HTTPAdapter(pool_maxsize=pool_maxsize)
        self.mount('https://www.showroom-live.com', https_adapter)
        self.headers = {"UserAgent": ua.chrome}

    def get(self, url, params=None, **kwargs):
        error_count = 0
        max_retries = 20
        while True:
            try:
                r = super().get(url, params=params, timeout=(2.0, 10.0), **kwargs)
                r.raise_for_status()
            except (Timeout, ReadTimeout, ConnectionError, ChunkedEncodingError, HTTPError) as e:
                logging.debug('Get of {} failed with {}'.format(url, e))
                error_count += 1
                if error_count > max_retries:
                    raise
                time.sleep(0.5 + 0 if error_count < 4 else error_count - 3)
            else:
                return r

    def json(self, url, default=None, params=None, **kwargs):
        try:
            r = self.get(url, params=params, **kwargs).json()
        except json.JSONDecodeError:
            return default
        else:
            return r
