from requests import Session as _Session
from requests.exceptions import ConnectionError, ChunkedEncodingError, Timeout, HTTPError
from requests.adapters import HTTPAdapter
import logging
import time
from .cookiejar import ClientCookieJar

#try:
#    from fake_useragent import UserAgent
#except ImportError:
#    UserAgent = None
#    ua = None
ua_str = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
#else:
#    ua = UserAgent(fallback='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36')
#    ua_str = ua.chrome

session_logger = logging.getLogger('showroom.session')


class ClientSession(_Session):
    """
    Wrapper for requests.Session.

    Mainly used to catch temporary errors and set a Timeout

    Overrides requests.Session.get() and increases max pool size

    Raises:
        May raise TimeoutError, ConnectionError, HTTPError, or ChunkedEncodingError
        if retries are exceeded.
    """

    # TODO: set pool_maxsize based on config
    def __init__(self, pool_maxsize=100):
        super().__init__()
        self.cookies = ClientCookieJar()
        https_adapter = HTTPAdapter(pool_maxsize=pool_maxsize)
        self.mount('https://www.showroom-live.com', https_adapter)
        self.headers = {"User-Agent": ua_str}

    # TODO: post
    def get(self, url, params=None, max_delay=30.0, max_retries=20, **kwargs):
        error_count = 0
        wait = 0
        timeouts = 0
        while True:
            try:
                r = super().get(url, params=params, timeout=(3.0, 15.0), **kwargs)
                r.raise_for_status()
            except Timeout as e:
                session_logger.debug('Timeout while fetching {}: {}'.format(url, e))
                timeouts += 1
                wait = min(2 * 1.5 ** timeouts, max_delay*4)

                if timeouts > max_retries:
                    session_logger.error('Max timeouts exceeded while fetching {}: {}'.format(url, e))
                    # raise
                elif timeouts > max_retries // 2:
                    session_logger.warning('{} timeouts while fetching {}: {}'.format(timeouts, url, e))

            except ChunkedEncodingError as e:
                session_logger.debug('Chunked encoding error while fetching {}: {}'.format(url, e))
                error_count += 1
                wait = min(wait + error_count, max_delay)

                if error_count > max_retries:
                    session_logger.warning('Max retries exceeded while fetching {}: {}'.format(url, e))
                    raise

            except HTTPError as e:
                status_code = e.response.status_code
                session_logger.debug('{} while fetching {}: {}'.format(status_code, url, e))

                error_count += 1
                wait = min(wait + 2 + error_count, max_delay)

                # Some of these aren't recoverable
                if status_code == 404:
                    session_logger.error('Getting {} failed permanently: 404 page not found'.format(url))
                    raise  # PageNotFoundError(e)  # ?
                elif status_code == 403:
                    session_logger.error('Getting {} failed permanently: 403 permission denied'.format(url))
                    raise  # specific error?
                elif status_code == 402:
                    session_logger.error('Getting {} failed permanently: '
                                         '401 auth required (not implemented)'.format(url))
                    raise
                elif status_code == 429:
                    session_logger.error('Too many requests while getting {}: {}'.format(url, e))
                    wait += 5 * 60.0
                elif 400 <= status_code < 500:
                    session_logger.error('Getting {} failed permanently: {}'.format(url, e))
                    raise

                if error_count > max_retries:
                    session_logger.warning('Max retries exceeded while fetching {}: {}'.format(url, e))
                    raise

            except ConnectionError as e:
                session_logger.debug('ConnectionError while accessing {}: {}'.format(url, e))

                error_count += 1
                wait = min(wait + 2 * error_count, max_delay)

                # ConnectionErrors are assumed to be always recoverable
                # if error_count > max_retries:
                #     session_logger.warning('Max retries exceeded while fetching {}: {}'.format(url, e))
                #     raise

            else:
                return r

            session_logger.debug('Retrying in {} seconds...'.format(wait))
            time.sleep(wait)