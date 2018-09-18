from .session import ClientSession
from .endpoints import (
    LiveEndpointsMixin,
    VREndpointsMixin,
    RoomEndpointsMixin,
    UserEndpointsMixin,
    OtherEndpointsMixin
)
from json import JSONDecodeError
import time
from showroom.api.utils import get_csrf_token
from requests.exceptions import HTTPError
import logging
_base_url = 'https://www.showroom-live.com'

# TODO: logging, warnings
# TODO: load auth or credentials from file or dict
# TODO: save auth or credentials to file
client_logger = logging.getLogger('showroom.client')


class ShowroomClient(
    LiveEndpointsMixin,
    UserEndpointsMixin,
    RoomEndpointsMixin,
    VREndpointsMixin,
    OtherEndpointsMixin
):
    """
    Client for interacting with the Showroom API.
    
    :param cookies: dict containing stored cookies
    
    :ivar cookies: Reference to the underlying session's cookies.
    """
    def __init__(self, cookies=None):
        self._session = ClientSession()
        self._auth = None
        self.cookies = self._session.cookies

        if cookies:
            self.cookies.update(cookies)
            expiry = self.cookies.expires_earliest
            if expiry and int(time.time()) >= expiry:
                # TODO: more information, more specific error
                raise ValueError('A cookie has expired')
            # TODO: does this actually mean we're logged in? if no, how do I check?
            self._auth = self.cookies.get('sr_id')

        # TODO: request responses in different languages
        # to force japanese text in responses:
        # self.session.cookies.update({'lang': 'ja'})
        # this doesn't always seem to work? it worked until i manually set lang:en, then switching back failed

        self.__csrf_token = None
        self._last_response = None

    @property
    def _csrf_token(self):
        if not self.__csrf_token:
            self._update_csrf_token(_base_url)
        return self.__csrf_token

    def _update_csrf_token(self, url):
        r = self._session.get(url)

        self.__csrf_token = get_csrf_token(r.text)

    def _api_get(self, endpoint, params=None, return_response=False, default=None, raise_error=True):
        try:
            r = self._session.get(_base_url + endpoint, params=params)
        except HTTPError as e:
            r = e.response
            if raise_error:
                raise
        self._last_response = r

        if return_response:
            return r
        else:
            try:
                return r.json()
            except JSONDecodeError as e:
                client_logger.error('JSON decoding error while getting {}: {}'.format(r.request.url, e))
                return default or {}

    def _api_post(self, endpoint, params=None, data=None, return_response=None, default=None):
        try:
            r = self._session.post(_base_url + endpoint, params=params, data=data)
        except HTTPError as e:
            r = e.response
        self._last_response = r

        # TODO: check for expired csrf_token
        if return_response:
            return r
        else:
            try:
                return r.json()
            except JSONDecodeError as e:
                client_logger.error('JSON decoding error while posting to {}: {}'.format(r.request.url, e))
                return default or {}


