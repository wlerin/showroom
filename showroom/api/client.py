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

_base_url = 'https://www.showroom-live.com'

# TODO: logging, warnings
# TODO: load auth or credentials from file or dict
# TODO: save auth or credentials to file


class Client(
    LiveEndpointsMixin,
    UserEndpointsMixin,
    RoomEndpointsMixin,
    VREndpointsMixin,
    OtherEndpointsMixin
):
    def __init__(self, cookies=None, settings=None):
        self._session = ClientSession()
        # TODO: auth
        self._auth = None
        self.cookies = self._session.cookies

        if cookies or (settings and settings.get('cookies')):
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

        self._csrf_token = None
        self._last_response = None

    @property
    def csrf_token(self):
        if not self._csrf_token:
            self.update_csrf_token(_base_url)
        return self._csrf_token

    def update_csrf_token(self, url):
        r = self._session.get(url)

        self._csrf_token = get_csrf_token(r.text)

    def _api_get(self, endpoint, params=None, return_response=False, default=None):
        r = self._session.get(_base_url + endpoint, params=params)
        self._last_response = r

        if return_response:
            return r
        else:
            try:
                return r.json()
            except JSONDecodeError:
                # TODO: log
                return default or {}

    def _api_post(self, endpoint, params=None, data=None, return_response=None, default=None):
        r = self._session.post(_base_url + endpoint, params=params, data=data)
        self._last_response = r

        if return_response:
            return r
        else:
            try:
                return r.json()
            except JSONDecodeError:
                # TODO: log
                return default or {}

    @property
    def is_authenticated(self):
        return bool(self._auth)


