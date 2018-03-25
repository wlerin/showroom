from .session import ClientSession
from .endpoints import (
    LiveEndpointsMixin,
    VREndpointsMixin,
    RoomEndpointsMixin,
    UserEndpointsMixin,
    OtherEndpointsMixin
)
from json import JSONDecodeError

_base_url = 'https://www.showroom-live.com/'

# TODO: logging, warnings
# TODO: load auth or credentials from file or dict


class Client(
    LiveEndpointsMixin,
    UserEndpointsMixin,
    RoomEndpointsMixin,
    VREndpointsMixin,
    OtherEndpointsMixin
):
    def __init__(self):
        self._session = ClientSession()
        # TODO: auth
        self._auth = None

        # TODO: request responses in different languages

        # to force japanese text in responses:
        # self.session.cookies.update({'lang': 'ja'})
        # this doesn't always seem to work? it worked until i manually set lang:en, then switching back failed

    def _call_api(self, method, endpoint, params=None):
        # TODO: is this useful? or is session enough?
        pass

    def _api_get(self, endpoint, params=None, return_response=False, default=None):
        r = self._session.get(_base_url + endpoint, params=params)
        if r.ok:
            if return_response:
                return r
            else:
                try:
                    return r.json()
                except JSONDecodeError:
                    # TODO: log
                    return default or {}

    def _api_post(self, endpoint, params=None, data=None):
        # r = self._session.post(_base_url + endpoint, params=params, data=data)
        raise NotImplementedError

    @property
    def is_authenticated(self):
        return bool(self._auth)


