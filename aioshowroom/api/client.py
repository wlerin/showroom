from .session import ClientSession
from json import JSONDecodeError
from aiohttp.typedefs import StrOrURL
from typing import Text, Dict, Any


# only the room and live endpoints seem relevant to recordings
class LiveEndpointsMixin:
    """
    Implemented:
    comment_log
    live_info
    onlives
    streaming_url
    telop
    upcoming

    Not Implemented (unused, might add at a later date):
    onlive_num
    bradaru_texts
    gift_list
    gift_log
    summary_ranking
    stage_user_list
    stage_gift_list
    polling
    enquete_result

    Never Implemented (requires auth, will never add):
    verify_age
    send_free_gift
    send_paid_gift
    send_comment
    """
    # TODO: verify and document the return values
    # TODO: send the request timestamp as a param the way the browser does
    async def comment_log(self, room_id: int, is_delay: int = None) -> list:
        endpoint = '/live/comment_log'
        # TODO: what is is_delay for?
        results = await self._api_get(endpoint, params=dict(room_id=room_id))
        return results.get('comment_log')

    async def live_info(self, room_id: int) -> dict:
        endpoint = '/live/live_info'
        results = await self._api_get(endpoint, params=dict(room_id=room_id))
        return results

    async def onlives(self) -> dict:
        endpoint = '/live/onlives'
        results = await self._api_get(endpoint)
        return results.get('onlives')

    async def streaming_url(self, room_id: int) -> list:
        endpoint = '/live/streaming_url'
        results = await self._api_get(endpoint, params=dict(room_id=room_id))
        return results.get('streaming_url_list')

    async def telop(self, room_id: int) -> str:
        endpoint = '/live/telop'
        results = await self._api_get(endpoint, params=dict(room_id=room_id))
        return results.get('telop')

    async def upcoming(self, genre_id: int) -> list:
        endpoint = '/live/upcoming'
        results = await self._api_get(endpoint, params=dict(genre_id=genre_id))
        return results.get('upcomings', [])


class RoomEndpointsMixin:
    """
    Implemented:
    settings
    next_live
    status
    profile

    Not Implemented:
    event_and_support
    contribution_ranking
    support_users
    banners

    Never Implemented:
    follow
    """
    async def next_live(self, room_id: int) -> int:
        endpoint = '/room/next_live'
        results = await self._api_get(endpoint, params=dict(room_id=room_id))
        return results.get('epoch')

    # do I really need to prepend room_ on these?
    async def room_profile(self, room_id: int) -> dict:
        endpoint = '/room/profile'
        results = await self._api_get(endpoint, params=dict(room_id=room_id))
        return results

    async def room_settings(self, room_id: int) -> dict:
        endpoint = '/room/settings'
        results = await self._api_get(endpoint, params=dict(room_id=room_id))
        return results

    async def room_status(self, room_url_key: str) -> dict:
        endpoint = '/room/status'
        results = await self._api_get(endpoint, params=dict(room_id=room_url_key))
        return results


class ShowroomClient(
    LiveEndpointsMixin,
    RoomEndpointsMixin
):
    def __init__(self):
        # TODO: does the session need to be closed properly? do I need __aexit__ and __aenter__ ?
        self.session = ClientSession(base_url='https://www.showroom-live.com/api')
        # TODO: do I need to do anything with cookies?

    # TODO: verify that this old design actually needs all these bits
    async def _api_get(self,
                       endpoint: StrOrURL,
                       params: Dict | None = None,
                       response_method: Text = 'json',
                       default: Any = None,
                       raise_error: bool = True
                       ):
        # TODO: retry on recoverable errors?
        async with self.session.get(endpoint, params=params) as response:
            if response.status >= 400:
                # TODO: properly handle errors, including returning the response as needed
                if raise_error:
                    response.raise_for_status()
            else:
                try:
                    return await getattr(response, response_method)
                except JSONDecodeError:
                    return default or {}
