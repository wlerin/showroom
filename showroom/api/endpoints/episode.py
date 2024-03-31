# https://www.showroom-live.com/api/episode/streaming_url?episode_id=34
import binascii
EPISODE_KEY = binascii.unhexlify('2A63847146F96DD3A17077F6C72DAFFB')
EPISODE_IV = binascii.unhexlify('553BCF6FFFFF412943A6BCE54FCB7E81')


class EpisodeEndpointsMixin:
    """
    For endpoints in */api/episode/*
    """
    def episode_streaming_url(self, episode_id):
        # get cloudflare cookies
        r = self._session.get(f'https://www.showroom-live.com/episode/watch?id={episode_id}')
        # self.__csrf_token = get_csrf_token(r.text)
        endpoint = "/api/episode/streaming_url"
        results = self._api_get(endpoint, params=dict(episode_id=episode_id))
        streaming_url = results['streaming_url_list'].get('hls_source', {}).get('hls')
        return streaming_url

    @property
    def _episode_key(self):
        return EPISODE_KEY

    @property
    def _episode_iv(self):
        return EPISODE_IV
    
