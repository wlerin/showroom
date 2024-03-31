import aiofiles
DEFAULT_CHUNKSIZE = 4096


async def save_segment(url, destfile, session, headers=None, timeout=None, attempts=None):
    async with aiofiles.open(destfile, 'wb') as outfp:
        async with session.get(url) as response:
            if response.status == 404:
                # TODO: more elaborate error handling, cf. hls.py
                return
            async for chunk in response.content.iter_chunked(DEFAULT_CHUNKSIZE):
                await outfp.write(chunk)


class Downloader:
    pass
