#!/usr/bin/env python3
# if having trouble connecting, try using this instead of the basic showroom.py,
# with HTTP_PROXY/http_proxy environment variables set
import showroom.core

BaseDownloader = showroom.core.Downloader


class PatchedDownloader(BaseDownloader):
    @property
    def stream_url(self):
        return super().stream_url.replace('rtmp://', 'rtmpt://')


showroom.core.Downloader = PatchedDownloader

from showroom.main import main

if __name__ == "__main__":
    main()
