# generic methods for downloading video (streaming or otherwise)
import re
import os
import glob
import time
import datetime
from multiprocessing.dummy import Process, Pool, JoinableQueue as Queue
import logging

import requests
import m3u8
from m3u8 import _parsed_url

hls_logger = logging.getLogger('showroom.hls')
filename_re = re.compile(r'/([\w=\-_]+\.ts)')

# TODO: make these configurable
FILENAME_PADDING = 6
NUM_WORKERS = 16
DEFAULT_CHUNKSIZE = 4096
MAX_ATTEMPTS = 5

# TODO: inherit headers from config
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
}
TIMEOUT = 3


def default_segment_sort_key(file):
    """
    Returns the last contiguous number in a filename as an integer

    e.g. given the filename "bob-and-cut=3516.ts"
    will return 3516
    """
    return int(re.findall(r'(\d+)', file)[-1])


def load_m3u8(src, url=None, headers=None):
    # is playlist raw text, a path to a file, or a url?
    if src.upper().startswith('#EXTM3U'):
        m3u8_obj = m3u8.loads(src)
        m3u8_obj.playlist_url = url
    elif os.path.exists(src):
        m3u8_obj = m3u8.load(src)
        # is this going to split the url correctly?
        m3u8_obj.playlist_url = url
        m3u8_obj.base_uri = _parsed_url(url)
    else:

        m3u8_obj = m3u8.load(src, headers=headers or DEFAULT_HEADERS)
        m3u8_obj.playlist_url = src
    return m3u8_obj


def is_m3u8_content(playlist):
    if playlist.startswith('#EXTM3U'):
        return True
    else:
        return False


def save_segment(url, destfile, key=None, iv=None, headers=None, timeout=None, attempts=None):
    if not headers:
        headers = {}
        headers.update(DEFAULT_HEADERS)
    chunksize = DEFAULT_CHUNKSIZE
    exc = None

    if timeout is None:
        timeout = TIMEOUT
    if attempts is None:
        attempts = MAX_ATTEMPTS

    for attempt in range(attempts):
        time.sleep(attempt)
        r = requests.get(url, headers=headers, timeout=timeout, stream=True)
        try:
            r.raise_for_status()
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            # live downloads should just exit immediately on 404 errors
            exc = e
            status = e.response.status_code
            if status == 404:
                raise
            continue

        if key:
            # TODO: handle keys
            pass
        else:
            with open(destfile, 'wb') as outfp:
                try:
                    for chunk in r.iter_content(chunk_size=chunksize):
                        if chunk:
                            outfp.write(chunk)
                except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                    hls_logger.debug(destfile, e)
                    exc = e
                    continue
                else:
                    break
                finally:
                    r.close()
    else:
        try:
            os.remove(destfile)
        except FileNotFoundError:
            pass
        raise exc


# TODO: better (i.e. more consistent) session handling when fetching m3u8 contents
# TODO: flag to use session only for fetching key, for fetching key and playlists, or fetching all three.
# 6 digit media_000000.ts file format
# this gets trickier with live recordings, but you can't really redo a live recording so who cares
def download_hls_video(
        src, dest, url=None,
        # authentication options
        session=None, headers=None, cookies=None,
        # how to use auth
        # best solution is to use advanced cookies that inform requests which domains to use which on
        auth_mode=0, skip_exists=False, use_original_filenames=True,
    ):
    """
    Download hls streaming video

    :param playlist: playlist source
    :param dest: destination, will be created as a folder containing individual segments,
        and then a merged ts file with the same name
    :param url: url of the playlist, used to set the base_uri if a file or raw text is given as playlist
    :param session:
    :param headers:
    :param cookies:
    :param auth_mode: 0/None - keys only, 1 - keys and playlists, 2 - keys, playlists, segments
    :param skip_exists: Whether to skip existing files (for VOD)
    :return:
    """
    if not session:
        # TODO: use a session with a Social48CookieJar instead that accepts more advanced cookie formats
        session = requests.Session()
    if headers:
        session.headers.update(headers)
    if cookies:
        session.cookies.update(cookies)

    if auth_mode >= 1:
        playlist_headers = headers or dict(session.headers)
        if auth_mode >= 2:
            segment_headers = headers or dict(session.headers)
        else:
            segment_headers = {}
    else:
        playlist_headers = {}
        segment_headers = {}

    m3u8_obj = load_m3u8(src, url, playlist_headers)
    # is this a variant playlist? (are there nested variant playlists???)
    while m3u8_obj.is_variant:
        # select the best playlist
        # https://tools.ietf.org/html/rfc8216
        # "Every EXT-X-STREAM-INF tag MUST include the BANDWIDTH attribute." -- 4.3.4.2
        best_variant = sorted((item for item in m3u8_obj.playlists), key=lambda x: x.stream_info.bandwidth)[-1]
        request_start = datetime.datetime.now()
        m3u8_obj = load_m3u8(best_variant.absolute_uri, headers=playlist_headers)

    if not m3u8_obj.is_endlist:
        # calculate when to fetch the next playlist
        total_duration = sum(x.duration for x in m3u8_obj.segments)
        target_duration = m3u8_obj.target_duration
        sleep_delta = datetime.timedelta(seconds=min(total_duration//3+1, target_duration))

    # TODO: split this into a separate function
    # is there a key? or even multiple keys?
    if len(m3u8_obj.keys) > 1 or m3u8_obj.keys[0] is not None:
        # TODO: fetch keys
        # how to handle multiple keys?
        key = None
        iv = None
    else:
        # I don't care about keys and iv for now
        key = None
        iv = None

    known_segments = set()
    ts_queue = Queue()
    error_queue = Queue()

    workers = []
    # TODO: setup workers
    for i in range(NUM_WORKERS):
        p = Process(target=_worker, args=(ts_queue, error_queue, save_segment))
        p.start()
        workers.append(p)

    os.makedirs(dest, exist_ok=True)
    filename_pattern = 'media_{:0%dd}.ts' % FILENAME_PADDING

    def sleep(sleepdt):
        time.sleep(max((sleepdt - datetime.datetime.now()).total_seconds(), 1))

    # check if is_endlist, if not keep looping and downloading new segments
    while True:
        # This isn't great for DMM or other live sources
        # as the segment index won't be comparable across recordings
        # also it can't be used to determine the iv
        index = m3u8_obj.media_sequence or 1
        new_segments = 0
        for segment in m3u8_obj.segments:
            if segment in known_segments:
                continue
            new_segments+=1
            segment_url = segment.absolute_uri
            if use_original_filenames:
                m = filename_re.search(segment_url)
                if not m:
                    filename = filename_pattern.format(index)
                else:
                    filename = m.group(1)
            else:
                filename = filename_pattern.format(index)
            outpath = '{}/{}'.format(dest, filename)
            if skip_exists and os.path.exists(outpath):
                index += 1
                continue
            # TODO: handle multiple keys (or any keys at all)
            # pass the whole segment object?
            # TODO: pass segment_headers
            # TODO: pass iv
            ts_queue.put((segment_url, outpath, key, iv, segment_headers))
            known_segments.add(segment.uri)
            index += 1

        if m3u8_obj.is_endlist or not segment_url:
            break
        sleep( request_start + sleep_delta / ( 1 if new_segments else 2 ) )
        request_start = datetime.datetime.now()
        m3u8_obj = load_m3u8(m3u8_obj.playlist_url, headers=playlist_headers)
    ts_queue.join()
    return error_queue  #

    # TODO: don't run this from within this function
    # merge_segments(dest)


def merge_segments(dest, sort_key=default_segment_sort_key):
    files = sorted(glob.glob('{}/*.ts'.format(dest)), key=sort_key)
    destfile = '{}.ts'.format(dest)
    bytes_written = 0

    total_size = sum(os.path.getsize(f) for f in files)
    hls_logger.debug('Merging to {}'.format(destfile))
    hls_logger.debug('Total Bytes:  {}'.format(total_size))
    with open(destfile, 'wb') as outfp:
        for i, file in enumerate(files):
            with open(file, 'rb') as infp:
                while True:
                    chunk = infp.read(4096)
                    if not chunk:
                        break
                    bytes_written += outfp.write(chunk)
    hls_logger.debug('Bytes Written: {}'.format(bytes_written))


def _worker(inq, outq, func):
    while True:
        item = inq.get()
        if item is None:
            break
        try:
            func(*item)
        except Exception as e:
            hls_logger.debug('\n'.join((e, item[0])))
            outq.put(item)
        inq.task_done()


class HLSDownloader:
    def __init__(self, dest, playlist):
        self.ts_queue, self.error_queue = None, None
        self.dest = dest
        self.playlist = playlist
        self._running = False

    def start(self):
        # this needs to open it in a separate process or something, because it's going to block
        self._running = True
        self.ts_queue, self.error_queue = download_hls_video(self.playlist, self.dest)

    def wait(self):
        pass

    def stop(self):
        # TODO: actually stop it. A ton of stuff needs to change for that to happen
        self._running = False

    def kill(self):
        self._running = False

    def poll(self):
        return self._running
