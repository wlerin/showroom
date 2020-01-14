# generic methods for downloading video (streaming or otherwise)
import re
import os
import glob
import time
import datetime
from multiprocessing.dummy import Process, Pool, JoinableQueue as Queue
from urllib.error import HTTPError, URLError
import logging

import requests
import m3u8
from m3u8 import _parsed_url

from .constants import TOKYO_TZ

hls_logger = logging.getLogger('showroom.hls')
filename_re = re.compile(r'/([\w=\-_]+\.ts)')
media_sequence_re = re.compile(r'\d+.ts')

# TODO: make these configurable
FILENAME_PADDING = 6
NUM_WORKERS = 20
DEFAULT_CHUNKSIZE = 4096
MAX_ATTEMPTS = 5
MAX_TIME_TRAVEL = 25

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


def save_segment(url, destfile, headers=None, timeout=None, attempts=None):
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

        with open(destfile, 'wb') as outfp:
            try:
                for chunk in r.iter_content(chunk_size=chunksize):
                    if chunk:
                        outfp.write(chunk)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                hls_logger.debug(', '.join(destfile, str(e)))
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
def download_hls_video(
        src, dest, url=None,
        # authentication options
        session=None, headers=None, cookies=None,
        # how to use auth
        # best solution is to use advanced cookies that inform requests which domains to use which on
        auth_mode=0, skip_exists=True, use_original_filenames=True,
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
    :param use_original_filenames: Whether to extract the original filenames or generate them based on sequence
    :return:
    """
    # TODO: decide on dest by #EXT-X-PROGRAM-DATE-TIME instead of when the recording started
    # simplest kludge is to just drop the end of the string

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

    request_start = datetime.datetime.now()
    try:
        m3u8_obj = load_m3u8(src, url, playlist_headers)
    except HTTPError as e:
        hls_logger.debug('HTTPError while first loading playlist: {}'.format(e))
        return
    except URLError as e:
        hls_logger.debug('URLError while first loading playlist: {}'.format(e))
        return

    # is this a variant playlist? (are there nested variant playlists???)
    while m3u8_obj.is_variant:
        # select the best playlist
        # https://tools.ietf.org/html/rfc8216
        # "Every EXT-X-STREAM-INF tag MUST include the BANDWIDTH attribute." -- 4.3.4.2
        best_variant = sorted((item for item in m3u8_obj.playlists), key=lambda x: x.stream_info.bandwidth)[-1]
        request_start = datetime.datetime.now()
        try:
            m3u8_obj = load_m3u8(best_variant.absolute_uri, headers=playlist_headers)
        except HTTPError as e:
            hls_logger.debug('HTTPError while loading variant playlist: {}'.format(e))
            return
        except URLError as e:
            hls_logger.debug('URLError while loading variant playlist: {}'.format(e))
            return

    # if we've reached this point, m3u8_obj is most likely a chunklist
    # check if it has n EXT-X-PROGRAM-DATE-TIME, and if so, modify the dest accordingly
    # It might be better to use the start time that Showroom's API reports, but then that might
    program_date_time = m3u8_obj.program_date_time
    if program_date_time:
        # should already be in Asia/Tokyo but just to make sure
        dt = program_date_time.astimezone(TOKYO_TZ)
        start_time = dt.strftime('%H%M%S')
        dest = ' '.join((dest.rsplit(' ', 1)[0], start_time))
    # this is specifically for Showroom, both HLS and LHLS have a similar segment length for no apparent reason
    # but HLS gives a much-too-high target duration
    # if not m3u8_obj.is_endlist:
    #     # calculate when to fetch the next playlist
    #     total_duration = sum(x.duration for x in m3u8_obj.segments)
    #     target_duration = m3u8_obj.target_duration
    #     sleep_delta = datetime.timedelta(seconds=min(total_duration//3+1, target_duration))
    sleep_delta = datetime.timedelta(seconds=3)

    known_segments = set()
    ts_queue = Queue()

    workers = []
    # TODO: setup workers
    for i in range(NUM_WORKERS):
        p = Process(target=_worker, args=(ts_queue, save_segment))
        p.start()
        workers.append(p)

    os.makedirs(dest, exist_ok=True)

    filename_pattern = 'media_{:0%dd}.ts' % FILENAME_PADDING

    def sleep(sleepdt):
        time.sleep(max((sleepdt - datetime.datetime.now()).total_seconds(), 1))

    errors = 0
    # check if is_endlist, if not keep looping and downloading new segments

    url_ptn = None
    # guess at earlier segments
    first_segment = m3u8_obj.segments[0]
    first_segment_url = first_segment.absolute_uri
    m = filename_re.search(first_segment_url)
    if m:
        filename = m.group(1)
        hls_logger.debug('First segment for {}: {}'.format(dest, filename))
        seq = default_segment_sort_key(filename)
        ptn = media_sequence_re.sub('{}.ts', filename)
        url_ptn = first_segment_url.replace(filename, ptn)
        for n in range(max(1, seq-MAX_TIME_TRAVEL), seq):
            filename = ptn.format(n)
            url = url_ptn.format(n)
            outpath = '{}/{}'.format(dest, filename)
            ts_queue.put((url, outpath, segment_headers))

    segment_url = None
    while True:
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
            ts_queue.put((segment_url, outpath, segment_headers))
            known_segments.add(segment.uri)
            index += 1

        if m3u8_obj.is_endlist or not segment_url:
            break
        sleep(request_start + sleep_delta / (1 if new_segments else 2))
        request_start = datetime.datetime.now()
        try:
            m3u8_obj = load_m3u8(m3u8_obj.playlist_url, headers=playlist_headers)
        except HTTPError as e:
            # TODO: analyse the exception
            hls_logger.debug('HTTPError while loading chunklist: {}'.format(e))
            break
        except URLError as e:
            # TODO: analyse the exception
            hls_logger.debug('URLError while loading chunklist: {}'.format(e))
            break

    # see if any later segments are available before exiting
    # never works
    # if url_ptn:
    #     hls_logger.debug('Last segment: {}'.format(outpath))
    #     for n in range(index, index+5):
    #         filename = ptn.format(n)
    #         url = url_ptn.format(n)
    #         outpath = '{}/{}'.format(dest, filename)
    #         hls_logger.debug('Speculatively downloading {}'.format(outpath))
    #         ts_queue.put((url, outpath, segment_headers))

    # just let the queue end on its own
    # for vods this would be bad but this is live only
    # might still be bad tbh...
    # ts_queue.join()


def merge_segments(dest, sort_key=default_segment_sort_key):
    files = sorted(glob.glob('{}/*.ts'.format(dest)), key=sort_key)

    # assume sort_key returns an integer
    final_media_sequence = sort_key(files[-1])
    expected_media_set = set(range(1, final_media_sequence+1))
    found_media_set = set(sort_key(e) for e in files)
    missing_segments = sorted(expected_media_set - found_media_set)
    if missing_segments:
        hls_logger.warning('Missing segments for {}:\n{}'.format(dest, missing_segments))
        choice = input('Really continue with merge?: ')
        if not choice[0].lower() == 'y':
            hls_logger.info('Aborting merge.')
            return
    else:
        hls_logger.info('All expected segments found, beginning merge.')

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


def _worker(inq, func):
    while True:
        item = inq.get()
        if item is None:
            break
        try:
            func(*item)
        except Exception as e:
            hls_logger.debug(', '.join((str(e), item[0])))
        inq.task_done()


class HLSDownloader:
    def __init__(self, dest, playlist):
        self.dest = dest
        self.playlist = playlist
        self._running = False

    def start(self):
        # this needs to open it in a separate process or something, because it's going to block
        self._running = True
        download_hls_video(self.playlist, self.dest)

    def wait(self):
        pass

    def stop(self):
        # TODO: actually stop it. A ton of stuff needs to change for that to happen
        self._running = False

    def kill(self):
        self._running = False

    def poll(self):
        return self._running
