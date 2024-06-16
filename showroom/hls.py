import re
import os
import glob
import time
import datetime
from multiprocessing.dummy import Process, JoinableQueue as Queue
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from http.client import RemoteDisconnected
import logging
from itertools import zip_longest
import shutil

import requests
try:
    import m3u8
except ImportError:
    print('Unable to import M3U8 library. Some functionality will be unavailable.')

from showroom.utils.media import md5sum
from showroom.archive.probe import probe_video2
from .constants import TOKYO_TZ


hls_logger = logging.getLogger('showroom.hls')
_filename_re = re.compile(r'([\w=\-]+?)(\d+).ts')

# TODO: make these configurable
FILENAME_PADDING = 6
NUM_WORKERS = 20
DEFAULT_CHUNKSIZE = 4096
MAX_ATTEMPTS = 5
MAX_TIME_TRAVEL = 25
# there seems to be a flaw in the recording script that results in streams being mixed together in one folder
# need more than just 5 matches to confirm it's the same stream
MAX_CHECKSUM_MATCHES = 15
# TODO: inherit headers from config
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
}
TIMEOUT = 3
# under ordinary circumstances, use 5*60
MAX_START_TIME_DIFFERENCE=2*60*60


def _segment_sort_key(file):
    """
    Returns a tuple containing the filename split into a string and an integer

    e.g. given the filename "bob-and-cut=3516.ts"
    will return ('bob-and-cut=', 3516)
    """
    # return int(re.findall(r'(\d+)', file)[-1])
    m = _filename_re.search(file)
    if m:
        return m.group(1), int(m.group(2))
    raise ValueError('Unable to determine sort sequence: {}'.format(file))


def _parsed_url(url):
    return urljoin(url, '.')


def load_m3u8(src, url=None, headers=None):
    # is playlist raw text, a path to a file, or a url?
    if src.upper().startswith('#EXTM3U'):
        m3u8_obj = m3u8.loads(src)
        m3u8_obj.playlist_url = url
        m3u8_obj.base_uri = _parsed_url(url)
    elif os.path.exists(src):
        m3u8_obj = m3u8.load(src)
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

    not_found_count = 0
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
                not_found_count += 1
                # allow three 404 errors
                if not_found_count > 3:
                    raise
            continue

        with open(destfile, 'wb') as outfp:
            try:
                for chunk in r.iter_content(chunk_size=chunksize):
                    if chunk:
                        outfp.write(chunk)
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                hls_logger.debug(', '.join((destfile, str(e))))
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
# TODO: wrap this up in a class
# TODO: once in a class, keep track of files, discontinuities, etc. and allow merging
# or printing such information to disk, for later merging
# TODO: automatically save segments to a tar file when complete
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

    :param src: playlist source
    :param dest: destination, will be created as a folder containing individual segments,
        and then a merged ts file with the same name
    :param url: url of the playlist, used to set the base_uri if a file or raw text is given as playlist
    :param session:
    :param headers:
    :param cookies:
    :param auth_mode: 0/None - keys only, 1 - keys and playlists, 2 - keys, playlists, segments
    :param skip_exists: Whether to skip existing files (for VOD)
    :param use_original_filenames: Whether to extract the original filenames or generate them based on sequence
    :return:run, PIPE,
    """
    if not session:
        # TODO: use a CookieJar that accepts more advanced cookie formats
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
    except (HTTPError, URLError, ConnectionResetError) as e:
        hls_logger.debug('Error while loading first playlist: {}'.format(e))
        return
    except RemoteDisconnected as e:
        hls_logger.debug('Remote disconnected while loading first playlist: {}'.format(e))
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
        except (HTTPError, URLError, ConnectionResetError) as e:
            hls_logger.debug('HTTPError while loading variant playlist: {}'.format(e))
            return
        except RemoteDisconnected as e:
            hls_logger.debug('Remote disconnected while loading variant playlist: {}'.format(e))
            return

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

    # guess at earlier segments
    first_segment = m3u8_obj.segments[0]
    first_segment_url = first_segment.absolute_uri
    m = _filename_re.search(first_segment_url)
    if m:
        filename, media_group, media_sequence = m.group(0), m.group(1), int(m.group(2))
        hls_logger.debug('First segment for {}: {}'.format(dest, filename))
        ptn = _filename_re.sub(r'\1{}.ts', filename)
        url_ptn = first_segment_url.replace(filename, ptn)
        for n in range(max(1, media_sequence-MAX_TIME_TRAVEL), media_sequence):
            filename = ptn.format(n)
            url = url_ptn.format(n)
            outpath = '{}/{}'.format(dest, filename)
            if skip_exists and os.path.exists(outpath):
                continue
            ts_queue.put((url, outpath, segment_headers))

    segment_url = None

    errors = 0
    url_ptn = None
    discontinuity_detected = False
    # sometimes the playlist just stays open, long after the stream is finished
    # number of loops without new segments
    no_new_segments_count = 0
    old_starting_index = None
    while True:
        index = m3u8_obj.media_sequence or 1

        # Check if the numbering has restarted
        if old_starting_index and old_starting_index > index:
            hls_logger.warning('Media sequence restarted, restarting download')
            break
        old_starting_index = index
        new_segments = 0

        # TODO: log if index doesn't match sequence extracted from segment url
        # TODO: detect if skipped segments or missing files on disk and add them to queue
        # before they are gone forever
        if index < 0:
            pass

        for segment in m3u8_obj.segments:
            if segment.uri in known_segments:
                index += 1
                continue
            segment_index = _segment_sort_key(segment.uri)[1]

            if segment.discontinuity:
                discontinuity_detected = True
                discontinuity_file = '{}/discontinuity_{}.m3u8'.format(
                    dest,
                    datetime.datetime.now(tz=TOKYO_TZ).strftime('%Y%m%d_%H%M%S')
                )
                m3u8_obj.dump(discontinuity_file)
            # alternative discontinuity detection
            if segment_index != index:
                hls_logger.warning(f'Segments out of order: expected {index}, got {segment_index}')
                for _ in range(NUM_WORKERS):
                    ts_queue.put(None)
                return

            new_segments += 1
            no_new_segments_count = 0
            segment_url = segment.absolute_uri

            if use_original_filenames or discontinuity_detected:
                m = _filename_re.search(segment_url)
                if not m:
                    filename = filename_pattern.format(index)
                else:
                    filename = m.group(0)
            else:
                filename = filename_pattern.format(index)
            outpath = '{}/{}'.format(dest, filename)
            if skip_exists and os.path.exists(outpath):
                index += 1
                continue
            ts_queue.put((segment_url, outpath, segment_headers))
            known_segments.add(segment.uri)
            index += 1

        # close if no new segments after several requests
        if not new_segments:
            no_new_segments_count += 1
        if no_new_segments_count > 3:
            break

        if m3u8_obj.is_endlist or not segment_url:
            break
        sleep(request_start + sleep_delta / (1 if new_segments else 2))
        request_start = datetime.datetime.now()
        try:
            m3u8_obj = load_m3u8(m3u8_obj.playlist_url, headers=playlist_headers)
        except (HTTPError, URLError, ConnectionResetError) as e:
            # TODO: analyse the exception
            hls_logger.debug('Error while loading chunklist: {}'.format(e))
            break
        except RemoteDisconnected as e:
            hls_logger.debug('Remote disconnected while loading chunklist: {}'.format(e))
            break
        # this is going to catch way too many other errors
        except ValueError as e:
            # thrown by M3U8 library
            # this shouldn't get thrown anymore, but might as well still catch it
            r = requests.get(m3u8_obj.playlist_url, headers=playlist_headers)
            hls_logger.debug('Failed to load M3U8: {}\n{}'.format(e, r.text))
            # probably recoverable, but we're going to rewind the stream anyway
            break

    for _ in range(NUM_WORKERS):
        ts_queue.put(None)

    # just let the queue end on its own
    # for vods this would be bad but this is live only
    # might still be bad tbh...
    # ts_queue.join()


# TODO: detect and handle segments in the same stream with differing filename patterns
# TODO: detect and utilise discontinuity m3u8 files
def merge_segments(dest, sort_key=_segment_sort_key, force_yes=False):
    files = sorted(glob.glob('{}/*.ts'.format(glob.escape(dest))), key=sort_key)

    # assume sort_key returns an tuple of a str and an integer
    first_key = sort_key(files[0])
    final_key = sort_key(files[-1])
    try:
        first_media_sequence = first_key[1]
        final_media_sequence = final_key[1]
        expected_media_set = set(range(first_media_sequence, final_media_sequence + 1))
        found_media_set = set(sort_key(e)[1] for e in files)
    except TypeError as e:
        hls_logger.warning('Unable to read media-sequence using provided sort_key, will not check for missing files')
    else:
        missing_segments = sorted(expected_media_set - found_media_set)
        if missing_segments:
            hls_logger.warning('Missing segments for {}:\n{}'.format(dest, missing_segments))
            if not force_yes:
                choice = input('Really continue with merge?: ')
                if not choice[0].lower() == 'y':
                    hls_logger.info('Aborting merge.')
                    return
            else:
                hls_logger.info('Continuing with merge anyway.')
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
        self._running = True
        download_hls_video(self.playlist, self.dest)

    def wait(self):
        pass

    def stop(self):
        self._running = False

    def kill(self):
        self._running = False

    def poll(self):
        return self._running


# various additional utilities for processing hls recordings saved by this module
def check_missing(files):
    # checks a list of files to see if any are missing, based on an int value returned by key
    # files.sort(key=key)
    key = _segment_sort_key
    files.sort(key=key)

    patterns = _identify_patterns(files)

    result = {}
    for pattern in patterns:
        # TODO: handle missing before the start segment differently.
        pattern_files = [e for e in files if pattern in e]
        start_index = key(pattern_files[0])[1]
        final_index = key(pattern_files[-1])[1]
        expected_segments = range(start_index, final_index+1)
        found_segments = set(key(file)[1] for file in pattern_files)
        missing = []
        new_run = []
        for segment in expected_segments:
            if segment in found_segments:
                if new_run:
                    if len(new_run) > 2:
                        missing.extend((new_run[0], f'..({len(new_run)-2})..', new_run[-1]))
                    else:
                        missing.extend(new_run)
                    new_run = []
            else:
                new_run.append(segment)
        # new_run should always be empty at the end of iteration, right?

        # missing = sorted(expected_segments - found_segments)
        # # TODO: fix this for the new patterns
        # if 'media' in pattern and start_index != 1:
        #     missing.insert(0, 0)
        #     missing.insert(1, start_index-1)
        result[pattern] = missing

    return result


def _identify_patterns(files):
    return sorted(set(_segment_sort_key(e)[0] for e in files))


# Segment folder consolidation
def simplify(path, ignore_checksums=False):
    """
    Tries to simplify an archive as much as possible prior to either tar + upload or comparison and merge
    """
    # TODO: handle filename pattern weirdness, e.g. pattern x -> pattern y -> pattern x
    # (probably the start_time is wrong for pattern_y)
    oldcwd = os.getcwd()
    os.chdir(path)

    streams = sorted(glob.glob('*Showroom*'))
    rooms = {}
    for stream in streams:
        if not os.path.isdir(stream):
            continue
        date, handle = stream.split(' Showroom - ')
        handle, time = stream.rsplit(' ', 1)
        if handle not in rooms:
            rooms[handle] = []
        rooms[handle].append((date, time, stream))

    def move_discontinuity_files(src, dest):
        chunklists = glob.glob('{}/*.m3u8'.format(src))
        if chunklists:
            move_files(chunklists, dest, no_probe=True)

    for handle, streams in rooms.items():
        start_date, start_time, first_stream = streams[0]
        # don't spam about singleton streams
        # hls_logger.debug('Beginning analysis of {}...'.format(first_stream))
        # check for a discontinuity m3u8
        # once i reach those i want to revise this script
        # begin with 2020-01-18
        # if glob.glob('{}/discontinuity*.m3u8'.format(first_stream)):
        #     print(first_stream, 'has a discontinuity.m3u8')
        # TODO: sort by date not filename
        base_files = sorted(glob.glob('{}/*.ts'.format(first_stream)), key=_segment_sort_key)
        if not base_files:
            hls_logger.debug('{} contains no segments, attempting to remove'.format(first_stream))
            try:
                os.rmdir(first_stream)
            except OSError as e:
                print(e)
            continue

        # abort_on_checksum_fail = False
        filename_patterns = _identify_patterns(base_files)
        # if 'media_' in filename_patterns or 'media_v2_' in filename_patterns:
        #     # media_XXX.ts or media_v2_XXX.ts
        #     # really we should be checking for checksum matches not aborting on fails
        #     # because this fails when a segment had a botched download
        #     hls_logger.warning('Detected old-style filename pattern in {}'.format(handle))
        #     abort_on_checksum_fail = True
        if len(filename_patterns) > 1:
            # TODO: handle this situation
            hls_logger.warning('Multiple filename patterns detected in first stream: {}\n{}'.format(
                first_stream, filename_patterns
            ))
            continue
        # TODO: identify the oldest pattern either by file date (preserved through tar) or a discontinuity m3u8
        # except actually we'd want to be using the latest pattern here (and split it from the earlier one(s))
        base_pattern = filename_patterns[0]
        # step 1: check for multiple filename patterns
        for stream in streams[1:]:
            new_date, new_time, new_stream = stream
            hls_logger.debug('Beginning analysis of {}...'.format(new_stream))

            new_files = sorted(glob.glob('{}/*.ts'.format(new_stream)), key=_segment_sort_key)
            
            hls_logger.debug('{} files found'.format(len(new_files)))
            if not new_files:
                move_discontinuity_files(new_stream, first_stream)
                try:
                    os.rmdir(new_stream)
                except OSError:
                    hls_logger.info('{} is not empty'.format(new_stream))
                continue
            new_patterns = _identify_patterns(new_files)
            hls_logger.debug('{} patterns found'.format(len(new_patterns)))
            # is this irrelevant now?
            if len(new_patterns) > 1:
                if 'media_v2_' in new_patterns:
                    hls_logger.error('No idea how to handle mixed v2 and v3 stream: {}\nAborting...'.format(
                        new_stream
                    ))
                    return
                if len(new_patterns) == 2 and base_pattern in new_patterns:
                    # move files that do match the old pattern
                    move_files((file for file in new_files if base_pattern in file), first_stream, ignore_checksums)
                    new_patterns.remove(base_pattern)  # will hit the if clause below and then continue
                    # don't know how to handle more than two patterns in a single folder
                    # at least not without a discontinuity.m3u8, and those don't exist yet in the archives i'm testing
                else:
                    # can this be dealt with without raising an error?
                    # in theory, even three patterns could be handled if we looked at streams before and after this one
                    hls_logger.warning('Too many filename patterns detected in stream: {}\n{}'.format(
                        new_stream, new_patterns
                    ))
                    continue

            new_pattern = new_patterns[0]
            if base_pattern != new_pattern:
                hls_logger.debug('Pattern mismatch: {} {}'.format(base_pattern, new_pattern))
                # Only one pattern in the new file, so this means we're in an actual new stream, not a discontinuity
                # future "streams" will be merged into this one instead
                start_date, start_time, first_stream = new_date, new_time, new_stream
                base_pattern = new_pattern
                continue

            # consolidate a lot of the checking into one function
            if not _stream_identity_check(first_stream, new_stream):
                start_date, start_time, first_stream = new_date, new_time, new_stream
                base_pattern = new_pattern
                continue

            num_moved = move_files(new_files, first_stream, ignore_checksums)
            hls_logger.debug('Moved {} files to {}'.format(num_moved, first_stream))
            # TODO: verify that the move works correctly, then delete the "new" stream
            base_files = sorted(glob.glob('{}/*.ts'.format(first_stream)), key=_segment_sort_key)
            if not set(e.split('/')[-1] for e in new_files) - set(e.split('/')[-1] for e in base_files):
                # rm_files = glob.glob('{}/*.ts'.format(new_stream))
                # for file in rm_files:
                #     os.remove(file)
                move_discontinuity_files(new_stream, first_stream)
                try:
                    os.rmdir(new_stream)
                except OSError:
                    hls_logger.info('{} is not empty'.format(new_stream))

    os.chdir(oldcwd)


def _stream_identity_check(stream1, stream2):
    """
    Test whether two folders represent parts of the same stream

    :param stream1: first stream folder to test against. this should be the earlier recording
    :param stream2: second stream folder to test. this should be the later recording
    :return: True if folders appear to be from the same stream, False otherwise
    """
    # check mod times
    # find overlap between the folders
    # if there is overlap:
    # check that it makes sense
    # i.e. it should look like <early files in stream1> <overlap in both> <later files in stream2>
    # if mod times are checked, can switch stream1 and stream2 if necessary. but i should control that
    # in the caller
    # if the overlap makes sense, test some checksums. if enough match (between 1 and 5, depending on how big the
    # overlap is, i.e. like, min(overlap, 1), given there is overlap)
    # if there's enough matching checksums (even just one should be enough tbh), return True
    # if there is no overlap, check that:
    # the earlier stream has lower sequence than the later stream
    # the gap is not too large
    # if both are true, return True
    # otherwise, return False
    files1 = sorted(glob.glob('{}/*.ts'.format(stream1)), key=_segment_sort_key)
    files2 = sorted(glob.glob('{}/*.ts'.format(stream2)), key=_segment_sort_key)
    if not files1 or not files2:
        # i dunno what this means
        hls_logger.warning('Empty folder')
        return True
    if _segment_sort_key(files1[0])[0] != _segment_sort_key(files2[0])[0]:
        hls_logger.warning('Different filename patterns')
        return False
    # build tables of file : (sequence, st_mtime)
    data1 = {}
    data2 = {}
    for files, data in ((files1, data1), (files2, data2)):
        earliest_modtime = None
        latest_modtime = None
        lastseq = 0
        for i, file in enumerate(files):
            _, seq = _segment_sort_key(file)
            modtime = os.stat(file).st_mtime
            data[file] = (seq, modtime)
            if i == 0:
                earliest_modtime = latest_modtime = modtime
                lastseq = seq
            else:
                # if seq - lastseq > 30:
                #     # see how often this triggers
                #     hls_logger.debug(
                #         'More than a minute gap between segments: {}s {}'.format((seq - lastseq)*2, file))
                #     # too often
                lastseq = seq
                if modtime < earliest_modtime:
                    gap = earliest_modtime - modtime
                    if gap > 120:
                        raise ValueError(
                            'Too large a gap between segment and start of the stream: {} {}'.format(gap, file))
                    earliest_modtime = modtime
                else:
                    gap = modtime - latest_modtime
                    # if abs(gap) > 60:
                    #     hls_logger.debug(
                    #         'Large modtime gap between segment and previous latest segment: {}s {}'.format(gap, file))
                    if gap > 0:
                        latest_modtime = modtime
                    # elif gap < -120:
                    #     hls_logger.warning('Large modtime decrease: {}s {}'.format(gap, file))

        data.update(dict(start_time=earliest_modtime, end_time=latest_modtime))

    # make sure the streams are in the correct order
    if data2['start_time'] < data1['start_time']:
        # flip them
        hls_logger.warning('Streams provided in wrong order, switching')
        stream1, stream2 = stream2, stream1
        files1, files2 = files2, files1
        data1, data2 = data2, data1

    # modtime check
    if data1['end_time'] > data2['start_time']:
        # this actually shouldn't happen at all during normal simplification, only possibly after comparison
        # and only then because i screwed something up there and having figured out what yet
        modtime_overlap = data1['end_time'] - data2['start_time']
        if modtime_overlap < 30:
            hls_logger.debug('Modtime overlap detected: {}'.format(modtime_overlap))
        else:
            hls_logger.warning('Large modtime overlap detected: {}s {} {}'.format(modtime_overlap, stream1, stream2))
    else:
        if data2['start_time'] - data1['end_time'] > 1800:
            # 2 minute gap == assume new stream
            # maybe increase the gap a bit though
            return False

    # TODO: test sequence check logic
    # startseq1 = data1[files2[0]][0]
    startseq2 = data2[files2[0]][0]
    endseq1 = data1[files1[-1]][0]
    # endseq2 = data2[files1[-1]][0]

    # case 1: media_792.ts vs media_777.ts (or so)
    # normal sequence progression with overlap from speculative rewind
    # but how big an overlap can it be? surely more than just MAX_TIME_TRAVEL
    # case 2: media_55.ts vs media_1.ts
    # short first stream, if it's short enough it could potentially still pass the sequence check
    # case 3: media_792.ts vs media_1.ts
    # long first stream, obviously not the same thing
    # case 4: media_55.ts vs. media_777.ts
    # no overlap, assume new stream unless the gap is very small
    if endseq1 >= startseq2:
        # excludes case 3 and some instances of case 2
        # Hasegawa Rena's 200320 stream had up to 32 files worth of overlap
        # in theory the max should be MAX_TIME_TRAVEL + chunklist length
        # but there's no way to know the latter here
        if endseq1 - startseq2 > MAX_TIME_TRAVEL+10:
            hls_logger.info('Sequence overlap between streams is very large')
            # hls_logger.info('Sequence overlap between streams is very large, assuming new stream')
            # return False
    else:
        # case 4, nothing to check so assume same stream, if it passed the modtime check earlier assume the same stream
        hls_logger.warning(
            'No sequence overlap, but segments are in the correct order: {} -> {}'.format(endseq1, startseq2))
        return True

    # TODO: checksum test
    file_overlap = sorted(
        (set(os.path.basename(file) for file in files1) & set(os.path.basename(file) for file in files2)),
        key=_segment_sort_key
    )
    if not file_overlap:
        hls_logger.warning('Passed all other tests, but no matching files to test checksums against')
        if startseq2 > 100:
            return True
        else:
            return False
    # half the matching segments, or 5 if more than 10 matches, or 1 if just one match
    matches_required = min(len(file_overlap) // 2, MAX_CHECKSUM_MATCHES) or 1
    matches = 0
    for i, file in enumerate(file_overlap):
        file1 = os.path.join(stream1, file)
        file2 = os.path.join(stream2, file)
        if md5sum(file1) == md5sum(file2):
            matches += 1
            if matches >= matches_required:
                return True
        elif i > MAX_CHECKSUM_MATCHES*2:
            hls_logger.info('no checksum matches in first {} overlapping files, assuming new stream'.format(
                MAX_CHECKSUM_MATCHES*2
            ))
            break
        # else:
        #     hls_logger.debug('checksum failed: {}'.format(file))
    if matches > 0:
        # this needs to be personally investigated
        hls_logger.error('Some checksums match but not as many as desired: {}/{} {} {}'.format(
            matches, len(file_overlap), stream1, stream2))
        raise ValueError('Not enough matches')


# def is_checksum_match(files, dest):
#     # rather than aborting on checksum fail, check that overlapping segments match checksums before transferring missing
#     # it's fine if a few don't match
#     # if
#     destfiles = [f.split('/')[-1] for f in glob.glob('{}/*.ts'.format(dest))]
#     matches = 0
#     overlap = 0
#
#     for file in files:
#         filename = file.split('/')[-1]
#         if filename not in destfiles:
#             continue
#         overlap += 1
#         destfile = '{}/{}'.format(dest, filename)
#         if md5sum(file) == md5sum(destfile):
#             matches += 1
#     if matches:
#         return True
#     elif overlap:
#         return False
#     else:
#         # no overlapping files, inconclusive
#         try:
#             source, filename = files[0].split('/')[-2:]
#         except IndexError:
#             print(files)
#             raise
#         hls_logger.error('No file overlap between: {} and {}'.format(source, dest))
#         return False


def move_files(files, dest, ignore_checksums=False, no_probe=False):
    num_moved = 0

    # if abort_on_checksum_fail:
    #     for file in files:
    #         source, filename = file.split('/')[-2:]
    #         destfile = '{}/{}'.format(dest, filename)
    #         if os.path.exists(destfile):
    #             if not md5sum(file) == md5sum(destfile):
    #                 hls_logger.error('Checksum mismatch in version 2 stream: {}'.format(source))
    #                 return num_moved
    for file in files:
        filename = file.split('/')[-1]
        destfile = '{}/{}'.format(dest, filename)
        if not os.path.exists(destfile):
            num_moved += 1
            os.replace(file, destfile)
        # bad destination file
        elif not no_probe and probe_video2(destfile) is None:
            if probe_video2(file):
                num_moved += 1
                os.replace(file, destfile)
            else:
                hls_logger.warning('{} exists in destination, but both versions failed probe.'.format(file))
                # os.remove(file)
                # os.remove(destfile)
        # bad source file
        elif not no_probe and probe_video2(file) is None:
            hls_logger.warning('{} failed probe'.format(file))
        # both videos were successfully probed
        elif not ignore_checksums and md5sum(file) == md5sum(destfile):
            # print('{} exists in destination, removing duplicate'.format(file))
            os.remove(file)
        elif os.path.getsize(file) > os.path.getsize(destfile):
            os.replace(file, destfile)
        else:
            os.remove(file)

    return num_moved


def group_by_archive(streams):
    data = {}
    for stream in streams:
        *_, archive, date, file = stream.split('/')
        if archive not in data:
            data[archive] = []
        data[archive].append(stream)
    return data
    

def compare_archives(archive_paths, final_root, simplify_first=False, check_only=False):
    """
    Compare archive folders containing separate recordings of the same streams

    Create a consolidated version of those recordings at final_root

    WARNING: run hls.simplify (or archiver.py hls simplify) on each archive beforehand
    If there are errors during simplify (e.g. two patterns in a folder)
    these must be resolved before running this function

    :param archive_paths:
    :param final_root:
    :param simplify_first: Runs hls.simplify on each archive path before doing anything else
    :param check_only: Exit after checking for symmetry
    :return:
    """
    # get a list of all streams in each archive
    # "zip" up the streams
    # run compare_streams on each set of zipped streams
    # may be necessary to scan files in each stream to determine which one goes with which
    if simplify_first:
        for archive_path in archive_paths:
            simplify(archive_path)

    archive_data = {
        # streams will be stored as full paths
        path: sorted(e for e in glob.glob('{}/*'.format(path)) if os.path.isdir(e))
        for path in archive_paths
    }

    # this is provided for documentation rather than performance
    def split_stream_name(file):
        stream_name = os.path.basename(file)
        room_name, start_time = stream_name.rsplit(' ', 1)
        return room_name, start_time

    def compare_start_times(start_times):
        def convert_time_str(time_str):
            hours, minutes, seconds = map(int, (time_str[:2], time_str[2:4], time_str[4:]))
            # i don't think the date will matter at all, but is this a correct assumption?
            return datetime.datetime(2020, 1, 1, hour=hours, minute=minutes, second=seconds)

        times = sorted(convert_time_str(e) for e in start_times)
        # all streams are within 5 minutes of the earliest stream
        return all((x - times[0]).total_seconds() < MAX_START_TIME_DIFFERENCE for x in times[1:])

    def get_filename_patterns(path):
        return _identify_patterns(glob.glob('{}/*.ts'.format(path)))

    rooms = {}
    for path, streams in archive_data.items():
        for stream in streams:
            # assumption: stream names are still those used by showroom.py
            room_name = split_stream_name(stream)[0]
            if room_name not in rooms:
                rooms[room_name] = {}
            if path not in rooms[room_name]:
                rooms[room_name][path] = []
            rooms[room_name][path].append(stream)

    b_too_many_streams = False
    all_streams = []
    for room_name, room_data in rooms.items():
        # how many streams are there for this room in each archive?
        lengths = set(map(len, room_data.values()))
        if len(lengths) == 1:
            # will run the same timestamp checks whether there are 1 or 20 streams for a given room
            new_streams = []
            check_passed = True
            for streams in zip(*room_data.values()):
                if compare_start_times(map(lambda x: split_stream_name(x)[-1], streams)):
                    new_streams.append(list(streams))
                else:
                    hls_logger.debug('Start times do not match: {}'.format(
                        room_name))

                    for archive, archive_streams in room_data.items():
                        print(archive, len(archive_streams))
                        print(*archive_streams, sep='\n')
                    check_passed = False
                    break
            if check_passed:
                all_streams.extend(new_streams)
                continue
            # if timestamp checks failed
            # assuming that simplify was run first
            # that means there will only be one folder per filename pattern
            # i *think* that the logic from this point on doesn't require equal length stream lists...
            # 2020-03-21: except now the filename pattern isn't unique, they're just all media_ or media_v2_
        # (room_name, filename_pattern): [streams]
        new_streams = {}
        for stream_list in room_data.values():
            for stream in stream_list:
                patterns = get_filename_patterns(stream)
                if not patterns:
                    # empty folder?
                    hls_logger.warning('Empty folder: {}'.format(stream))
                    continue
                if len(patterns) > 1:
                    # this should have been solved via simplify or by hand!!!
                    raise ValueError('Too many filename patterns in folder: {}'.format(stream))
                stream_key = (room_name, patterns[0])
                if stream_key not in new_streams:
                    new_streams[stream_key] = []
                new_streams[stream_key].append(stream)

        for stream_key, stream_list in new_streams.items():
            archives_grouped = group_by_archive(stream_list)
            if len(set(map(len, archives_grouped.values()))) != 1:
                # more streams in than there should be, run simplify!!!!
                # raise ValueError('Too many streams with the same key, run simplify first! '
                #                  'Problem room: {}'.format(room_name))
                error_lines = ['Too many streams with the same stream_key: {} {}'.format(stream_key, room_name),]
                for archive, archive_streams in archives_grouped.items():
                    error_lines.append(f'{archive} {len(archive_streams)}')
                    error_lines.extend(archive_streams)
                error_lines.append('\n')
                error_msg = '\n'.join(error_lines)
                hls_logger.debug(error_msg)
                b_too_many_streams = True
            all_streams.append(stream_list)

    # only check if ready for merging into final destination, don't actually merge
    if check_only:
        return
    elif b_too_many_streams:
        raise TooManyStreamsWithSameKeyError('Too many streams with the same key, run simplify first!', ())
    
    # that should be all of them?
    for stream_list in all_streams:
        try:
            stream_list.sort(key=split_stream_name)
        except AttributeError:
            print(stream_list)
            raise
        dest = '{}/{}'.format(final_root, os.path.basename(stream_list[0]))
        final_files = compare_streams(stream_list, final_root)
        prefix, missing = list(check_missing(final_files).items())
        if missing:
            hls_logger.info('Missing segments for {}: {} - {}'.format(dest, prefix.strip('-_'), missing))


class TooManyStreamsWithSameKeyError(ValueError):
    """
    Super common error during comparisons, this will allow for easier troubleshooting
    """
    def __init__(self, message, streams):
        super().__init__(message)
        self.message = message
        self.streams = streams

    def __repr__(self):
        return '{}\n{}'.format(self.message, '\n'.join(self.streams))


def compare_streams(scan_paths, final_root):
    """
    Compare several different recordings of the same stream, save a single (more) complete copy under final_root

    :param scan_paths: list of 2 or more paths to recordings of the same stream to compare
    :param final_root: root path for final, merged recording (still as segments)
        - folder name will be the earliest of the paths, placed under final_root
    :return: number of segments in the final stream, and the indexes of any still-missing segments
    :raises: ValueError if multiple segment filename prefixes found
        or two segments at the same index have different filenames (should be impossible?)
        hls.simplify should fix the first case most of the time, but it may occasionally require manual intervention
    """
    scan_paths = sorted(scan_paths, key=lambda x: os.path.basename(x))  # make the earliest folder the first one
    dest = '{}/{}'.format(final_root, os.path.basename(scan_paths[0]))
    os.makedirs(dest, exist_ok=True)
    hls_logger.info('Comparing sources for {}'.format(os.path.basename(dest)))
    data = {}

    # for path in scan_paths:
    #     data[path] = scan_stream(path)
    data = scan_streams(scan_paths)

    def compare_segments(segments):
        """
        Compare different versions of the same segment, return the best one

        :param segments: list of file data objects like that returned by scan_stream
        :return: returns best version of the segment in question
        :raises: ValueError if the segment names do not match
        """
        choice = None
        # TODO: how to compare ffprobe data?
        for segment in segments:
            # skip non-existent segments and segments that failed probe (i.e. no readable content)
            if not segment:  # or not segment['probe_result']:
                continue

            if choice is None:  # set initial data
                choice = segment
                continue

            if choice['name'] != segment['name']:
                raise ValueError('Segment names do not match: {}\n{}'.format(
                    choice['path'], segment['path']
                ))

            # if choice['checksum'] != segment['checksum']:
            #     hls_logger.warning('Checksums do not match: {}\n{}'.format(choice['path'], segment['path']))
            # TODO: how can i skip this if the destination path already exists?
            if os.path.getsize(choice['path']) < os.path.getsize(segment['path']):
                choice = segment

        return choice

    # compare each index starting from 1 until the longest path
    # identify the best at each index, append that to chosen
    # None = no segment for that index
    # UPDATE: this no longer "starts from 1"
    final_segments = (compare_segments(item) for item in zip_longest(*data.values()))
    final_files = []
    for i, item in enumerate(final_segments, 1):
        if item:
            destfile = '{}/{}'.format(dest, item['name'])
            if os.path.exists(destfile):
                continue  # avoid writing over a previous run of this function
            # TODO: use shutil.copy2 instead
            # Using os.replace for now because it's faster and i've already made copies
            shutil.copy2(item['path'], destfile)
            final_files.append(os.path.basename(destfile))
    return final_files


# def scan_stream(path):
#     """
#     Scan a stream and return info about each saved segment
#     :param path: Path to stream
#     :return: a list of dicts containing info about each segment:
#         - index
#         - name
#         - path
#         # - checksum
#         # - ffprobe results (to be further parsed)
#         # - size
#     """
#     files = sorted(glob.glob('{}/*.ts'.format(path)), key=_segment_sort_key)
#     if not files:
#         return
#     # check that there is only one prefix
#     prefixes = set(_segment_sort_key(e)[0] for e in files)
#     if len(prefixes) > 1:
#         raise ValueError('Too many segment filename prefixes in {}'.format(path))
#     final_index = _segment_sort_key(files[-1])[1]
#     segments = {}
#     for file in files:
#         item = {}
#         prefix, index = _segment_sort_key(file)
#         item['index'] = index
#         item['name'] = os.path.basename(file)
#         item['path'] = file
#         segments[index] = item
#
#     # delay time consuming tasks until the file is asked for
#     # TODO: avoid doing any of this unless needed
#     for i in range(1, final_index+1):
#         item = segments.get(i)
#         # if item:
#         #     item['probe_result'] = probe_video2(item['path'])
#         #     # item['checksum'] = md5sum(item['path'])
#         #     item['size'] = os.path.getsize(item['path'])
#         yield item
#     # return [segments.get(i) for i in range(1, final_index+1)]


def scan_streams(paths):
    data = dict()
    segments = dict()
    low_index = None
    high_index = 0

    def yield_by_index(segments, start, end):
        for i in range(start, end+1):
            yield segments.get(i)

    for path in paths:
        segments[path] = dict()
        files = sorted(sorted(glob.glob('{}/*.ts'.format(path)), key=_segment_sort_key))
        if not files:
            continue
        prefixes = set(_segment_sort_key(e)[0] for e in files)
        if len(prefixes) > 1:
            raise ValueError('Too many segment filename prefixes in {}'.format(path))

        li = _segment_sort_key(files[0])[-1]
        hi = _segment_sort_key(files[-1])[-1]
        if low_index is None or low_index > li:
            low_index = li
        if high_index < hi:
            high_index = hi

        for file in files:
            item = dict()
            prefix, index = _segment_sort_key(file)
            item['index'] = index
            item['name'] = os.path.basename(file)
            item['path'] = file
            segments[path][index] = item

    if low_index is None:
        low_index = 1

    for path in paths:
        data[path] = yield_by_index(segments[path], low_index, high_index)

    return data
