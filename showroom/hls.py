import re
import os
import glob
import time
import datetime
from multiprocessing.dummy import Process, JoinableQueue as Queue
from urllib.error import HTTPError, URLError
import logging
from itertools import zip_longest
import shutil

import requests
import m3u8
from m3u8 import _parsed_url

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

# TODO: inherit headers from config
DEFAULT_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
}
TIMEOUT = 3


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
    while True:
        index = m3u8_obj.media_sequence or 1
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

            if segment.discontinuity:
                discontinuity_detected = True
                discontinuity_file = '{}/discontinuity_{}.m3u8'.format(
                    dest,
                    datetime.datetime.now(tz=TOKYO_TZ).strftime('%Y%m%d_%H%M%S')
                )
                m3u8_obj.dump(discontinuity_file)

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
        except HTTPError as e:
            # TODO: analyse the exception
            hls_logger.debug('HTTPError while loading chunklist: {}'.format(e))
            break
        except URLError as e:
            # TODO: analyse the exception
            hls_logger.debug('URLError while loading chunklist: {}'.format(e))
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
    files = sorted(glob.glob('{}/*.ts'.format(dest)), key=sort_key)

    # assume sort_key returns an tuple of a str and an integer
    final_key = sort_key(files[-1])
    try:
        final_media_sequence = final_key[1]
        expected_media_set = set(range(1, final_media_sequence + 1))
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
        pattern_files = [e for e in files if pattern in e]
        final_index = key(pattern_files[-1])[1]
        expected_segments = set(range(1, final_index+1))
        found_segments = set(key(file)[1] for file in pattern_files)
        missing = expected_segments - found_segments
        result[pattern] = sorted(missing)

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
        move_files(chunklists, dest, no_probe=True)

    for handle, streams in rooms.items():
        start_date, start_time, first_stream = streams[0]
        hls_logger.debug('Beginning analysis of {}...'.format(first_stream))
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

        abort_on_checksum_fail = False
        filename_patterns = _identify_patterns(base_files)
        if 'media_' in filename_patterns or 'media_v2_' in filename_patterns:
            # media_XXX.ts or media_v2_XXX.ts
            # really we should be checking for checksum matches not aborting on fails
            # because this fails when a segment had a botched download
            hls_logger.warning('Detected old-style filename pattern in {}'.format(handle))
            abort_on_checksum_fail = True
        elif len(filename_patterns) > 1:
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
                base_files = new_files
                if new_pattern == 'media_v2_':
                    hls_logger.warning('{} switched from v3 to v2'.format(new_stream))
                    abort_on_checksum_fail = True
                else:
                    abort_on_checksum_fail = False
                continue

            num_moved = move_files(new_files, first_stream, ignore_checksums, abort_on_checksum_fail)
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


def move_files(files, dest, ignore_checksums=False, abort_on_checksum_fail=False, no_probe=False):
    num_moved = 0
    if abort_on_checksum_fail:
        for file in files:
            source, filename = file.split('/')[-2:]
            destfile = '{}/{}'.format(dest, filename)
            if os.path.exists(destfile):
                if not md5sum(file) == md5sum(destfile):
                    hls_logger.error('Checksum mismatch in version 2 stream: {}'.format(source))
                    return num_moved

    for file in files:
        filename = file.split('/')[-1]
        destfile = '{}/{}'.format(dest, filename)
        if not os.path.exists(destfile):
            num_moved += 1
            os.replace(file, destfile)
        # tarring and untarring has invalidated checksums on a few occasions
        # TODO: find a way to prevent or reduce the frequency of this
        # and determine how much damage is actually being done
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
        elif ignore_checksums or md5sum(file) == md5sum(destfile):
            # print('{} exists in destination, removing duplicate'.format(file))
            os.remove(file)
        elif os.path.getsize(file) > os.path.getsize(destfile):
            os.replace(file, destfile)
        else:
            os.remove(file)

    return num_moved


def compare_archives(archive_paths, final_root, simplify_first=False):
    """
    Compare archive folders containing separate recordings of the same streams

    Create a consolidated version of those recordings at final_root

    WARNING: run hls.simplify (or archiver.py hls simplify) on each archive beforehand
    If there are errors during simplify (e.g. two patterns in a folder)
    these must be resolved before running this function

    :param archive_paths:
    :param final_root:
    :param simplify_first: Runs hls.simplify on each archive path before doing anything else
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
        return all((x - times[0]).total_seconds() < 5*60 for x in times[1:])

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
                    check_passed = False
                    break
            if check_passed:
                all_streams.extend(new_streams)
                continue
            # if timestamp checks failed
            # assuming that simplify was run first
            # that means there will only be one folder per filename pattern
            # i *think* that the logic from this point on doesn't require equal length stream lists...
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
            if len(stream_list) > len(archive_paths):
                # more streams in than there should be, run simplify!!!!
                # raise ValueError('Too many streams with the same key, run simplify first! '
                #                  'Problem room: {}'.format(room_name))
                hls_logger.debug('Too many streams with the same stream_key: {}\n{}'.format(room_name, stream_list))
                raise ValueError('Too many streams with the same key, run simplify first!')

            all_streams.append(stream_list)

    # that should be all of them?
    for stream_list in all_streams:
        try:
            stream_list.sort(key=split_stream_name)
        except AttributeError:
            print(stream_list)
            raise
        dest = '{}/{}'.format(final_root, os.path.basename(stream_list[0]))
        final_count, missing = compare_streams(stream_list, final_root)
        if missing:
            hls_logger.info('Missing segments for {}: {}'.format(dest, missing))


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
    for path in scan_paths:
        data[path] = scan_stream(path)

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
    final_segments = (compare_segments(item) for item in zip_longest(*data.values()))
    missing = []
    i = 0
    for i, item in enumerate(final_segments, 1):
        if item:
            destfile = '{}/{}'.format(dest, item['name'])
            if os.path.exists(destfile):
                continue  # avoid writing over a previous run of this function
            # TODO: use shutil.copy2 instead
            # Using os.replace for now because it's faster and i've already made copies
            os.replace(item['path'], destfile)
        else:
            missing.append(i)
    length = i
    return length, missing


def scan_stream(path):
    """
    Scan a stream and return info about each saved segment
    :param path: Path to stream
    :return: a list of dicts containing info about each segment:
        - index
        - name
        - path
        - checksum
        - ffprobe results (to be further parsed)
        - size
    """
    # it would be more efficient to only grab some of this info as needed
    # especially the checksums
    files = sorted(glob.glob('{}/*.ts'.format(path)), key=_segment_sort_key)
    if not files:
        return
    # check that there is only one prefix
    prefixes = set(_segment_sort_key(e)[0] for e in files)
    if len(prefixes) > 1:
        raise ValueError('Too many segment filename prefixes in {}'.format(path))
    final_index = _segment_sort_key(files[-1])[1]
    segments = {}
    for file in files:
        item = {}
        prefix, index = _segment_sort_key(file)
        item['index'] = index
        item['name'] = os.path.basename(file)
        item['path'] = file
        segments[index] = item

    # delay time consuming tasks until the file is asked for
    # TODO: avoid doing any of this unless needed
    for i in range(1, final_index+1):
        item = segments.get(i)
        # if item:
        #     item['probe_result'] = probe_video2(item['path'])
        #     # item['checksum'] = md5sum(item['path'])
        #     item['size'] = os.path.getsize(item['path'])
        yield item

    # return [segments.get(i) for i in range(1, final_index+1)]
