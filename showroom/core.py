#!/usr/bin/env python3
"""
URLs of note

https://www.showroom-live.com/room/is_live?room_id=61879
{"ok": 0} == not live
{"ok": 1} == live (may be other keys)

https://www.showroom-live.com/room/get_live_data?room_id=61747

https://www.showroom-live.com/event/akb48_sousenkyo_45th poll for time schedule

https://www.showroom-live.com/event/all_rooms?event_id=1364

List of all current live broadcasts w/ streaming links
https://www.showroom-live.com/api/live/onlives

All upcoming lives in the Idol genre
https://www.showroom-live.com/api/live/upcoming?genre_id=102

https://www.showroom-live.com/api/time_table/time_tables?order=asc&ended_at=1493621999&_=1492566692848

Find the next live for a room
https://www.showroom-live.com/api/room/next_live?room_id=61576

Video Banner (the strip of text across the top of the video)
https://www.showroom-live.com/api/live/telop?room_id=61627

Comment Log
https://www.showroom-live.com/api/live/comment_log?room_id=61627

Ranking Summary
https://www.showroom-live.com/api/live/summary_ranking?room_id=61627

NetworkInterface sources:
http://stackoverflow.com/a/14671133/3380530

KeyboardInterface sources:
read loop http://stackoverflow.com/a/19655992/3380530

Faster strptime
http://ze.phyr.us/faster-strptime/
    I don't use strptime in this program, but it might be useful elsewhere
    Also I can probably do something similar for strftime


Other observations:
It appears that (dt-dt).total_seconds() calls are extremely fast.
About 0.003 seconds per 10000 operations.
What is peculiar is creating the timedelta object doesn't seem to take any time?
Anyway, this means there's no reason not to move these calls to Watcher.check()

Still need to run cProfile over a long run.


A common feature of failed recordings is an overabundance of 
HandleCtrl, Ping <number>
in the output log


FFmpeg compile requirements:
    openssl
    librtmp
    libx264 (?)
"""
# from sys import stdout, stdin, exit
import datetime
import itertools
import json
import logging
import os
import subprocess
import threading
# import glob
import time
from collections import OrderedDict
# import argparse
from heapq import heapify, heappush, heappop
from json.decoder import JSONDecodeError
from queue import Queue  # Empty as QueueEmpty

# from .message import ShowroomMessage
# from .exceptions import ShowroomDownloadError
from .comments import CommentLogger
from .constants import TOKYO_TZ, HHMM_FMT, FULL_DATE_FMT, WATCHSECONDS, MODE_TO_STATUS
from .index import ShowroomIndex, Room
from .session import WatchSession
from .settings import ShowroomSettings
from .utils import format_name, strftime

# The times and dates reported on the website are screwy, but when fetched
# through BeautifulSoup they *seem* to come in JST
# If you're getting incorrect times you probably need to mess with 
# Schedule.convert_time()
# Or add custom headers to the requests.get() call in Scheduler.tick()


# TODO: create a separate loggers.py or something, set the levels there
# and other important details, e.g. define custom levels for finer verbosity control
core_logger = logging.getLogger('showroom.core')


def watch_seconds(priority: int):
    """
    Translates priority to a watch duration in seconds.

    Looks up the priority in a tuple, returns number of seconds before
    start_time to begin watching a room with the given priority.

    Args:
        priority: An int representing the room's priority.

    Returns:
        Seconds as an int. For all priorities over 10, returns 120,
        else looks up priority in WATCHSECONDS.

    TODO:
        Make this a feature of Watcher objects, calculated on creation or
        updated when (watch) duration or (room) priority is updated.
    """
    if priority > len(WATCHSECONDS):
        return 120
    else:
        return WATCHSECONDS[priority-1]


class Downloader(object):
    """
    Handles downloads for a parent Watcher.

    Created with a room, a session, and an output directory. Started with start(),
    then call wait() to wait on the underlying Popen process. Wait will return when the
    download process ends, either because the stream has completed, because it timed out,
    or because it was terminated from outside the thread. On POSIX systems, a negative
    return code from wait() signals termination/timeout, however this is not portable.

    Regardless of why the download finished, Watcher still needs to check live status, so
    the only reason why termination vs. completion matters is potentially responding to
    repeated timeouts (e.g. like that time all the streams failed for 4 hours)

    Attributes:
        destdir: final destination for the download
        tempdir: temporary ("active") directory
        outfile: name of the file being written to
        all_files: list of files this downloader has written, eventually will be logged
            when the stream completes

    NOTE: all_files is the only attribute that has any reason to be public

    Properties:
        stream_url: url of the stream, picks from rtmp_url and hls_url based on protocol
        protocol: protocol in use, either rtmp or hls (use enum?)
        rtmp_url, hls_url: separate handles for rtmp and hls urls
        timed_out: whether the last wait() timed out

    Methods: (remove this before release)
        start
        wait
        get_info
        is_running -- whether the child process is running
        stop, kill -- usually called from outside the current thread
        update_stream_url -- internal use only
        move_to_dest -- internal use only
        switch_protocol -- don't change protocol, change downloaders

    TODO:
        Use streamlink instead of Popen([ffmpeg...]) ???
        Logging (e.g. "download has started" or let Watcher handle this)
        Fix ffmpeg logging on Windows without pulling in PATH

    DONE:
        For now, instead of the below, just use rtmp streams:

            Separate downloaders for rtmp and hls streams? That is, if one is failing
            instead of switching the protocol, have Watcher pop off the failing stream
            and make a new downloader, handing the failing downloader off to some
            cleanup thread via queue. Or can we handle all cleanup here?

        Add a wait() function that wraps the internal Popen process and checks for fail
        states without bothering the wrapping Watcher. Raise on failure?

    TESTING:
        hls recording fails awfully. find out why
        For the failure detection to work properly, must ffmpeg be compiled with librtmp?
    """
    def __init__(self, room, session, outdir, ffmpeg_logging=True, default_protocol='rtmp'):
        self._room = room
        self._session = session
        self._rootdir = outdir
        self.destdir, self.tempdir, self.outfile = "", "", ""

        self._protocol = default_protocol
        self._rtmp_url = ""
        self._hls_url = ""

        self._process = None
        # self._timeouts = 0
        # self._timed_out = False
        self._pingouts = 0
        self._logging = ffmpeg_logging

        self._lock = threading.Lock()

        # Index of dead processes, list of tuples
        # (outfile, destdir, tempdir, process)
        self._dead_files = []

        # keep a list of previous outfile names
        self.all_files = []

    @property
    def rtmp_url(self):
        return self._rtmp_url

    @property
    def hls_url(self):
        return self._hls_url

    @property
    def stream_url(self):
        return getattr(self, '_{}_url'.format(self.protocol))

    @property
    def protocol(self):
        return self._protocol

    def get_info(self):
        with self._lock:
            return {"streaming_urls": {
                        "hls_url": self._hls_url,
                        "rtmp_url": self._rtmp_url},
                    "protocol": self._protocol,
                    "filename": self.outfile,
                    "dest_dir": self.destdir,
                    "active": self.is_running(),
                    "timeouts": 0,
                    "pingouts": self._pingouts,
                    "completed_files": self.all_files.copy()}

    def is_running(self):
        """Checks if the child process is running."""
        if self._process:
            return self._process.poll() is None
        else:
            return False

    def switch_protocol(self):
        """Switches protocol between rtmp and hls."""
        with self._lock:
            if self.protocol == 'rtmp':
                self._protocol = 'hls'
            else:
                self._protocol = 'rtmp'

    def wait(self):
        """
        Waits for a download to finish.

        Returns:
            returncode of the child process, or None if a ping loop of death was detected.

            On POSIX systems, this will be a negative value if the process
            was terminated (e.g. by timeout) rather than exiting normally.

            Will wait progressively longer if the download keeps timing out.

        TODO:
            Detect ping loop of death ? Or is timeout sufficient?
            Check for other issues, e.g. black 540p
            Logging
            Reset _pingouts?
            I need to check for both pinging and a timeout
            Because the ping message comes from librtmp, and that might not be part
            of ffmpeg
            Check periodically that the stream is still live:
                I've had a couple zombie streams even with the new system
                (no ffmpeg logs, so no idea what happened)
        """
        num_pings = 0
        # Some streams seem to start fine with up to 4 pings before beginning download?
        # More investigation is needed
        max_pings = 1 + self._pingouts
        try:
            for line in self._process.stderr:
                if "Output #0, mp4" in line:
                    self._process.communicate()
                    self.move_to_dest()
                    self._pingouts = 0
                    break
                elif "HandleCtrl, Ping" in line:
                    num_pings += 1
                if num_pings > max_pings:
                    # The main issue with this is that the slain processes will not have their files moved
                    # But I think this is preferable to the other solutions I've come up with.
                    # For future reference, those were:
                    #
                    # 1) Sending SIGINT then continuing to read stderr until it exited (sometimes it doesn't)
                    # 2) Sending SIGINT, storing a reference to the process, then restarting the download.
                    #    This prevents the process from being garbage collected until the Watcher is
                    # 3) Sending SIGINT, then storing info about src and dest paths for the stopped download.
                    #    If a reference to the process is NOT stored, there's no way to be sure it has finished writing
                    #    (if it's writing at all). The only way was to give them a grace period and then just start
                    #    moving, but this adds undesirable time to the cleanup phase, when we may want to restart
                    #    a falsely completed Watcher asap.
                    # 4) Just moving the file straightaway. This is obviously bad since ffmpeg takes a few moments to
                    #    finish.
                    # NOTE: only option #1 was actually tried, the others were partially written before being
                    # abandoned as their problems became clear
                    #
                    # Two additional options exist (not mutually exclusive):
                    # 1) Passing the dead processes off to a queue and having another thread clean up.
                    # 2) Having regular maintenance sweep the active folder and move files it can be sure are done
                    #    to their proper folders.
                    #
                    # I *probably* need to use 1) eventually, especially once I figure out how to actually end
                    # stuck processes without killing the parent. But it requires a lot more code.
                    # Until then let's just see how this works.
                    #
                    # When that time does come, a Downloader copy constructor may be useful.
                    core_logger.debug("Download pinged {} times: Stopping".format(num_pings))
                    self._pingouts += 1
                    self.stop()

                    # close stderr to force the loop to exit
                    time.sleep(0.1)
                    self._process.stderr.close()
                    time.sleep(0.1)
                    # process will be garbage collected when the next one is started, or the Watcher dies
                    # self._process = None
        except ValueError:
            core_logger.debug('ffmpeg stderr closed unexpectedly')

        # Is it possible for the process to end prematurely?
        return self._process.returncode

    def stop(self):
        """Stop an active download.

        Returns immediately, check is_running() for success.
        """
        # trying this instead of SIGTERM
        # http://stackoverflow.com/a/6659191/3380530
        # self._process.send_signal(SIGINT)
        # Or not. SIGINT doesn't exist on Windows
        self._process.terminate()

    def kill(self):
        """Kill an active download.

        Like stop, only tries to kill the process instead of just terminating it.
        Only use this as a last resort, as it will render any video unusable."""
        self._process.kill()

    def move_to_dest(self):
        """Moves output file to its final destination."""
        destpath = self._move_to_dest(self.outfile, self.tempdir, self.destdir)

        if destpath:
            self.all_files.append(destpath)
            core_logger.info('Completed {}'.format(destpath))

        with self._lock:
            self.outfile = ""

    @staticmethod
    def _move_to_dest(outfile, tempdir, destdir):
        srcpath = '{}/{}'.format(tempdir, outfile)
        destpath = '{}/{}'.format(destdir, outfile)
        if os.path.exists(destpath):
            raise FileExistsError
        else:
            try:
                os.replace(srcpath, destpath)
            except FileNotFoundError:
                return
            else:
                return destpath

    def update_streaming_url(self):
        """Updates streaming urls from the showroom website."""
        data = self._session.json('https://www.showroom-live.com/room/get_live_data',
                                  params={'room_id': self._room.room_id},
                                  headers={'Referer', self._room.long_url})
        if not data:
            pass  # how to resolve this? can it even happen without throwing an exception earlier?

        # TODO: Check that strings aren't empty
        stream_name = data['streaming_name_rtmp']
        stream_url = data["streaming_url_rtmp"]
        new_rtmp_url = '{}/{}'.format(stream_url, stream_name)
        new_hls_url = data["streaming_url_hls"]

        with self._lock:
            if new_rtmp_url != self.rtmp_url:
                # TODO: log url change
                # TODO: Trigger this message when the stream first goes live, from elsewhere
                # print('Downloading {}\'s Showroom'.format(self.room.name))
                # self.announce((self.web_url, self.stream_url))
                pass

            if new_hls_url != self.hls_url:
                # TODO: log url change
                pass

            self._rtmp_url = new_rtmp_url
            self._hls_url = new_hls_url

    def start(self):
        """
        Starts the download.

        Refreshes the streaming url, generates a new file name, and starts a new ffmpeg
        process.

        Returns:
            datetime object representing the time the download started
        """
        self.update_streaming_url()
        tokyo_time = datetime.datetime.now(tz=TOKYO_TZ)
        temp, dest, out = format_name(self._rootdir,
                                      strftime(tokyo_time, FULL_DATE_FMT),
                                      self._room)

        with self._lock:
            self.tempdir, self.destdir, self.outfile = temp, dest, out

        # TODO: fix this so it works on windows
        # TODO: Fix implemented, now just need to check it.
        # passing env variables results in using custom ffmpeg instead of system ffmpeg
        # which doesn't print handlectrl, ping
        # TODO: determine windows/etc properly elsewhere
        if os.name == 'nt':
            env = os.environ.copy()
        else:
            env = {}

        if self._logging is True:
            log_file = os.path.normpath('{}/logs/{}.log'.format(self.destdir, self.outfile))
            env.update({'FFREPORT': 'file={}:level=40'.format(log_file)})
            # level=48  is debug mode, with lots and lots of extra information
            # maybe too much
        normed_outpath = os.path.normpath('{}/{}'.format(self.tempdir, self.outfile))

        if self.protocol == 'hls':
            extra_args = ['-bsf:a', 'aac_adtstoasc']
        else:
            extra_args = []

        self._process = subprocess.Popen([
            'ffmpeg',
            # '-nostdin',
            # '-nostats',  # will this omit any useful information?
            '-loglevel', '40',  # 40+ required for wait() to check output
            '-copytb', '1',
            '-i', self.stream_url,
            '-c', 'copy',
            *extra_args,
            normed_outpath
            ],
            stdin=subprocess.DEVNULL,
            stderr=subprocess.PIPE,  # ffmpeg sends all output to stderr
            universal_newlines=True,
            bufsize=1,
            env=env)


class Watcher(object):
    """Manages downloads for a single room/stream.

    TODO:
        docstrings
        logging
        flow analysis for run()
        review end states
        option to download all streams but only keep wanted
            instead of default of only downloading wanted
    """
    def __init__(self, room: Room, session: WatchSession, settings: ShowroomSettings,
                 update_flag: threading.Event=None, start_time: datetime.datetime=None,
                 watch_duration: int=None):
        self._lock = threading.RLock()
        if update_flag:
            self._update_flag = update_flag
        else:
            self._update_flag = threading.Event()

        self._room = room
        self._session = session
        self._settings = settings

        self._download = Downloader(room, session, settings.directory.output, settings.ffmpeg.logging)

        if self._settings.comments.record and self.priority < self._settings.comments.max_priority:
            self.comment_logger = CommentLogger(self.room, self._session, self._settings, self)
        else:
            self.comment_logger = None
        # originally start_time was the time the stream began recording
        # now however i'm using the start_time according to Showroom
        # so Watcher is always created with a start_time
        if start_time:
            self.__start_time = start_time
        else:
            self.__start_time = datetime.datetime.now(tz=TOKYO_TZ)

        self._end_time = None

        self._watch_duration = watch_duration
        self._watch_start_time = self._watch_end_time = None
        self.set_watch_time(self.__start_time, self.watch_duration)

        self._live = False
        self.__live_time = datetime.datetime.fromtimestamp(0.0, tz=TOKYO_TZ)

        self.__mode = "schedule"

    # mainly used by hacked together priority heapq
    def __bool__(self):
        return bool(self._room)

    # access to internal objects
    @property
    def room(self):
        return self._room

    @property
    def download(self):
        return self._download

    # informational properties
    @property
    def name(self):
        return self._room.name

    @property
    def web_url(self):
        return self.room.long_url
    
    @property
    def room_id(self):
        return self._room['showroom_id']

    @property
    def priority(self):
        return self._room['priority']

    @property
    def watch_duration(self):
        """
        Time in seconds to start watching a room ahead of when it is scheduled to go live.
        Will keep watching for watch_duration*2 after scheduled start_time
        """
        if self._watch_duration:
            return self._watch_duration
        else:
            return watch_seconds(self.priority)

    @property
    def mode(self):
        return self.__mode

    @property
    def formatted_start_time(self):
        return strftime(self.__start_time, HHMM_FMT)

    @property
    def start_time(self):
        return self.__start_time

    # internal properties
    @property
    def _start_time(self):
        return self.__start_time

    @_start_time.setter
    def _start_time(self, new_time):
        with self._lock:
            self.__start_time = new_time

    @property
    def _mode(self):
        return self.__mode

    @_mode.setter
    def _mode(self, new_mode):
        with self._lock:
            self.__mode = new_mode

    @property
    def __watch_rate(self):
        return self._settings.throttle.rate.watch

    @property
    def __live_rate(self):
        return self._settings.throttle.rate.live

    @property
    def __download_timeout(self):
        return self._settings.throttle.timeout.downloads

    def get_info(self):
        """Returns a dictionary describing the Watcher's state.

        Also returns info for child Downloader and Room objects."""
        with self._lock:
            room_info = self.room.get_info()
            return {
                # TODO: fix the write_completed method below to handle datetime
                "start_time": self._start_time,
                "end_time": self._end_time,
                "live": self.is_live(),
                "mode": self._mode,
                # this is kinda hokey, but it's needed often enough so...
                "name": room_info['name'],
                "room": room_info,
                "download": self.download.get_info()}

    # TODO: review uses and functionality of these two methods
    def reschedule(self, new_time):
        with self._lock:
            if self._mode == "schedule":
                self._start_time = new_time
                self.set_watch_time(new_time)
                self._update_flag.set()

    def set_watch_time(self, watch_time, watch_duration: int=None):
        with self._lock:
            if watch_duration is None:
                watch_duration = self.watch_duration
            self._watch_start_time = watch_time - datetime.timedelta(seconds=watch_duration)
            self._watch_end_time = watch_time + datetime.timedelta(seconds=watch_duration*2.0)

    def is_live(self):
        """Returns whether the stream is live or not.

        May be stale"""
        return self._live

    def _watch_ready(self):
        # start watch_seconds before start_time
        # finish watch_seconds * 2 after start_time
        curr_time = datetime.datetime.now(tz=TOKYO_TZ)

        # TODO: is this noticeably slower than the old (int > (curr - start).totalseconds() > int)
        if (self._watch_start_time
                < curr_time
                < self._watch_end_time):
            return True
        else:
            return False

    def _live_ready(self):
        curr_time = datetime.datetime.now(tz=TOKYO_TZ)
        if (curr_time - self.__live_time).total_seconds() > self.__live_rate:
            self.__live_time = curr_time
            return True
        else:
            return False

    def check_live_status(self):
        """Checks if the stream is live or not.

        This actually checks the website"""
        status = self._session.json('https://www.showroom-live.com/room/is_live',
                                    params={"room_id": self.room_id},
                                    headers={'Referer', self.room.long_url},
                                    default={'ok': 0})['ok']

        if status == 0:
            self._live = False
        elif status == 1:
            self._live = True
        return self._live

    def stop(self):
        self._mode = "quitting"
        if self._download.is_running():
            self._download.stop()

    def kill(self):
        with self._lock:
            if self._mode == "quitting" and self.download.is_running():
                self._download.kill()

    def run(self):
        """
        Watcher flow:

        Case 1: Scheduled Live
            1) New Watcher is created at step "schedule" and started
            2) Enter schedule loop:
                Check if curr_time is close enough to start_time to begin watching.
                Manager may update start_time if the schedule changes.
                If curr_time is close enough, change mode to watch
                Else, sleep for a short period of time
            3) Enter watch loop:
                Check if stream is live
                If live, update start_time and check room.is_wanted()
                WARNING: ensure Watcher and Manager don't overwrite each other's start_times
                    Manager should only update start_time if in schedule mode
                    e.g. use a reschedule() method that locks mode until updated? I don't think that's sufficient
                    Watcher should only update start_time if in watch mode
                If room.is_wanted()
            3) When the room goes live, Watcher starts the download and switches to "download"
            4) When the stream ends, Watcher completes the download and switches to "complete"
               and the thread ends (returns a completed watcher?)
            FLOW: schedule -> watch -> download -> completed

        Returns:
            Nothing
        """
        self._update_flag.set()
        core_logger.debug('Entering {} mode for {}'.format(self.mode, self.name))
        while self._mode == "schedule":
            if self._watch_ready():
                core_logger.info('Watching {}'.format(self.name))
                self._mode = "watch"
            else:
                time.sleep(1.0)

        core_logger.debug('Entering {} mode for {}'.format(self.mode, self.name))
        while self._mode == "watch":
            if self._watch_ready():
                if self.check_live_status():
                    self._start_time = datetime.datetime.now(tz=TOKYO_TZ)
                    core_logger.info('{} is now live'.format(self.name))
                    if self.room.is_wanted():
                        self._mode = "download"
                    else:
                        self.download.update_streaming_url()
                        self._mode = "live"
                else:
                    # This is okay as long as watch rate is a short period of time
                    time.sleep(self.__watch_rate)
            else:
                self._mode = "expired"

        if self.mode in ("live", "download"):
            self._update_flag.set()
            if self.comment_logger:
                self.comment_logger.start()

        core_logger.debug('Entering {} mode for {}'.format(self.mode, self.name))
        while self._mode in ("live", "download"):
            # These are together so that users can toggle
            # "wanted" status and switch between them, though it would almost be better
            # if we just automatically recorded everything and discarded unwanted files...
            # except when stuff like New Year's happens.
            # TODO: add an optional flag (to settings) that does exactly that
            while self._mode == "live":
                if self._live_ready():
                    # TODO: periodically update the streaming urls
                    if self.check_live_status():
                        if self.room.is_wanted():
                            self._mode = "download"
                    else:
                        self._end_time = datetime.datetime.now(tz=TOKYO_TZ)
                        self._mode = "completed"
                time.sleep(1.0)

            while self._mode == "download":
                # this happens at the top here so that changing mode to "quitting"
                # will cause the loop to break before the download is resumed
                # check_live_status was moved to the end to avoid
                # pinging the site twice whenever a download starts
                if self.is_live():
                    if self.room.is_wanted():
                        self.download.start()
                    else:
                        self._mode = "live"
                else:
                    self._end_time = datetime.datetime.now(tz=TOKYO_TZ)
                    self._mode = 'completed'

                # self.download.wait(timeout=self.__download_timeout)
                self.download.wait()
                time.sleep(0.5)
                self.check_live_status()

        core_logger.debug('Entering {} mode for {}'.format(self.mode, self.name))
        # TODO: decide what to do with the three end states
        if self._mode == "quitting":
            # what is this mode? it's presumably a way to break out of the download loop
            # to quit:
            #    change mode to quitting
            #    and call stop() on the downloader
            # actually we should never make it to this block while the download is active, right?
            # unless it times out. but if it times out and the process doesn't end, wait() will
            # never return, so again we'll never reach this block. Thus, all I need to do is...
            self._mode = "completed"

        self._update_flag.set()
        if self._mode == "expired":
            # download never started
            # watcher needs to vacate the premises as fast as possible
            # how it does that I'm not really sure... it depends what WatchManager is doing
            # If watchqueue has a window that returns all expired watchers, that would work
            return
        elif self._mode == "completed":
            # download started and finished, and has already been moved to dest
            return


class WatchQueue(object):
    """Priority heap queue that also permits iteration.

    TODO:
        Review need for this object
        Write a proper docstring
        Decide how to quit
        Deal with downloads and lives in remove/prune methods.
        Review need for prune
        Review need for list_info
    """
    REMOVED = None
    MODE_GROUPS = {"upcoming": ("schedule",),
                   "working":  ("schedule", "watch", "download", "live"),
                   "active":   ("watch", "download", "live"),
                   "live":     ("live", "download"),
                   "ending":   ("quitting",),
                   "done":     ("expired", "completed")}

    def __init__(self):
        self.queue = []
        self.entry_map = {}
        self._counter = itertools.count()
        self._dirty = False
        self._rlock = threading.RLock()

    def __len__(self):
        return len(self.entry_map)

    def __iter__(self):
        with self._rlock:
            _index = 0

            while _index < len(self.queue):
                val = self.queue[_index][2]
                if val is not None:
                    yield val
                _index += 1

    def __bool__(self):
        return len(self.entry_map) > 0

    def __getitem__(self, key):
        return self.entry_map[key][2]

    def __contains__(self, room_id):
        with self._rlock:
            if room_id in self.entry_map:
                return True
            else:
                return False

    # This looks nicer than ~7 different methods, but is it clearer?
    def get_by_mode(self, mode):
        """Returns an iterator through all Watchers with the given mode or mode group.

        Modes:
            schedule
            watch
            live (both live and download)
            download
            quitting (or ending)
            expired
            completed

        Groups:
            upcoming: schedule
            live: live, download
            working: schedule, watch, live, download, quitting
            active: watch, live, download
            done: expired, completed
        """
        if mode in self.MODE_GROUPS:
            mode = self.MODE_GROUPS[mode]
        else:
            mode = (mode,)

        with self._rlock:
            yield from (i for i in self if i.mode in mode)

    def ids(self):
        """Returns all room ids in the queue.

        Of debatable utility."""
        with self._rlock:
            return self.entry_map.copy().keys()
    
    def add(self, item):
        """Adds an item to the queue.

        Preserves heap invariant."""
        if item.room_id in self.entry_map:
            return False  # do nothing
        else:
            with self._rlock:
                count = next(self._counter)
                entry = [item.priority, count, item]
                self.entry_map[item.room_id] = entry
                heappush(self.queue, entry)
                return True

    def pop(self):
        """Pops the item at the front of the queue.

        Preserves heap invariant."""
        with self._rlock:
            while self.queue:
                priority, count, item = heappop(self.queue)
                if item is not self.REMOVED:
                    del self.entry_map[item.room_id]
                    return item

    def replace(self, item):
        """Places an item on the queue and pops another from the front of the queue.

        Preserves heap invariant.

        Args:
            Item to place on the queue.

        Returns:
            Item formerly at the front of the queue.

        Issues:
            Unneeded.
        """
        with self._rlock:
            if item.room_id in self.entry_map:
                return self.pop()
            else:
                count = next(self._counter)
                entry = [item.priority, count, item]
                self.entry_map[item.room_id] = entry
                heappush(self.queue, entry)
                return self.pop()

    def peek(self):
        """Peeks at the front of the queue without popping.

        May reentrant lock while rebuilding the queue."""
        if len(self) > 0:
            result = self.queue[0][2]
            if not result:
                self.rebuild()
                result = self.queue[0][2]
            return result
        else:
            return None

    def rebuild(self):
        """Rebuilds the queue, removing dead items left by dirty operations.

        Dirty operations:
            remove
            dirty_pop
            pop_end
            prune
        """
        if self._dirty:
            with self._rlock:
                self.queue = [e for e in self.queue if e[2] != self.REMOVED]
                heapify(self.queue)

    def remove(self, item):
        """Removes item from queue.

        Heap should be rebuilt afterwards."""
        with self._rlock:
            entry = self.entry_map.pop(item.room_id)
            entry[2] = self.REMOVED
            self._dirty = True

    # DownloadQueue ver.
    '''
    def remove(self, item, kill=False):
        if kill or self.entry_map[item.room_id][2].check():
            entry = self.entry_map.pop(item.room_id)
            if kill:
                print('Ending download for {}'.format(entry[-1].name))
                entry[-1].kill()
                if not entry[-1].check():
                    entry[-1].kill()
            entry[-1] = self.REMOVED
            self.dirty = True
            return True
        else:
            return False
    '''

    def dirty_pop(self, item):
        """Pops a specific item from anywhere in the queue.

        Heap should be rebuilt afterwards."""
        with self._rlock:
            entry = self.entry_map.pop(item.room_id)
            if entry:
                result = entry[2]
                entry[2] = self.REMOVED
                self._dirty = True
                return result
            else:
                return None

    def pop_end(self):
        """Removes the last item from the queue.

        Heap should be rebuilt afterwards."""
        with self._rlock:
            while self.queue:
                priority, count, item = self.queue.pop(-1)
                if item is not self.REMOVED:
                    del self.entry_map[item.room_id]
                    self._dirty = True
                    return item

    def prune(self, priority):
        """Removes a low priority item from the end of the queue.

        Heap should be rebuilt afterwards.

        Note that "low" priority is a slight misnomer, since the lowest
        *value* priorities are actually the "highest", most resistant to pruning.

        In general this method should be avoided, and rooms changed to wanted/unwanted
        instead.
        """
        with self._rlock:
            while self.queue:
                if self.queue[-1][2] is None:
                    self.queue.pop(-1)
                elif self.queue[-1][2].priority > priority:
                    self.remove(self.queue[-1][2])
                    return True
                else:
                    return False

    # DownloadQueue ver.
    '''
    def prune(self, priority):
        if self.queue[-1][2].priority >= priority * 2:
            print('Killing {} to make room'.format(self.queue[-1][2].name))
            self.remove(self.queue[-1][2], kill=True)
            return True
        else:
            return False
    '''

    # DownloadQueue quit method
    '''
    def quit(self):
        self.rebuild()
        while self:
            done = []
            for d in self.queue:
                try:
                    next(d[2].quit())
                except StopIteration:
                    done.append(d)
            for d in done:
                super().remove(d[2])
            self.rebuild()
            done = []
    '''


class WatchManager(object):
    def __init__(self, index: ShowroomIndex, settings: ShowroomSettings):
        """
        
        """
        # maintains a list?
        # does it still need a priority queue?
        # various permutations of the base list
        self.index = index
        self.session = WatchSession()
        self.settings = settings
        self.watchers = WatchQueue()
        self.completed = []

        self._threads = {}
        self._counter = itertools.count()
        self._undead_threads = Queue()
        # TODO: undead thread handler?

        self._completed_lock = threading.RLock()

        self.__schedule_time = datetime.datetime.fromtimestamp(0.0, tz=TOKYO_TZ)
        self.__lives_time = self.__schedule_time

        self.update_flag = threading.Event()

        # TODO: design better organised configuration/settings
        if self.settings.feedback.write_schedules_to_file:
            self._schedule_update_thread = threading.Thread(target=self.write_schedules, name="ScheduleWriter")
            self._schedule_update_thread.daemon = True
            self._schedule_update_thread.start()

    def __len__(self):
        return len(self.watchers)

    def list_ids(self):
        """Returns a list of room ids in the child WatchQueue.

        List may become stale, check that item is still in the queue before
        operating on it."""
        return self.watchers.ids()

    # TODO: This is unneeded, I think, but I do need to reset other state overnight
    # And print a list of completed lives
    '''
    def reset_ticks(self):
        self._tick_count = 0
    '''

    # These three properties are not in use at the moment
    @property
    def output_dir(self):
        return self.settings.directory.output
    
    @property
    def max_watches(self):
        return self.settings.throttle.max.watches
    
    @property
    def max_downloads(self):
        return self.settings.throttle.max.downloads

    def _setup_thread(self, watcher):
        """
        Sets up, names, and starts a thread for the watcher.

        Args:
            A Watcher object ready to start.

        Returns:
            Nothing
        """
        if watcher.room_id in self._threads:
            if self._threads[watcher.room_id].is_alive():
                # TODO: handle this error
                pass
        thread_name = "Watcher-{count}-{name}".format(name=watcher.name, count=next(self._counter))
        t = threading.Thread(target=watcher.run, name=thread_name)
        t.start()
        self._threads[watcher.room_id] = t

    def update_lives(self):
        """Looks for unexpected live rooms."""
        onlives_url = 'https://www.showroom-live.com/api/live/onlives'
        onlives = self.session.json(onlives_url,
                                    headers={'Referer': 'https://www.showroom-live.com/onlive'},
                                    default={'onlives': []})['onlives']

        # find the idol genre
        for e in onlives:
            # can't find "genre_name":"アイドル" in input, possible de/encoding problem?
            if e['genre_id'] == 102:
                lives = e['lives']
                break
        else:
            core_logger.debug('Couldn\'t find idol live category')
            return

        # lives is a list of json objects (dicts)
        # representing items to include in the page
        # both rooms and groups
        # entries with:
        #   "cell_type": 0
        # represent page formatting clues, e.g. the header for mainichi idol/onlives
        # other useful in each entry:
        #   "started_at": 1484559975
        #   "room_url_key": "48_YUNA_EGO"
        #   "follower_num": 9690
        #   "view_num": 9662
        # core_logger.debug('Checking idol lives')
        for item in [e for e in lives if 'room_id' in e and str(e['room_id']) in self.index]:
            room_id = str(item['room_id'])
            start_time = datetime.datetime.fromtimestamp(float(item['started_at']), tz=TOKYO_TZ)

            # core_logger.debug('Checking live room id {}'.format(room_id))
            if room_id in self.watchers:
                if self.watchers[room_id].mode == "schedule":
                    self.watchers[room_id].reschedule(start_time)
                    self.watchers[room_id].set_watch_time(datetime.datetime.now(tz=TOKYO_TZ))
                    core_logger.debug('Early live for {} at {}'.format(self.watchers[room_id].name,
                                                                       self.watchers[room_id].formatted_start_time))
            else:
                new = Watcher(self.index[room_id], self.session, self.settings,
                              update_flag=self.update_flag, start_time=start_time)
                new.set_watch_time(datetime.datetime.now(tz=TOKYO_TZ))
                info = new.get_info()
                core_logger.debug('Unscheduled live for {} starting at {}'.format(info['name'], info['start_time']))
                self.add(new)

    def update_schedule(self):
        """Checks the schedule and adds watchers for any new rooms found."""
        # TODO: get multiple genres
        upcoming_url = 'https://www.showroom-live.com/api/live/upcoming?genre_id=102'
        upcoming = self.session.json(upcoming_url,
                                     headers={'Referer': 'https://www.showroom-live.com/onlive'},
                                     default={'upcomings': []})['upcomings']

        for item in [e for e in upcoming if str(e['room_id']) in self.index]:
            start_time = datetime.datetime.fromtimestamp(float(item['next_live_start_at']), 
                                                         tz=TOKYO_TZ)
            room_id = str(item['room_id'])

            if room_id in self.watchers:
                if (self.watchers[room_id].mode == 'schedule' and
                        start_time != self.watchers[room_id].start_time):
                    # Update start_time if still in schedule mode, otherwise
                    # defer this until the room finishes its current live
                    # This seems like way too many things to check tbh
                    # but then the schedule won't be updated that often.
                    # Will occasionally get false positives if the schedule is updated
                    # right as the room goes live, but that's fine because
                    # reschedule won't let start_time be changed if it's not still
                    # in schedule mode.
                    self.watchers[room_id].reschedule(start_time)
                    core_logger.debug('{} rescheduled for {}'.format(self.watchers[room_id].name,
                                                                     self.watchers[room_id].formatted_start_time))
            else:
                new = Watcher(self.index[room_id], self.session, self.settings,
                              update_flag=self.update_flag, start_time=start_time)
                core_logger.info('{} scheduled for {}'.format(new.name, new.formatted_start_time))
                self.add(new)

    def update_completed(self):
        for watch in self.watchers.get_by_mode("done"):
            with self._completed_lock:
                watch = self.watchers.dirty_pop(watch)
                self.completed.append(watch)
            try:
                thread = self._threads.pop(watch.room_id)
            except KeyError:
                # TODO: handle this error
                pass
            else:
                # is this check necessary?
                # is it too fast?
                if thread.is_alive():
                    # TODO: log undead threads
                    # TODO: handle undead threads elsewhere
                    self._undead_threads.put(thread)
        self.watchers.rebuild()

    def write_schedules(self):
        outfile = self.settings.file.schedule

        def lookup_mode(mode):
            return MODE_TO_STATUS[mode]

        # TODO: toggle this on/off
        while self.update_flag.wait():
            # TODO: allow exit
            # it's a daemon thread so it shouldn't matter
            watchers = self.get_working_list()
            # index_filter = self.index.filter_get_list()
            self.update_flag.clear()

            schedules = []
            for item in watchers:
                status = lookup_mode(item['mode'])
                new_schedule = OrderedDict([('name', item['name']),
                                            ('live', False if status not in ('live', 'downloading') else True),
                                            ('status', status),
                                            ('start_time', strftime(item['start_time'], FULL_DATE_FMT)),
                                            ('streaming_urls', item['download']['streaming_urls'].copy()),
                                            ('room', item['room'])])
                schedules.append(new_schedule)

            # TODO: add filter management to index
            # TODO: add a way to verify that the filters are set correctly

            # schedules should already be sorted
            core_logger.debug('Writing schedules to file')
            with open(outfile, 'w', encoding='utf8') as outfp:
                json.dump(schedules, outfp, ensure_ascii=False, indent=2)

            # even if the update_flag gets set again we don't want to spit out another update so fast
            # TODO: make the sleep time here configurable?
            time.sleep(4.0)

    def write_completed(self):
        """Called by the manager?"""
        # TODO: add today's date to the completed file, change it during nightly maintenance
        outfile = self.settings.file.completed
        try:
            with open(outfile, 'r', encoding='utf8') as infp:
                completed = json.load(infp)
        except FileNotFoundError:
            completed = []
        except JSONDecodeError:
            # TODO: backups
            raise

        with self._completed_lock:
            for item in self.completed:
                info = item.get_info()
                for key in ('start_time', 'end_time'):
                    info[key] = str(info[key])
                completed.append(info)
            self.completed = []

        with open(outfile, 'w', encoding='utf8') as outfp:
            json.dump(completed, outfp, indent=2, ensure_ascii=False)

    def add(self, watcher):
        if watcher.room_id not in self.watchers:
            # TODO: tell the logger about newly scheduled rooms
            self._setup_thread(watcher)
            self.watchers.add(watcher)

    def get_working_list(self):
        """Returns a list of all currently scheduled and live rooms"""
        # Watchers that aren't in one of these 4 modes shouldn't be in the WatchQueue any more.
        return sorted((e.get_info() for e in self.watchers.get_by_mode("working")),
                      key=lambda x: (x['start_time'], x['room']['name']))

    @property
    def __schedule_rate(self):
        return self.settings.throttle.rate.upcoming

    @property
    def __lives_rate(self):
        return self.settings.throttle.rate.onlives

    def _schedule_ready(self):
        curr_time = datetime.datetime.now(tz=TOKYO_TZ)
        time_diff = (curr_time - self.__schedule_time).total_seconds()
        if time_diff > self.__schedule_rate:
            # core_logger.debug('Time difference of {} is greater than schedule rate of {}, '
            #                   'beginning schedule check'.format(time_diff, self.__schedule_rate))
            self.__schedule_time = curr_time
            return True
        else:
            # core_logger.debug('Skipping schedule check')
            return False

    def _lives_ready(self):
        curr_time = datetime.datetime.now(tz=TOKYO_TZ)
        time_diff = (curr_time - self.__lives_time).total_seconds()
        if time_diff > self.__lives_rate:
            # core_logger.debug('Time difference of {} is greater than live rate of {}, '
            #                   'beginning live check'.format(time_diff, self.__lives_rate))
            self.__lives_time = curr_time
            return True
        else:
            # core_logger.debug('Skipping live check')
            return False

    def tick(self):
        """Periodic live and schedule check"""

        if self._lives_ready():
            # core_logger.debug('Checking lives')

            # completed is checked before lives so that a completed room
            # doesn't prevent a new live from being added

            self.update_completed()
            self.update_lives()

        if self._schedule_ready():
            core_logger.debug('Checking schedule')
            self.update_schedule()
            core_logger.debug('{} active watchers'.format(len(self.watchers)))

    def stop(self):
        for watch in self.watchers:
            watch.stop()
        while self.watchers:
            self.update_completed()
            time.sleep(0.5)
        # TODO: handle zombie threads/watchers




