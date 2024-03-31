import datetime
import threading
import time
import logging
from queue import Empty as QueueEmpty

from showroom.api import ShowroomClient
from showroom.api.broadcast import ShowroomWebSocket
from showroom.constants import TOKYO_TZ, HHMM_FMT
from showroom.downloader import Downloader
from showroom.index import Room
from showroom.settings import ShowroomSettings
from showroom.utils import strftime

core_logger = logging.getLogger('showroom.watcher')
WATCHSECONDS = (600, 420, 360, 360, 300, 300, 240, 240, 180, 150)


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
    elif priority < 0:
        return 600
    else:
        return WATCHSECONDS[priority-1]


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
    def __init__(self, room: Room, client: ShowroomClient, settings: ShowroomSettings,
                 update_flag: threading.Event = None, start_time: datetime.datetime = None,
                 watch_duration: int = None):
        self._lock = threading.RLock()
        if update_flag:
            self._update_flag = update_flag
        else:
            self._update_flag = threading.Event()

        self._room = room
        self._client = client
        self._settings = settings

        self._download = Downloader(room, client, settings)
        # if self._settings.comments.record and self.priority < self._settings.comments.max_priority:
        #     self.comment_logger = CommentLogger(self.room, self._client, self._settings, self)
        # else:
        #     self.comment_logger = None
        self._ws = None
        self._messages = []

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
        return self._room.room_id

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

    def set_watch_time(self, watch_time, watch_duration: int = None):
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

    # def check_live_status(self):
    #     """Checks if the stream is live or not.
    #
    #     This actually checks the website"""
    #     try:
    #         self._live = self._client.is_live(self.room_id)
    #     except HTTPError as e:
    #         core_logger.warning('Caught HTTPError while checking room\'s live status: {}'.format(e))
    #         self._live = False
    #     return self._live

    def stop(self):
        self._mode = "quitting"
        if self._download.is_running():
            self._download.stop()
        if self._ws:
            self._ws.stop()
        # self.comment_logger.quit()

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

        # core_logger.debug('Entering {} mode for {}'.format(self.mode, self.name))
        while self._mode == "schedule":
            if self._watch_ready():
                core_logger.info('Watching {}'.format(self.name))
                self._mode = "watch"
            else:
                time.sleep(1.0)

        # core_logger.debug('Entering {} mode for {}'.format(self.mode, self.name))
        while self._mode == "watch":
            if not self._ws:
                info = self._client.room_status(self.room.room_url_key)
                if info['live_status'] == 2:
                    self._mode = 'live'
                    break
                else:
                    self._ws = ShowroomWebSocket(
                        self._client._session,
                        self._room.name,
                        server_key=info['broadcast_key']
                    )
                    self._ws.start()

            if not self._ws.empty():
                try:
                    msg = self._ws.get()
                except QueueEmpty:
                    continue
                else:
                    if int(msg['t']) == 104:
                        self._mode = 'live'
                        self._live = True
                        self._ws._quit = True
                        self._ws = None
                    else:
                        core_logger.warning(f'Unknown message while listening to offline stream: {msg}')

        if self.mode in ("live", "download"):
            self._update_flag.set()
            # if self.comment_logger:
            #     self.comment_logger.start()

        # core_logger.debug('Entering {} mode for {}'.format(self.mode, self.name))
        while self._mode in ("live", "download"):
            # These are together so that users can toggle
            # "wanted" status and switch between them, though it would almost be better
            # if we just automatically recorded everything and discarded unwanted files...
            # except when stuff like New Year's happens.
            # TODO: add an optional flag (to settings) that does exactly that
            info = self._client.room_status(self.room.room_url_key)
            self._ws = ShowroomWebSocket(
                self._client._session,
                self._room.name,
                server_key=info['broadcast_key']
            )
            self._ws.start()

            while self._mode == "live":
                if self.room.is_wanted():
                    self._mode = "download"
                else:
                    messages = []
                    while not self._ws.empty():
                        try:
                            msg = self._ws.get()
                        except QueueEmpty:
                            break
                        else:
                            messages.append(msg)
                            if int(msg['t']) == 101:
                                self._end_time = datetime.datetime.now(tz=TOKYO_TZ)
                                self._mode = "completed"
                                self._live = False
                time.sleep(1.0)

            while self._mode == "download":
                # this happens at the top here so that changing mode to "quitting"
                # will cause the loop to break before the download is resumed
                # check_live_status was moved to the end to avoid
                # pinging the site twice whenever a download starts
                live_info = self._client.live_info(self.room_id)
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

        # core_logger.debug('Entering {} mode for {}'.format(self.mode, self.name))
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

