#!/usr/bin/env python3
"""
URLs of note

https://www.showroom-live.com/room/is_live?room_id=61879
{"ok": 0} == not live
{"ok": 1} == live (may be other keys)

!!!! As of 2017-10-23, this url no longer works.
~~~ https://www.showroom-live.com/room/get_live_data?room_id=61747

https://www.showroom-live.com/event/akb48_sousenkyo_45th poll for time schedule

https://www.showroom-live.com/event/all_rooms?event_id=1364

List of all current live broadcasts w/ streaming links
https://www.showroom-live.com/api/live/onlives

All upcoming lives in the Idol genre
https://www.showroom-live.com/api/live/upcoming?genre_id=102

https://www.showroom-live.com/api/time_table/time_tables?order=asc&ended_at=1493621999&_=1492566692848

Find the next live for a room
https://www.showroom-live.com/api/room/next_live?room_id=61576

Some basic info about a live broadcast
https://www.showroom-live.com/api/live/live_info?room_id=44010
{
  "age_verification_status": 0,
  "video_type": 0,
  "enquete_gift_num": 0,
  "is_enquete": false,
  "bcsvr_port": 8080,
  "live_type": 0,
  "is_free_gift_only": false,
  "bcsvr_host": "online.showroom-live.com",
  "live_id": 2741506,
  "is_enquete_result": false,
  "live_status": 2,
  "room_id": 44010,
  "bcsvr_key": "29d502:AJ4IiAqb",
  "background_image_url": null
}
https://www.showroom-live.com/api/live/live_info?room_id=75207
{
  "age_verification_status": 0,
  "video_type": 0,
  "enquete_gift_num": 0,
  "is_enquete": false,
  "bcsvr_port": 8080,
  "live_type": 0,
  "is_free_gift_only": false,
  "bcsvr_host": "online.showroom-live.com",
  "live_id": 0,
  "is_enquete_result": false,
  "live_status": 1,
  "room_id": 75207,
  "bcsvr_key": "",
  "background_image_url": null
}

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
import re
import threading
# import glob
import time
from collections import OrderedDict
# import argparse
from heapq import heapify, heappush, heappop
from json.decoder import JSONDecodeError
from queue import Queue

from requests.exceptions import HTTPError

from showroom.api import ShowroomClient

# from .message import ShowroomMessage
# from .exceptions import ShowroomDownloadError
# from .comments import CommentLogger
from .constants import TOKYO_TZ, FULL_DATE_FMT, MODE_TO_STATUS
from .index import ShowroomIndex
from .settings import ShowroomSettings
from .utils import strftime
from .watcher import Watcher

# The times and dates reported on the website are screwy, but when fetched
# through BeautifulSoup they *seem* to come in JST
# If you're getting incorrect times you probably need to mess with 
# Schedule.convert_time()
# Or add custom headers to the requests.get() call in Scheduler.tick()


# TODO: create a separate loggers.py or something, set the levels there
# and other important details, e.g. define custom levels for finer verbosity control

core_logger = logging.getLogger('showroom.core')

hls_url_re1 = re.compile(r'(https://edge-(\d*)-(\d*)-(\d*)-(\d*).showroom-live.com:443/liveedge/(\w*))/playlist.m3u8')

# TODO: Make this a config file option
STREAM_PREFERENCE = ("rtmp", "hls", "lhls", )

# TODO: handle genre/category by individual rooms
# currently this checks the onlive list for each of Music, Idol, and Talent/Model
# schedules are still Idol only
GENRE_IDS = {101, 102, 103, 104, 105, 106, 107, 200}


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


class WatchManager(object):
    def __init__(self, index: ShowroomIndex, settings: ShowroomSettings):
        """
        
        """
        # maintains a list?
        # does it still need a priority queue?
        # various permutations of the base list
        self.index = index
        self.client = ShowroomClient()
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
        
        self._next_maintenance = None
        self.schedule_next_maintenance()

        # TODO: design better organised configuration/settings
        if self.settings.feedback.write_schedules_to_file:
            self._schedule_update_thread = threading.Thread(target=self.write_schedules, name="ScheduleWriter")
            self._schedule_update_thread.daemon = True
            self._schedule_update_thread.start()

        self.__schedule_warned = False
        self.__onlives_warned = False

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
        try:
            onlives = self.client.onlives() or []
        except HTTPError as e:
            if not self.__onlives_warned:
                if e.response.status_code >= 400:
                    core_logger.warning('Fetching onlives failed with error: {}'.format(e))
                else:
                    # I don't think any of these would actually raise?
                    core_logger.warning('Fetching onlives failed unexpectedly: {}'.format(e))
                self.__onlives_warned = True
            return
        self.__onlives_warned = False

        # temporary fix for getting multiple genres
        for livelist in onlives:
            if livelist['genre_id'] in GENRE_IDS:
                for item in [e for e in livelist['lives'] if 'room_id' in e and str(e['room_id']) in self.index]:
                    room_id = str(item['room_id'])
                    start_time = datetime.datetime.fromtimestamp(float(item['started_at']), tz=TOKYO_TZ)

                    # core_logger.debug('Checking live room id {}'.format(room_id))
                    if room_id in self.watchers:
                        if self.watchers[room_id].mode == "schedule":
                            self.watchers[room_id].reschedule(start_time)
                            self.watchers[room_id].set_watch_time(datetime.datetime.now(tz=TOKYO_TZ))
                            core_logger.debug('Early live for {} at {}'.format(self.watchers[room_id].name,
                                                                               self.watchers[
                                                                                   room_id].formatted_start_time))
                    else:
                        new = Watcher(self.index[room_id], self.client, self.settings,
                                      update_flag=self.update_flag, start_time=start_time)
                        new.set_watch_time(datetime.datetime.now(tz=TOKYO_TZ))
                        info = new.get_info()
                        core_logger.debug(
                            'Unscheduled live for {} starting at {}'.format(info['name'], info['start_time']))
                        self.add(new)

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

    def update_schedule(self):
        """Checks the schedule and adds watchers for any new rooms found."""
        # TODO: get multiple genres
        try:
            upcoming = self.client.upcoming(genre_id=102) or []
        except HTTPError as e:
            if not self.__schedule_warned:
                if e.response.status_code >= 500:
                    core_logger.warn('Fetching schedule failed temporarily: {}'.format(e))
                elif e.response.status_code >= 400:
                    core_logger.warn('Fetching schedule failed permanently: {}'.format(e))
                else:
                    # I don't think any of these would actually raise?
                    core_logger.warn('Fetching onlives failed unexpectedly: {}'.format(e))
                self.__schedule_warned = True
            return
        except AttributeError as e:
            core_logger.warn('Failed to fetch upcoming')
            return
        
        self.__schedule_warned = False

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
                new = Watcher(self.index[room_id], self.client, self.settings,
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
            # else:
            # is this check necessary?
            # is it too fast?
            # if thread.is_alive():
            # TODO: log undead threads
            # TODO: handle undead threads elsewhere
            # self._undead_threads.put(thread)

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
                                            ('streaming_urls', (item['download']['streaming_urls'] or []).copy()),
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

        # dirty hack: no timezone, so we get the "correct" date even after midnight JST
        datestr = datetime.datetime.now().strftime(FULL_DATE_FMT)[:10]
        outfile = self.settings.file.completed.replace('.json', '_{}.json'.format(datestr))
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

    def schedule_next_maintenance(self, minutes=None):
        if minutes:
            maint_time = self._next_maintenance + datetime.timedelta(minutes=minutes)

        if not minutes or maint_time.hour > 5:
            maint_time = (datetime.datetime.now(tz=TOKYO_TZ) + datetime.timedelta(days=1)).replace(hour=0, minute=5, second=0, microsecond=0)

        self._next_maintenance = maint_time

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

    def _maintenance_ready(self):
        curr_time = datetime.datetime.now(tz=TOKYO_TZ)
        if self._next_maintenance < curr_time:
            if len(list(self.watchers.get_by_mode("live"))) < 1:
                return True
            else:
                # core_logger.debug('Live watcher prevents maintenance, rescheduling')
                # TODO: print live watchers that are preventing maintenance
                self.schedule_next_maintenance(30)
        return False

    def do_maintenance(self):
        self.write_completed()
        self.schedule_next_maintenance()

    def tick(self):
        """Periodic live and schedule check"""

        if self._lives_ready():
            # core_logger.debug('Checking lives')

            # completed is checked before lives so that a completed room
            # doesn't prevent a new live from being added

            self.update_completed()
            self.update_lives()

        if self._schedule_ready():
            # core_logger.debug('Checking schedule')
            self.update_schedule()
            # core_logger.debug('{} active watchers'.format(len(self.watchers)))

        if self._maintenance_ready():
            self.do_maintenance()

    def stop(self):
        for watch in self.watchers:
            watch.stop()
        while self.watchers:
            self.update_completed()
            time.sleep(0.5)
        # TODO: handle zombie threads/watchers




