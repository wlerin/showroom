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

Find the next live for a room
https://www.showroom-live.com/api/room/next_live?room_id=61576
"""
from sys import stdout
import os
import glob
import time
import subprocess
from json.decoder import JSONDecodeError
import json
import datetime
import argparse
from heapq import heapify, heappush, heappop, heappushpop
import itertools
from operator import attrgetter

try:
    from announce import Announcer
except ImportError:
    class DefaultAnnouncer(object):
        def __init__(self, separator='\n', outfp=stdout):
            self.sep = separator
            self.outfp = outfp
        
        def send_message(self, msg):
            """
            Receives and prints either a string or a list of strings. If a list of strings,
            the separator specified when creating the class will be used (default newline)
            """
            print(*msg, sep=self.sep, file=self.outfp)
    
    Announcer = DefaultAnnouncer

import pytz
from requests import Session
from requests.exceptions import ConnectionError, ChunkedEncodingError


MAX_DOWNLOADS = 60
MAX_WATCHES = 30
MAX_PRIORITY = 40
LIVE_RATE = 11.0
SCHEDULE_TICKS = 20
END_HOUR = 0
RESUME_HOUR = 5
DEFAULT_INDEX = 'index/default_members.json'
NEW_INDEX_LOC = 'index'

OUTDIR = 'output'

# The times and dates reported on the website are screwy, but when fetched through BeautifulSoup
# they *seem* to come in JST
# If you're getting incorrect times you probably need to mess with Schedule.convert_time()
# Or add custom headers to the requests.get() call in Scheduler.tick()
TOKYO_TZ = pytz.timezone('Asia/Tokyo')

WATCHSECONDS = [600, 420, 360, 360, 300, 300, 240, 240, 180, 150]


def watch_seconds(priority):
    if priority > len(WATCHSECONDS):
        return 120
    else:
        return WATCHSECONDS[priority-1]


class Showroom(object):
    def __init__(self, room_info=None, mod_time=0):
        """
        :param room_info: Dictionary describing the room, from an index file
        :param mod_time: Time the source file was last modified
        """
        self._mod_time = mod_time
        self._room_info = room_info

    def __getitem__(self, key):
        return self._room_info[key]

    # def __setitem(self, key, value) # only the indexer should make changes here, and only to priority

    # I don't want to accidentally set this, so an explicit set is used instead of a setter
    def set_priority(self, new_priority, mod_time):
        self._mod_time = mod_time
        self._room_info['priority'] = new_priority

    def __bool__(self):
        return bool(self._room_info)

    @property
    def mod_time(self):
        return self._mod_time

    @property
    def short_url(self):
        return self._room_info['web_url'].split('/')[-1]

    @property
    def room_id(self):
        return self._room_info['showroom_id']

    @property
    def priority(self):
        return self._room_info['priority']


class Indexer(object):
    # TODO: dictionary with room_id as key, dictionary with room_url as key...
    # how to make sure they stay in sync?
    def __init__(self):
        self.room_dict = {}
        self.room_url_lookup = {}

    def update(self):
        # checks if index files (*.jdex) have been updated
        # and if so opens them to see if any new rooms have been added
        # also check priorities? or other existing room data?
        pass

    def rebuild(self):
        # rebuilds index from scratch
        # only way to remove deleted rooms (or alter priorities?)
        # if i just overwrite the old room_dict and lookup, any active rooms will stay active until their
        # schedule/watch/download finishes
        # this may be desirable
        # however, it's definitely possible to change priorities on active rooms...
        pass

    def find_room(self, room_id=None, room_url=None):
        if room_id:
            try:
                return self.room_dict[room_id]
            except KeyError:
                print("Failed to find ID {}".format(room_id))
                return None
        elif room_url:
            # Primary hangup here is that the "url" could be a number of things...
            # but let us limit it to the end of the url, after the final /
            try:
                return self.room_url_lookup[room_url]
            except KeyError:
                print("Failed to find Room URL {}".format(room_url))
                return None

    def __contains__(self, room_id):
        if room_id in self.room_dict:
            return True


class IndexerOld(Indexer):
    def __init__(self):
        super(Indexer, self).__init__()


class IndexerNew(Indexer):
    def __init__(self, index_directory):
        super(IndexerNew, self).__init__()
        # read index_directory
        # make note of modification times and file sizes for all *.jdex files
        # load data from all jdex files, creating Showroom objects for each unique room
        # updating rooms as necessary (include mod date w/ Showroom object? include source jdex?)
        # Including the source_jdex is superfluous since a room can be in multiple files,
        # and should only be removed if it's removed from all files (and I don't want that to be a regular event)
        # mod_date is semi-useful when building the initial index
        self.directory = index_directory
        self.known_files = {}

        # get list of *.jdex files in index_directory
        found_files = glob.glob("{}/{}".format(self.directory, "*.jdex"))
        # need to keep a record of each file + mod_date and file_size

        for e in found_files:
            statinfo = os.stat(e)
            self.known_files[e] = {"mod_time": statinfo.st_mtime, "file_size": statinfo.st_size}

        found_files = sorted(self.known_files, key=lambda x: self.known_files[x]['mod_time'])

        for jdex in found_files:
            mod_time = self.known_files[jdex]['mod_time']
            # open the jdex
            try:
                with open(jdex, encoding='utf8') as infp:
                    temp_data = json.load(infp)
            except JSONDecodeError:
                print('{} could not be read'.format(jdex))
                continue
            # add each room to the room_dict and the room_url_lookup
            # perhaps in this phase it is not necessary to update existing rooms but simply overwrite
            # but later we will need to update
            for room in temp_data:
                new_room = Showroom(room, mod_time)
                self.room_dict[new_room.room_id] = new_room
                self.room_url_lookup[new_room.short_url] = new_room

    def update(self):
        # TODO: too much code duplication, consolidate to single method?
        found_files = glob.glob("{}/{}".format(self.directory, "*.jdex"))

        changed_files = []

        for e in found_files:
            statinfo = os.stat(e)
            if e in self.known_files:
                # known file w/ changes
                if (statinfo.st_mtime > self.known_files[e]['mod_time'] or
                        statinfo.st_size != self.known_files[e]['file_size']):
                    changed_files = e
                    self.known_files[e] = {'mod_time': statinfo.st_mtime, 'file_size': statinfo.st_size}
            else:
                # new file
                changed_files = e
                self.known_files[e] = {'mod_time': statinfo.st_mtime, 'file_size': statinfo.st_size}

        if len(changed_files) > 0:
            print("Updating index")
        else:
            return

        changed_files = [e for e in sorted(self.known_files, key=lambda x: self.known_files[x]['mod_time']) if e in changed_files]

        for jdex in changed_files:
            # is it faster to assume new priorities or to check if they have changed?
            mod_time = self.known_files[jdex]['mod_time']
            try:
                with open(jdex, encoding='utf8') as infp:
                    temp_data = json.load(infp)
            except JSONDecodeError:
                print('{} could not be read'.format(jdex))
                continue
            # check if room exists, if it does, check priority
            # if different, update priority and mod_time
            # if room does not exist, add
            for room in temp_data:
                room_id = room['showroom_id']
                if room_id in self.room_dict:
                    # is this check necessary?
                    if room['priority'] != self.room_dict[room_id]['priority']:
                        self.room_dict[room_id].set_priority(room['priority'], mod_time)
                else:
                    new_room = Showroom(room, mod_time)
                    self.room_dict[new_room.room_id] = new_room
                    self.room_url_lookup[new_room.short_url] = new_room

    def rebuild(self):
        pass


class WatchSession(Session):
    def __init__(self, *args, **kwargs):
        # TODO: 
        super(WatchSession, self).__init__(*args, **kwargs)
    
    def get(self, url, params=None, **kwargs):
        while True:
            try:
                r = super().get(url, params=params, **kwargs)
            except (ConnectionError, ChunkedEncodingError):
                # TODO: Back off gradually if errors keep happening
                time.sleep(1.0)
            else:
                return r


class WatchQueue(object):
    def __init__(self):
        self.queue = []
        self.entry_map = {}
        self.REMOVED   = None
        self.counter = itertools.count()
        self.dirty = False

    def __len__(self):
        return len(self.entry_map)

    def __iter__(self):
        # TODO: pick a name other than index to avoid confusion
        self.index = 0
        return self

    def __next__(self):
        if self.index >= len(self):
            raise StopIteration

        while self.queue[self.index][2] is None:
            self.index += 1
            if self.index >= len(self):
                raise StopIteration

        val = self.queue[self.index][2]
        self.index += 1
        return val
    
    def __getitem__(self, index):
        return self.queue[index]
    # def __setitem__(self):

    def keys(self):
        return self.entry_map.keys()
    
    def add(self, item):
        if item.room_id in self.entry_map:
            return False# do nothing
        else:
            count = next(self.counter)
            entry = [item.priority, count, item]
            self.entry_map[item.room_id] = entry
            heappush(self.queue, entry)
            return True

    def remove(self, item):
        entry = self.entry_map.pop(item.room_id)
        entry[-1] = self.REMOVED
        self.dirty = True

    def pop(self):
        while self.queue:
            priority, count, item = heappop(self.queue)
            if item is not self.REMOVED:
                del self.entry_map[item.room_id]
                return item

    def dirty_pop(self, item):
        entry = self.entry_map.pop(item.room_id)
        if entry:
            result = entry[2]
            entry[-1] = self.REMOVED
            self.dirty = True
            return result
        else:
            return None
        
    def pop_end(self):
        while self.queue:
            priority, count, item = self.queue.pop(-1)
            if item is not self.REMOVED:
                del self.entry_map[item.room_id]
                self.dirty = True
                return item
                
    def prune(self, priority):
        # What are the consequences of doing this ?
        while self.queue:
            if self.queue[-1][2] is None:
                self.queue.pop(-1)
            elif self.queue[-1][2].priority > priority:
                self.remove(self.queue[-1][2])
                return True
            else:
                return False

    def replace(self, item):
        if item.room_id in self.entry_map:
            return heappop(self.queue)[2]
        else:
            count = next(self.counter)
            entry = [item.priority, count, item]
            self.entry_map[item.room_id] = entry
            return heappushpop(self.queue, entry)[2]

    def peek(self):
        if len(self) > 0:
            result = self.queue[0][2]
            if not result:
                self.rebuild()
                result = self.queue[0][2]
            return result
        else:
            return None

    def rebuild(self):
        if self.dirty:
            self.queue = [e for e in self.queue if e[2] != self.REMOVED]
            heapify(self.queue)


class DownloadQueue(WatchQueue):
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
    
    def prune(self, priority):
        if self.queue[-1][2].priority >= priority*2:
            print('Killing {} to make room'.format(self.queue[-1][2].name))
            self.remove(self.queue[-1][2], kill=True)
            return True
        else:
            return False


class Downloader(object):
    def __init__(self, member, session, outdir, logging):
        self.session = session
        self._member = member
        self.process = None
        # self.failures = 0
        self.rootdir = outdir # set by WatchManager
        self.destdir, self.tempdir, self.outfile = "", "", ""
        self._url = ""
        self._logging = logging
        self._announcer = Announcer()
        self.sent_quit = False

    @property
    def name(self):
        return self._member['engName']

    @property
    def room_id(self):
        return self._member['showroom_id']

    @property
    def priority(self):
        return self._member['priority']

    @property
    def member(self):
        return self._member
    
    @property
    def logging(self):
        return self._logging
    
    @property
    def web_url(self):
        return self._member['web_url']

    def announce(self, msg):
        self._announcer.send_message(msg)
        
    def is_live(self):
        while True:
            try:
                status = self.session.get('https://www.showroom-live.com/room/is_live', params={"room_id": self.room_id}).json()['ok']
            except JSONDecodeError:
                continue

            if status == 0:
                return False
            elif status == 1:
                return True
    
    def check(self):
        self.process.poll()
        if self.process.returncode is None:
            # _, err = self.process.communicate()
            # if b'already exists. Overwrite' in err:
            #    self.process.communicate(b'y\n')
            return False
        else:
            if self.outfile:
                self.move_to_dest()
            if self.is_live():
                time.sleep(2)  # give the stream some time to restart
                self.start()
                return False
            return True  # how to respond to failed exits?

    def kill(self):
        if not self.sent_quit:
            print('Quitting {}'.format(self.name))
            self.process.terminate()
            self.sent_quit = True
        else:
            self.process.kill()
            self.sent_quit = False

    def move_to_dest(self):
        srcpath = '{}/{}'.format(self.tempdir, self.outfile)
        destpath = '{}/{}'.format(self.destdir, self.outfile)
        if os.path.exists(destpath):
            # how? why? this should never happen
            raise FileExistsError
        else:
            try:
                os.replace(srcpath, destpath)
            except FileNotFoundError:
                # probably means srcpath not found, which means the download errored out
                # before creating a file. right now, do nothing
                # Most likely what's happening is the script is trying to access the stream
                # while it's down (but the site still reports it as live)
                # print('Download for {} failed'.format(self.name))
                pass
            else:
                print('Completed {}/{}'.format(self.destdir, self.outfile))
            self.destdir, self.tempdir, self.outfile = ("", "", "")
        
    def start(self):
        data = self.session.get('https://www.showroom-live.com/room/get_live_data', params={'room_id': self.room_id}).json()
        stream_name = data['streaming_name_rtmp']
        stream_url = data["streaming_url_rtmp"]
        tokyo_time = datetime.datetime.now(tz=TOKYO_TZ)
        new_url = '{}/{}'.format(stream_url, stream_name)
        self.tempdir, self.destdir, self.outfile = format_name(self.rootdir, tokyo_time.strftime('%Y-%m-%d %H%M%S'), self.member)

        self.sent_quit = False
        
        if new_url != self.url:
            self._url = new_url 
            print('Downloading {}\'s Showroom'.format(self.name, self.url))
            self.announce((self.web_url, self.url))
        
        if self.logging is True:
            log_file = os.path.normpath('{}/logs/{}.log'.format(self.destdir, self.outfile))
            ENV = {'FFREPORT': 'file={}:level=40'.format(log_file)}
        else:
            ENV = None
        
        normed_outpath = os.path.normpath('{}/{}'.format(self.tempdir, self.outfile))
        self.process = subprocess.Popen([
                'ffmpeg',
                '-loglevel', '16',
                '-copytb', '1',
                '-i', self.url,
                '-c', 'copy',
                normed_outpath
            ],
            stdin=subprocess.PIPE,
            env=ENV)
    
    @property
    def url(self):
        return self._url
    

def format_name(rootdir, time_str, member):
    dir_format  = '{root}/{date}/{team}'
    tempdir     = '{root}/active'.format(root=rootdir)
    name_format = '{date} Showroom - {team} {name} {time}{count}.mp4'
    count       = 0 
    count_str   = '_{:02d}'

    # TODO: Evaluate this in light of all the new rooms
    if '48' in member['engTeam'] and 'Gen' not in member['engTeam']:
        team = member['engTeam'][:5]
    else:
        team = member['engTeam']  # just Nogizaka46 right now
    
    destdir  = dir_format.format(root=rootdir, date=time_str[:10], team=team)
    
    os.makedirs('{}/logs'.format(destdir), exist_ok=True)
    
    _date, _time = time_str.split(' ')
    short_date = _date[2:].replace('-', '')
    
    outfile = name_format.format(date=short_date, team=member['engTeam'], name=member['engName'], 
                                 time=_time, count='')
    while os.path.exists('{}/{}'.format(destdir, outfile)):
        count +=1
        outfile = name_format.format(date=short_date, team=member['engTeam'], name=member['engName'], 
                                     time=_time, count=count_str.format(count))
    return tempdir, destdir, outfile


class Watcher(object):
    def __init__(self, member, start_time = None):
        self._member = member
        self.session  = WatchSession()
        self.start_time = start_time

    def check(self):
        while True:
            if self.start_time and (datetime.datetime.now(tz=TOKYO_TZ) > 
                                    self.start_time + datetime.timedelta(seconds=watch_seconds(self.priority)*2.0)):
                raise TimeoutError
            try:
                status = self.session.get('https://www.showroom-live.com/room/is_live',
                                          params={"room_id": self.room_id}).json()['ok']
            except JSONDecodeError:
                continue

            if status == 0:
                return False
            elif status == 1:
                return True

    def download(self, outdir, logging):
        return Downloader(self._member, self.session, outdir, logging)

    @property
    def name(self):
        return self._member['engName']

    @property
    def room_id(self):
        return self._member['showroom_id']

    @property
    def priority(self):
        return self._member['priority']

    @property
    def member(self):
        return self._member


class Schedule(object):
    def __init__(self, start_time, index, web_url=None, room_id=None, is_live=False, dt=None):
        self.start_time = start_time

        # it's really quite silly to do this here
        # also this really should be self.room
        if web_url:
            self.member = self.find_room_by_url(web_url, index)
        elif room_id:
            self.member = self.find_room_by_id(room_id, index)
        else:
            self.member = None

        self._live = is_live

    def __bool__(self):
        return bool(self.member)
    
    # TODO: use this again, but accept timestamps instead
    '''
    def convert_time(self, time_str, dt=None):
        # This seems to differ from platform to platform, need a way to 
        # for the site to report a specific date format
        if '/' in time_str:
            month, time_str = time_str.split('/')
            day, time_str, ampm = time_str.split(' ')
        else:
            month, day = dt.month, dt.day
            time_str, ampm = time_str.split(' ')
            ampm = ampm.strip('〜')
        
        hour, minute = time_str.split(':', 1)
        time_str = '2016-{:02d}-{:02d} {:02d}:{:02d} {}'.format(int(month), int(day), int(hour), int(minute), ampm)
        pattern = '%Y-%m-%d %I:%M %p'
        # return datetime.datetime.strptime(time_str, pattern).replace(tzinfo=LA_TZ).astimezone(tz=TOKYO_TZ)
        return datetime.datetime.strptime(time_str, pattern).replace(tzinfo=TOKYO_TZ)
    '''

    def find_room_by_url(self, web_url, index):
        try:
            return index.find_room(web_url=web_url.split('/')[-1])
        except KeyError:
            return None

    def find_room_by_id(self, room_id, index):
        try:
            return index.find_room(room_id=room_id)
        except KeyError:
            return None

    def check(self, curr_time):
        if (self.start_time - curr_time).total_seconds() < watch_seconds(self.priority):
            return True
        else:
            return False

    def is_live(self):
        return self._live

    def go_live(self):
        self._live = True


    @property
    def priority(self):
        return self.member['priority']

    @property
    def name(self):
        return self.member['engName']

    @property
    def room_id(self):
        return self.member['showroom_id']
    
    @property
    def formatted_time(self):
        return self.start_time.strftime('%H:%M')

    
class DownloadManager(object):
    def __init__(self, scheduled):
        # self._max_dls   = 
        self._downloads = DownloadQueue()
        # self._live      = live # from Scheduler
        self._time      = datetime.datetime.now(tz=TOKYO_TZ)
        self._scheduled = scheduled


    def __len__(self):
        return len(self._downloads)
    

    def tick(self, new_time):
        # tick rate per 10 sec
        # check if download has stopped
        # recheck to see if was just interrupted
        if (new_time - self._time).total_seconds() >= 4.0:
            # print('Running DownloadManager')
            self._time = new_time
        
            #check if download has stopped
            for download in self.downloads:
                if download.check():
                    # print('{}\'s download stopped'.format(download.name))
                    # if it's still live it'll be readded to the schedule
                    if download.room_id in self.scheduled:
                        self.scheduled.pop(download.room_id)
                    self.downloads.remove(download)
            
            self.rebuild()

    def add(self, new_dl):
        if self.downloads.add(new_dl):
            new_dl.start()

    def prune(self, member):
        return self.downloads.prune(member['priority'])
    
    @property
    def downloads(self):
        return self._downloads

    @property
    def scheduled(self):
        return self._scheduled

    def rebuild(self):
        self.downloads.rebuild()


class WatchManager(object):
    def __init__(self, scheduled, live, settings):
        # maintains both a list and a priority queue
        # the list has a maximum size
        self.watches     = WatchQueue()
        self._inqueue    = WatchQueue()
        self._time       = datetime.datetime.now(tz=TOKYO_TZ)
        self._scheduled  = scheduled # from Scheduler, tracks watches added to _inqueue
        self._live       = live # from Scheduler, tracks live streams
        self.downloads   = DownloadManager(scheduled=scheduled)
        self.settings    = settings
        
        # self.max_watches = MAX_WATCHES
        # self.max_dls     = MAX_DOWNLOADS
        # self.outdir     = 'output'

    def __len__(self):
        return len(self.watches)

    def keys(self):
        return self.watches.keys()
    
    def tick(self, new_time):
        # tick rate per 2 seconds
        if (new_time - self._time).total_seconds() >= 2.0:
            self._time = new_time

            for watch in self.watches:
                # check if video is live
                try:
                    if watch.check():
                        # add to downloads
                        if len(self.downloads) < self.max_downloads or self.downloads.prune(watch.member):
                            watch = self.watches.dirty_pop(watch)
                            self.downloads.add(watch.download(self.outdir, self.settings['logging']))
                except TimeoutError:
                    print('{}\'s Watch expired'.format(watch.name))
                    if watch.room_id in self._scheduled:
                        self._scheduled.pop(watch.room_id)
                    self.watches.remove(watch)
            
            while (self.has_queued() 
                   and (len(self.watches) < self.max_watches 
                   or self.watches.prune(self.next_priority()))):
                new_watch = self.pop_next_in_queue()
                print('Watching {}\'s Showroom'.format(new_watch.name))
                self.watches.add(new_watch)
            
            self.rebuild()

    def next_priority(self):
        if len(self._inqueue) > 0:
            return self._inqueue.peek().priority
        
    def pop_next_in_queue(self):
        return self._inqueue.pop()
    
    def has_queued(self):
        if len(self._inqueue) > 0:
            return True
        else:
            return False

    def add(self, item):
        self._inqueue.add(item)

    def rebuild(self):
        self.watches.rebuild()
        self._inqueue.rebuild()

    @property
    def outdir(self):
        return self.settings['outdir']
    
    @property
    def max_watches(self):
        return self.settings['max_watches']
    
    @property
    def max_downloads(self):
        return self.settings['max_downloads']


class Scheduler(object):
    def __init__(self, index, settings):
        self.settings       = settings
        self._index         = index
        # self._rooms         = frozenset([e['showroom_id'] for e in self.index])
        
        self.session        = WatchSession()
        self.upcoming       = {}
        self.live           = {}
        self._scheduled     = {}
        self._watchmanager  = WatchManager(settings=self.settings, scheduled=self._scheduled, live=self.live)
        
        self._time = datetime.datetime.now(tz=TOKYO_TZ)
        self._tick_count    = 0
        self.firstrun       = True

    def tick(self, new_time):
        # tick rate per 33 seconds
        time_diff = (new_time - self._time).total_seconds()
        # print(time_diff)
        if time_diff >= self.settings['live_rate'] or self.firstrun:
            # print('Checking lives')
            self._time = new_time
            self.firstrun = False

            if self._tick_count % self.settings['schedule_ticks'] == 0 or self.firstrun:
                # print('Checking schedules')
                self.update_schedule()

            if self._tick_count % 81 == 0:
                print('Current Time is {}'.format(self._time.strftime('%H:%M')))
                self._index.update()
                
            self._tick_count += 1

            self.update_live()

            for key in self.upcoming.copy():
                if self.upcoming[key].check(self._time):
                    temp = self.upcoming.pop(key)
                    self.watchmanager.add(Watcher(temp.member, temp.start_time))
                    self.scheduled.update({temp.room_id: temp})

            # check live broadcasts
            for schedule in [self.live[x] for x in self.live if x not in self.scheduled.keys()]:
                self.watchmanager.add(Watcher(schedule.member))
                self.scheduled.update({schedule.room_id: schedule})

    def update_live(self):
        self.live.clear()
        onlives = self.session.get('https://www.showroom-live.com/api/live/onlives').json()['onlives']

        # find the idol genre
        for e in onlives:
            if int(e['genre_id']) == 102:
                lives = e['lives']
                break
        
        for item in [e for e in lives if 'room_id' in e and str(e['room_id']) in self.index]:
            room_id = str(item['room_id'])
            start_time = datetime.datetime.fromtimestamp(float(item['started_at']), tz=TOKYO_TZ)
            
            # it's possible to get the stream_url here, from streaming_url_list[]
            
            if room_id in self.upcoming:
                new = self.upcoming.pop(room_id)
                new.go_live()
            else:
                new = Schedule(start_time, self.index, room_id=room_id, is_live=True, dt = self._time)
            
            if new.priority <= self.settings['max_priority']:
                self.live.update({new.room_id: new})

    def update_schedule(self):
        upcoming = self.session.get('https://www.showroom-live.com/api/live/upcoming?genre_id=102').json()['upcomings']
        for item in [e for e in upcoming if str(e['room_id']) in self.index]:
            start_time = datetime.datetime.fromtimestamp(float(item['next_live_start_at']), tz=TOKYO_TZ)
            new = Schedule(start_time, self.index, room_id=str(item['room_id']))
            if new:
                self.add(new)

    def add(self, schedule):
        if (schedule.room_id not in self.upcoming.keys()
                and schedule.room_id not in self.scheduled.keys()
                and schedule.priority <= self.settings['max_priority']):
            print('Scheduling {} for {}'.format(schedule.name, schedule.formatted_time))
            self.upcoming.update({schedule.room_id: schedule})

    @property
    def index(self):
        return self._index
    
    @property
    def watchmanager(self):
        return self._watchmanager
    
    @property
    def scheduled(self):
        return self._scheduled

    # @property
    # def rooms(self):
    #     # TODO: remove this entirely, the index should be just as fast as a frozenset
    #     # except of course if we try using the old index... until IndexerOld is implemented
    #     # things will massively break
    #     return self._index


class Controller(object):
    def __init__(self, index=None, outdir=OUTDIR,
                 max_downloads=MAX_DOWNLOADS, max_priority=MAX_PRIORITY, max_watches=MAX_WATCHES,
                 live_rate=LIVE_RATE, schedule_ticks=SCHEDULE_TICKS, end_hour=END_HOUR, resume_hour=RESUME_HOUR,
                 new_index=True, index_loc=NEW_INDEX_LOC, logging=False):
        self.session = WatchSession()

        if new_index:
            self.index = IndexerNew(index_loc)
        else:
            # TODO: convert this to IndexerOld
            self.index = index
        self.settings = {'outdir':         outdir,
                         'max_downloads':  max_downloads,
                         'max_watches':    max_watches,
                         'max_priority':   max_priority,
                         'live_rate':      live_rate,
                         'schedule_ticks':  schedule_ticks,
                         'logging':        logging}
        
        self.end_time = datetime.time(hour=end_hour, minute=10, tzinfo=TOKYO_TZ)
        self.resume_time = datetime.time(hour=resume_hour-1, minute=50, tzinfo=TOKYO_TZ)

        self.live_rate = self.settings['live_rate']

        # defined in run()
        self.scheduler = None
        self.watchers = None
        self.downloaders = None
        self.time = None

    def run(self):
        # why are these defined here?
        self.scheduler = Scheduler(index=self.index, settings=self.settings)
        self.watchers  = self.scheduler.watchmanager
        self.downloaders = self.watchers.downloads
        # self.downloaders
        # sleep_minutes = 20
        
        while True:
            self.time = datetime.datetime.now(tz=TOKYO_TZ)
            
            if self.resume_time > self.time.time() > self.end_time:
                sleep_seconds = (datetime.datetime.combine(self.time, self.resume_time) - self.time).total_seconds() + 1.0
                print('Time is {}, sleeping for {} seconds, until {}'.format(self.time.strftime('%H:%M'), sleep_seconds, self.resume_time.strftime('%H:%M')))
                time.sleep(sleep_seconds)
                
            else:
                self.scheduler.tick(self.time)  # Scheduler object
                self.watchers.tick(self.time)  # WatchManager object
                self.downloaders.tick(self.time)  # DownloadManager object

                if len(self.watchers) == 0 and len(self.downloaders) == 0:
                    time.sleep(self.live_rate)
                else:
                    time.sleep(1.0)
            
            # TODO: allow soft exit i.e. on user input, rather than ctrl+c


def watch(member, outdir):
    s = WatchSession()

    params = {'room_id': member['showroom_id']}
    member_name = member['engName']
    print('Watching {}\'s Room'.format(member_name))
    count = 0
    while True:
        count+=1
        try:
            status = s.get('https://www.showroom-live.com/room/is_live', params=params).json()['ok']
        except JSONDecodeError:
            continue

        if count % 30 == 0:
            print('Still watching {}\'s Room'.format(member_name))
        if status == 0:
            time.sleep(2)
            continue
        elif status == 1:
            data = s.get('https://www.showroom-live.com/room/get_live_data', params=params).json()
            stream_name = data['streaming_name_rtmp']
            stream_url  = data["streaming_url_rtmp"]
            normed_path = os.path.normpath('{}/{}.mp4'.format(outdir, member_name.lower()))
            subprocess.call(['ffmpeg', '-i', '{}/{}'.format(stream_url, stream_name), 
                '-user-agent', 'User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 \
                (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36',
                '-headers', 'Referer: {}'.format(member['web_url']),
                '-c', 'copy', normed_path])
            break


def find_member(target, index):
    for e in index:
        if target.lower() == e['engName'].lower():
            return e

    return None

# def merge_fragments(fragment_list, ):


# TODO: implement this, probably using WatchQueue + concurrent.futures
def simple_watcher(members, max_downloads):
    # threadpool for watches, list of active Popen objects once the watches complete
    # with concurrent.futures.ThreadPoolExecutor() as executor:
    #    result = executor.map(function, iterable)
    while True:
        pass

if __name__ == "__main__":
    """
    Syntax:
    python3 showroom.py "Member Name"|--all
    """
    # TODO: add verbosity levels
    parser = argparse.ArgumentParser(description="Watches Showroom for live videos and downloads them \
                                     when they become available. Most options only apply in --all mode",
                                     epilog="The max-* options, parser, index, and output-dir haven't been \
                                     fully tested yet. A new indexing system is currently in use, but \
                                     no command-line arguments to control it yet exist.")
    parser.add_argument('names', nargs='*',
                        help='A single quoted Member Name to watch. Technically accepts a list of names \
                        but only the first matched name will be used. For now. \n\nCompletely ignored if \
                        --all is given.')
    parser.add_argument('--all',            '-a', action='store_true',
                        help='Watch the main showroom page for live shows and record all of them. \
                        Noisy and probably still buggy.')
    parser.add_argument('--output-dir',     '-o', default=OUTDIR,
                        help='Directory in which to store active and completed downloads. \
                        Defaults to "%(default)s"')
    parser.add_argument('--index',          '-i', default=DEFAULT_INDEX,
                        help='Path to an index file, e.g. members.json or ske48_only.json. \
                        All members must be included, this is just used to give them different priorities. \
                        Defaults to %(default)s')
    parser.add_argument('--max-downloads',  '-D', default=MAX_DOWNLOADS, type=int,
                        help='Maximum number of concurrent downloads. \
                        Defaults to %(default)s')
    parser.add_argument('--max-watches',    '-W', default=MAX_WATCHES, type=int,
                        help='Maximum number of rooms to watch at once (waiting for them to go live). \
                        Defaults to %(default)s')
    parser.add_argument('--max-priority',   '-P', default=MAX_PRIORITY, type=int,
                        help='Any members with priority over this value will be ignored. \
                        Defaults to %(default)s')
    parser.add_argument('--live-rate',      '-R', default=LIVE_RATE, type=float,
                        help='Seconds between each poll of ONLIVES. \
                        Defaults to %(default)s')
    parser.add_argument('--schedule-ticks', '-S', default=SCHEDULE_TICKS, type=float,
                        help='Live ticks between each check of the schedule. \
                        Defaults to %(default)s')
    # TODO: Allow the user to provide a schedule with different start and end hours per day.
    # Or else instead of stopping entirely, slow down polling during off hours. 
    parser.add_argument('--end_hour',             default=END_HOUR, type=int,
                        help='Hour to stop recording (will actually stop 10 minutes later). \
                        Defaults to %(default)s')
    parser.add_argument('--resume_hour',          default=RESUME_HOUR, type=int,
                        help='Hour to resume recording (will actually start 10 minutes earlier). \
                        Defaults to %(default)s')
    parser.add_argument('--logging', action='store_true', help="Turns on ffmpeg logging.")
    args = parser.parse_args()
    
    # will raise an exception if not found but that's probably best
    # with open(args.index, encoding='utf8') as infp:
    #     member_index = json.load(infp)
    
    if args.all is True:
        os.makedirs(args.output_dir + '/active', exist_ok=True)
        c = Controller(# index=member_index,
                       outdir=args.output_dir,
                       max_downloads=args.max_downloads,
                       max_priority=args.max_priority,
                       max_watches=args.max_watches,
                       live_rate=args.live_rate,
                       schedule_ticks=args.schedule_ticks,
                       end_hour=args.end_hour,
                       resume_hour=args.resume_hour,
                       logging=args.logging)
        c.run()
    elif len(args.names) > 0:
        # silently discards all but the first matched member
        names = args.names[:args.max_downloads]
        members = [find_member(name, member_index) for name in names]
        if members[0]:
            os.makedirs(args.output_dir, exist_ok=True)
            watch(members[0], args.output_dir)
        else:
            print("Member not found")
        
        """
        for name in names:
            member = find_member(name)
            if member:
                members.append(member)
            
        simple_watcher(members, args.max_downloads)
        """
    else:
        print('Please supply either a quoted name or --all')
