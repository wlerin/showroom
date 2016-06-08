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

import os
import time
import subprocess
from json.decoder import JSONDecodeError
import json
import datetime
import argparse
from heapq import heapify, heappush, heappop, heappushpop
import itertools


import pytz
from requests import Session


MAX_DOWNLOADS = 20
MAX_WATCHES = 10
MAX_PRIORITY = 40
DEFAULT_INDEX = 'index/default_members.json'

OUTDIR = 'output'

# The times and dates reported on the website are screwy, but when fetched through BeautifulSoup they *seem* to come in JST
# If you're getting incorrect times you probably need to mess with Schedule.convert_time()
# Or add custom headers to the requests.get() call in Scheduler.tick()
TOKYO_TZ = pytz.timezone('Asia/Tokyo')


WATCHSECONDS = [600, 420, 360, 360, 300, 300, 240, 240, 180, 150]

def watch_seconds(priority):
    if priority > len(WATCHSECONDS):
        return 120
    else:
        return WATCHSECONDS[priority-1]


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
        self.index = 0
        return self

    def __next__(self):
        if self.index >= len(self):
            raise StopIteration

        while self.queue[self.index][2] == None:
            self.index += 1
            if self.index >= len(self):
                raise StopIteration

        val = self.queue[self.index][2]
        self.index += 1
        return val
    
    def __getitem__(self, index):
        return self.queue[index]
    #def __setitem__(self):

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
            if self.queue[-1][2] == None:
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
    def __init__(self, member, session, outdir):
        self.session = session
        self._member = member
        self.process = None
        self.failures = 0
        self.rootdir = outdir # set by WatchManager
        self.destdir, self.tempdir, self.outfile = "", "", ""

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
        if self.process.returncode == None:
            #_, err = self.process.communicate()
            #if b'already exists. Overwrite' in err:
            #    self.process.communicate(b'y\n')
            return False
        else:
            if self.outfile:
                self.move_to_dest()
            if self.is_live():
                time.sleep(1) # give the stream some time to restart
                self.start()
                return False
            return True # how to respond to failed exits?
    
    
    
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
        destpath ='{}/{}'.format(self.destdir, self.outfile)
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
                print('Download for {} failed'.format(self.name))
                pass
            else:
                print('Completed {}/{}'.format(self.destdir, self.outfile))
            self.destdir, self.tempdir, self.outfile = ("", "", "")
            
        
        
    def start(self):
        data = self.session.get('https://www.showroom-live.com/room/get_live_data', params={'room_id': self.room_id}).json()
        stream_name = data['streaming_name_rtmp']
        stream_url  = data["streaming_url_rtmp"]
        tokyo_time = datetime.datetime.now(tz=TOKYO_TZ)
        
        self.tempdir, self.destdir, self.outfile = format_name(self.rootdir, tokyo_time.strftime('%Y-%m-%d %H%M%S'), self.member)
        
        self.sent_quit = False
        self.process = subprocess.Popen([
                'ffmpeg', 
                '-loglevel', '16', 
                '-i', '{}/{}'.format(stream_url, stream_name), 
                '-c', 'copy', 
                '{}/{}'.format(self.tempdir, self.outfile)
            ],
            stdin=subprocess.PIPE)
    

def format_name(rootdir, time_str, member):
    dir_format  ='{root}/{date}/{team}'
    tempdir     = '{root}/active'.format(root=rootdir)
    name_format ='{date} Showroom - {team} {name} {time}{count}.mp4'
    count       = 0 
    count_str   = '_{:02d}'
    destdir  = dir_format.format(root=rootdir, date=time_str[:10], team=member['engTeam'][:5])
    
    os.makedirs(destdir, exist_ok=True)
    
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
        self.session  = Session()
        self.start_time = start_time

    def check(self):
        while True:
            if self.start_time and (datetime.datetime.now(tz=TOKYO_TZ) > 
                                    self.start_time + datetime.timedelta(seconds=watch_seconds(self.priority)*2.0)):
                raise TimeoutError
            try:
                status = self.session.get('https://www.showroom-live.com/room/is_live', params={"room_id": self.room_id}).json()['ok']
            except JSONDecodeError:
                continue

            if status == 0:
                return False
            elif status == 1:
                return True

    def download(self, outdir):
        return Downloader(self._member, self.session, outdir)

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
        
        if web_url:
            self.member = self.find_member_by_url(web_url, index)
        elif room_id:
            self.member = self.find_member_by_room(room_id, index)
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

    def find_member_by_url(self, web_url, index):
        for e in index:
            if e['web_url'].endswith(web_url):
                return e
        return None

    def find_member_by_room(self, room_id, index):
        for e in index:
            if e['showroom_id'] == str(room_id):
                return e
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
        if (new_time - self._time).total_seconds() >= 5.0:
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
            print('Downloading {}\'s Showroom'.format(new_dl.name))
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
                            self.downloads.add(watch.download(self.outdir))
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
        self._rooms         = frozenset([e['showroom_id'] for e in self.index])
        
        self.session        = Session()
        self.upcoming       = {}
        self.live           = {}
        self._scheduled     = {}
        self._watchmanager  = WatchManager(settings=self.settings, scheduled=self._scheduled, live=self.live)
        
        self._time          = datetime.datetime.now(tz=TOKYO_TZ)
        self._tick_count    = 0
        self.firstrun       = True
        

    def tick(self, new_time):
        # tick rate per 33 seconds
        if (new_time - self._time).total_seconds() >= 13.0 or self.firstrun:
            self.firstrun = False
            self._time = new_time
            
            if self._tick_count % 10 == 0:
                self.update_schedule()

            if self._tick_count % 50 == 0:
                print('Current Time is {}'.format(self._time.strftime('%H:%M')))
                
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
        
        for item in [e for e in lives if 'room_id' in e and str(e['room_id']) in self.rooms]:
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
        for item in [e for e in upcoming if str(e['room_id']) in self.rooms]:
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

    @property
    def rooms(self):
        return self._rooms


class Controller(object):
    def __init__(self, index, outdir=OUTDIR, 
                 max_downloads=MAX_DOWNLOADS, max_priority=MAX_PRIORITY, max_watches=MAX_WATCHES):
        self.session = Session()
        self.index   = index
        self.settings = {'outdir':        outdir,
                         'max_downloads': max_downloads,
                         'max_watches':   max_watches,
                         'max_priority':  max_priority}

    def run(self):
        self.scheduler = Scheduler(index=self.index, settings=self.settings)
        self.watchers  = self.scheduler.watchmanager
        self.downloaders = self.watchers.downloads
        self.downloaders

        while True:
            self.time = datetime.datetime.now(tz=TOKYO_TZ)

            self.scheduler.tick(self.time) # Scheduler object
            self.watchers.tick(self.time) # WatchManager object
            self.downloaders.tick(self.time) #DownloadManager object
            
            # TODO: allow soft exit i.e. on user input, rather than ctrl+c


def watch(member, outdir):
    s = Session()

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
            subprocess.call(['ffmpeg', '-i', '{}/{}'.format(stream_url, stream_name), 
                '-user-agent', 'User-Agent: Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36', 
                '-headers', 'Referer: {}'.format(member['web_url']),
                '-c', 'copy', '{}/{}.mp4'.format(outdir, member_name.lower())])
            break

def find_member(target, index):
    for e in index:
        if target.lower() == e['engName'].lower():
            return e

    return None


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
    parser = argparse.ArgumentParser(description="Watches Showroom for live videos and downloads them when they become available. Most options only apply in --all mode", 
                                     epilog="The max-* options, parser, index, and output-dir haven't been fully tested yet")
    parser.add_argument('names', nargs='*',
                        help='A single quoted Member Name to watch. Technically accepts a list of names but only the first matched name will be used. For now. \n\nCompletely ignored if --all is given.')
    parser.add_argument('--all', '-a', action='store_true',
                        help='Watch the main showroom page for live shows and record all of them. Noisy and probably still buggy.')
    parser.add_argument('--output-dir', '-o', default=OUTDIR,
                        help='Directory in which to store active and completed downloads. Defaults to "%(default)s"')
    parser.add_argument('--index', '-i', default=DEFAULT_INDEX, 
                        help='Path to an index file, e.g. members.json or ske48_only.json. All members must be included, this is just used to give them different priorities. Defaults to %(default)s')
    parser.add_argument('--max-downloads', '-D', default=MAX_DOWNLOADS, type=int,
                        help='Maximum number of concurrent downloads. Defaults to %(default)s')
    parser.add_argument('--max-watches', '-W', default=MAX_WATCHES, type=int,
                        help='Maximum number of rooms to watch at once (waiting for them to go live). Defaults to %(default)s')
    parser.add_argument('--max-priority', '-P', default=MAX_PRIORITY, type=int,
                        help='Any members with priority over this value will be ignored. Defaults to %(default)s')
    args = parser.parse_args()
    
    # will raise an exception if not found but that's probably best
    with open(args.index, encoding='utf8') as infp:
        member_index = json.load(infp)
    
    if args.all == True:
        os.makedirs(args.output_dir + '/active', exist_ok=True)
        c = Controller(index=member_index, 
                       outdir=args.output_dir, 
                       max_downloads=args.max_downloads,
                       max_priority=args.max_priority,
                       max_watches=args.max_watches)
        c.run()
    elif len(args.names) > 0:
        # silently discards all but the first matched member
        names = args.names[:args.max_downloads]
        members = [find_member(name, member_index) for name in names]
        if members[0]:
            os.makedirs(args.output_dir)
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