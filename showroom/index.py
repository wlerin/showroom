import os
import json
import glob
import requests
from threading import Thread, RLock
import logging
import datetime
import time
import re
from .constants import TOKYO_TZ

__all__ = ['ShowroomIndex']

index_logger = logging.getLogger('showroom.index')
_filename_re = re.compile(
    r'''
        (?:\d{6}\ Showroom\ -\ )?
        ([\w\ \-!\?’】！？、.]+?)
        (?:\ \d{4,6})
        (?:\.mp4)?$
    ''',
    re.VERBOSE
)

# maps numerical genre_ids to human readable names
# when fetching from the API via a script it seems to return English names
# genre_id: (english name, japanese api name, english api name)
full_genre_map = {
    0: ("Popular", "人気", "Popularity"),
    101: ("Music", "ミュージック", "Music"),
    102: ("Idol", "アイドル", "Idol"),
    103: ("Talent/Model", "タレント・モデル", "Talent Model"),
    104: ("Voice Actor/Anime", "声優・アニメ", "Voice Actors & Anime"),
    105: ("Comedy/Talk Show", "お笑い・トーク", "Comedians/Talk Show"),
    106: ("Sports", "スポーツ", "Sports"),
    200: ("Amateur", "アマチュア", "Non-Professionals"),
}
genre_map = {key: val[0] for key, val in full_genre_map.items()}


class Room(object):
    def __init__(self, room_info=None, mod_time=0, language='eng', wanted=True):
        """
        :param room_info: Dictionary describing the room, from an index file
        :param mod_time: Time the source file was last modified
        """
        self._mod_time = mod_time
        self._room_info = room_info
        self.set_language(language)
        self._wanted = wanted
        self._lock = RLock()

    def __getitem__(self, key):
        return self._room_info[key]

    def __bool__(self):
        return bool(self._room_info)

    def set_priority(self, new_priority, mod_time):
        with self._lock:
            self._mod_time = mod_time
            self._room_info['priority'] = new_priority

    @property
    def mod_time(self):
        return self._mod_time

    @property
    def short_url(self):
        return self._room_info['web_url'].split('/')[-1]

    @property
    def long_url(self):
        if self._room_info['web_url'].startswith('https://'):
            return self._room_info['web_url']
        else:
            return 'https://www.showroom-live.com/' + self._room_info['web_url'].strip('/')

    @property
    def room_id(self):
        return self._room_info['showroom_id']

    @property
    def priority(self):
        return self._room_info['priority']

    @property
    def name(self):
        return self._room_info[self._language + 'Name']

    @property
    def group(self):
        return self._room_info[self._language + 'Group']

    @property
    def team(self):
        return self._room_info[self._language + 'Team']

    @property
    def handle(self):
        return ' '.join((x for x in (self.group, self.team, self.name) if x))

    def is_wanted(self):
        return self._wanted

    def set_wanted(self, truth_value):
        with self._lock:
            if truth_value:
                self._wanted = True
            else:
                self._wanted = False

    def set_language(self, new_language):
        if new_language.lower() in ('eng', 'jpn'):
            self._language = new_language.lower()
        elif new_language.lower() in ('english', 'en'):
            self._language = 'eng'
        elif new_language.lower() in ('japanese', 'jp'):
            self._language = 'jpn'
        else:
            index_logger.debug('Unknown language: {}'.format(new_language))

    def get_language(self):
        if self._language == 'eng':
            return 'English'
        elif self._language == 'jpn':
            return 'Japanese'

    def get_info(self):
        with self._lock:
            return {"name": self.name,
                    "group": self.group,
                    "team": self.team,
                    "room_id": self.room_id,
                    "priority": self.priority,
                    "web_url": self.long_url,
                    "wanted": self.is_wanted()}


class RoomOld(Room):
    def __init__(self, room_info=None, mod_time=0, language='eng', wanted=True):
        super(RoomOld, self).__init__(room_info=room_info, mod_time=mod_time,
                                      language=language, wanted=wanted)

    @property
    def group(self):
        # emulates old guessing method
        team = self._room_info[self._language + 'Team']
        if '48' in team and len(team) > 5 and 'Gen' not in team:
            return team[:5]
        else:
            return team

    @property
    def team(self):
        # emulates old guessing method
        team = self._room_info[self._language + 'Team']
        if '48' in team and len(team) > 5 and 'Gen' not in team:
            return team[5:].strip()
        else:
            return ''


# TODO: periodically check filter
class ShowroomIndex(object):
    def __init__(self, index_directory: str,
                 session: requests.Session = None,
                 record_all: bool = False,
                 language: str = 'eng'):
        self.room_dict = {}
        self._room_url_lookup = None
        self._room_name_lookup = None
        self._room_handle_lookup = None

        if session:
            self.session = session
        else:
            self.session = requests.Session()

        # TODO: test validity
        self.language = language
        # read index_directory
        # make note of modification times and file sizes for all *.jdex files
        # load data from all jdex files, creating Room objects for each unique room
        # updating rooms as necessary (include mod date w/ Room object? include source jdex?)
        # Including the source_jdex is superfluous since a room can be in multiple files,
        # and should only be removed if it's removed from all files
        # (and I don't want that to be a regular event)
        # mod_date is semi-useful when building the initial index
        self.directory = index_directory
        
        self.known_files = {}
        self.wanted_default = record_all

        self._thread = None
        self._build()
        self._lock = RLock()

        self._quitting = False

    def __len__(self):
        return len(self.room_dict)

    def __contains__(self, room_id):
        if room_id in self.room_dict:
            return True
        else:
            return False

    def __getitem__(self, room_id):
        if room_id in self.room_dict:
            return self.room_dict[room_id]
        else:
            return None

    def wants(self, room_id):
        try:
            return self.room_dict[room_id].is_wanted()
        except KeyError:
            return False

    def find_room(self, room_id=None, url=None, name=None, file_name=None):
        """
        Find a room matching one criterion.

        The first provided (non-None, non-False, non-"", etc.) criterion 
        to match a room will be used.
        
        Args:
            room_id: id of the room to search for
            url: last part of room url, 
                48_Tomu_Mutou
            name: member name in the index's language, 
                "Muto Tomu"
            file_name: either a filename, or the unique component of a filename,
                "161018 Showroom - AKB48 Team K Muto Tomu 2104.mp4"
                "AKB48 Team K Muto Tomu"

        Returns:
            A room object matching one of the given criteria, else None if no match is found.
        """
        if room_id:
            try:
                return self.room_dict[room_id]
            except KeyError:
                index_logger.debug("Failed to find ID {}".format(room_id))
        if url:
            # TODO: use the full https://www.showroom-live.com url
            # Primary hangup here is that the "url" could be a number of things...
            # but let us limit it to the end of the url, after the final /
            try:
                return self.room_url_lookup[url]
            except KeyError:
                index_logger.debug("Failed to find Room URL {}".format(url))
        if name:
            # TODO: support separating group/teams from string
            try:
                return self.room_name_lookup[name]
            except KeyError:
                index_logger.debug("Failed to find Room Name {}".format(name))
        if file_name:
            match = _filename_re.match(file_name)
            if match:
                handle = match.groups()[0]
                try:
                    return self.room_handle_lookup[handle]
                except KeyError:
                    index_logger.debug("Failed to find Room Handle {}".format(handle))

        return None

    # Filter methods
    def filter_add(self, names_to_add):
        for room_id in self.room_dict:
            if self.room_dict[room_id].name in names_to_add:
                self.room_dict[room_id].set_wanted(True)

    def filter_remove(self, names_to_remove):
        for room_id in self.room_dict:
            if self.room_dict[room_id].name in names_to_remove:
                self.room_dict[room_id].set_wanted(False)

    def filter_all(self):
        for room_id in self.room_dict:
            self.room_dict[room_id].set_wanted(True)
        self.wanted_default = True

    def filter_none(self):
        for room_id in self.room_dict:
            self.room_dict[room_id].set_wanted(False)
        self.wanted_default = False

    def filter_get_list(self):
        wanted = [e for e in self.room_dict if self.room_dict[e].is_wanted()]
        unwanted = [e for e in self.room_dict if not self.room_dict[e].is_wanted()]

        # is it better to process them here or at the caller?
        '''
        if len(wanted) == len(self.room_dict):
            return {"index_filters": {"wanted": "all", "unwanted": None}}
        elif len(unwanted) == len(self.room_dict):
            return {"index_filters": {"wanted": None, "unwanted": "all"}}
        elif len(wanted) > len(unwanted):
            result = [self.room_dict[e].name for e in unwanted]
            return {"index_filters": {"wanted": "remaining", "unwanted": result}}
        else:
            result = [self.room_dict[e].name for e in wanted]
            return {"index_filters": {"wanted": result, "unwanted": "remaining"}}
        '''

        return {"wanted": [self.room_dict[e].name for e in wanted],
                "unwanted": [self.room_dict[e].name for e in unwanted]}

    # Index methods
    def _build(self):
        index_logger.debug("Building index...")

        # TODO: apply record_all setting
        # get list of *.jdex files in index_directory
        found_files = glob.glob("{}/{}".format(self.directory, "*.jdex"))
        # need to keep a record of each file + mod_date and file_size

        for e in found_files:
            statinfo = os.stat(e)
            self.known_files[e] = {"mod_time": statinfo.st_mtime,
                                   "file_size": statinfo.st_size}

        found_files = sorted(self.known_files, key=lambda x: self.known_files[x]['mod_time'])

        index_logger.debug('\n'.join(['Found index files: '] + found_files))
        new_room_dict = {}

        for jdex in found_files:
            mod_time = self.known_files[jdex]['mod_time']
            # open the jdex
            try:
                with open(jdex, encoding='utf8') as infp:
                    temp_data = json.load(infp)
            except json.JSONDecodeError:
                index_logger.debug('{} could not be read'.format(jdex))
                continue
            # add each room to the room_dict and the room_url_lookup
            # perhaps in this phase it is not necessary to update existing
            # rooms but simply overwrite but later we will need to update
            for room in temp_data:
                if 'engGroup' in room:
                    new_room = Room(room_info=room, mod_time=mod_time,
                                    wanted=self.wanted_default, language=self.language)
                else:
                    # TODO: phase this out over time
                    new_room = RoomOld(room_info=room, mod_time=mod_time,
                                       wanted=self.wanted_default, language=self.language)
                new_room_dict[new_room.room_id] = new_room

        self.room_dict.clear()
        self.room_dict.update(new_room_dict)

    def rebuild(self):
        self.known_files = {}
        self._build()

    def update(self):
        # index_logger.debug("Checking local index")
        found_files = glob.glob("{}/{}".format(self.directory, "*.jdex"))

        changed_files = []

        for e in found_files:
            statinfo = os.stat(e)
            if e in self.known_files:
                # known file w/ changes
                if (statinfo.st_mtime > self.known_files[e]['mod_time'] or
                        statinfo.st_size != self.known_files[e]['file_size']):
                    changed_files.append(e)
                    self.known_files[e] = {'mod_time': statinfo.st_mtime,
                                           'file_size': statinfo.st_size}
            else:
                # new file
                changed_files.append(e)
                self.known_files[e] = {'mod_time': statinfo.st_mtime,
                                       'file_size': statinfo.st_size}

        if len(changed_files) > 0:
            index_logger.info("Updating index")
        else:
            return

        changed_files = [e for e in sorted(self.known_files,
                                           key=lambda x: self.known_files[x]['mod_time'])
                         if e in changed_files]

        # TODO: update information for existing rooms, not just priority
        for jdex in changed_files:
            # is it faster to assume new priorities or to check if they have changed?
            mod_time = self.known_files[jdex]['mod_time']
            try:
                with open(jdex, encoding='utf8') as infp:
                    temp_data = json.load(infp)
            except json.JSONDecodeError:
                index_logger.debug('{} could not be read'.format(jdex))
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
                    if 'engGroup' in room:
                        new_room = Room(room_info=room, mod_time=mod_time,
                                        wanted=self.wanted_default, language=self.language)
                    else:
                        # TODO: phase this out over time
                        new_room = RoomOld(room_info=room, mod_time=mod_time,
                                           wanted=self.wanted_default, language=self.language)
                    self.room_dict[new_room.room_id] = new_room
                    if self._room_url_lookup:
                        self._room_url_lookup[new_room.short_url] = new_room
                    if self._room_name_lookup:
                        self._room_name_lookup[new_room.name] = new_room
                    if self._room_handle_lookup:
                        self._room_handle_lookup[new_room.handle] = new_room

                    # adding a custom room is managed by something else
                    # likewise the option to create rooms from an event or campaign page

    def update_from_web(self, update_url=None):
        """
        :param update_url: URL to a list of JDEX files, w/ name, modtime, and path for each.
            Defaults to https://wlerin.github.io/showroom-index/list.json
            which see for an example.

        Modtime of each file is compared against the local copy and if newer, the contents are
        compared. Priorities and names are not changed. Group and Team may be updated, and any
        new rooms will be added.

        TODO: decide how to handle ignored rooms, i.e. say someone removes a file from the index
        Or just find a better way to manage who gets downloaded, e.g. a config.json option.
        """
        if not update_url:
            update_url = "https://wlerin.github.io/showroom-index/list.json"

        update_data = self.session.get(update_url).json()
        # TODO: Catch the error this raises when decoding fails

        # TODO: finish this method
        # 1) compare mod times
        # 2) get updated and new files
        # 3) compare contents

    def start(self):
        self._quitting = False
        self._thread = Thread(target=self.run, name='ShowroomIndex')
        self._thread.start()

    def stop(self):
        self._quitting = True

    def run(self):
        last_update = datetime.datetime.now(tz=TOKYO_TZ)
        update_interval = 120.0
        while not self._quitting:
            curr_time = datetime.datetime.now(tz=TOKYO_TZ)
            if (curr_time - last_update).total_seconds() > update_interval:
                self.update()
            time.sleep(0.9)

            # TODO: update from web
            # TODO: make last_update an attribute so it can be updated by other methods
            # TODO: make sure this is all thread-safe, in particular any index methods
            # that can be called from outside the index thread

    def _build_name_lookup(self):
        self._room_name_lookup = {}
        for room_id, room in self.room_dict.items():
            self._room_name_lookup[room.name] = room

    def _build_handle_lookup(self):
        self._room_handle_lookup = {}
        for room_id, room in self.room_dict.items():
            # TODO: make this a
            self._room_handle_lookup[room.handle] = room

    def _build_url_lookup(self):
        self._room_url_lookup = {}
        for room_id, room in self.room_dict.items():
            self._room_name_lookup[room.url] = room

    @property
    def room_name_lookup(self):
        with self._lock:
            if not self._room_name_lookup:
                self._build_name_lookup()
        return self._room_name_lookup

    @property
    def room_handle_lookup(self):
        with self._lock:
            if not self._room_handle_lookup:
                self._build_handle_lookup()
        return self._room_handle_lookup

    @property
    def room_url_lookup(self):
        with self._lock:
            if not self._room_url_lookup:
                self._build_url_lookup()
        return self._room_url_lookup
