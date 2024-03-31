import asyncio
import json
import datetime
import glob
import aiofiles
from aiofiles import os

from showroom import index as _index
from showroom.constants import TOKYO_TZ
# _index.RLock = asyncio.Lock
from showroom.index import Room, index_logger


# TODO: simplify this/make it async
# TODO: and check that it actually works
# until then, as long as I don't call start() it should be fine
# right now it's basically just copypasted from the synchronous version
class AsyncShowroomIndex(_index.ShowroomIndex):
    def start(self):
        self._quitting = False
        self._thread = asyncio.create_task(self.run(), name='ShowroomIndex')
        # self._thread.start()

    def stop(self):
        self._quitting = True
        self._thread.cancel()

    async def run(self):
        last_update = datetime.datetime.now(tz=TOKYO_TZ)
        update_interval = 120.0
        while not self._quitting:
            # TODO: sleep for entire interval
            curr_time = datetime.datetime.now(tz=TOKYO_TZ)
            if (curr_time - last_update).total_seconds() > update_interval:
                self.update()
            await asyncio.sleep(0.9)

            # TODO: update from web
            # TODO: make last_update an attribute so it can be updated by other methods
            # TODO: make sure this is all thread-safe, in particular any index methods
            # that can be called from outside the index thread

    async def update(self):
        # index_logger.debug("Checking local index")
        found_files = glob.glob("{}/{}".format(self.directory, "*.jdex"))

        changed_files = []

        for e in found_files:
            statinfo = await os.stat(e)
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
                index_logger.warning('{} could not be read'.format(jdex))
                continue
            # check if room exists, if it does, check priority
            # if different, update priority and mod_time
            # if room does not exist, add
            for room in temp_data:
                room_id = room.get('room_id') or room.get('showroom_id')
                if room_id in self.room_dict:
                    # is this check necessary?
                    if room['priority'] != self.room_dict[room_id]['priority']:
                        self.room_dict[room_id].set_priority(room['priority'], mod_time)
                else:
                    new_room = Room(room_info=room, mod_time=mod_time,
                                    wanted=self.wanted_default, language=self.language)
                    self.room_dict[new_room.room_id] = new_room
                    if self._room_url_key_lookup:
                        self._room_url_key_lookup[new_room.short_url] = new_room
                    if self._room_name_lookup:
                        self._room_name_lookup[new_room.name] = new_room
                    if self._room_handle_lookup:
                        self._room_handle_lookup[new_room.handle] = new_room

                    # adding a custom room is managed by something else
                    # likewise the option to create rooms from an event or campaign page