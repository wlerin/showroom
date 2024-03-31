# room object
# fusion of old Room and Watcher objects
# responsible for managing broadcast subscriptions and downloads for a given room
import logging
import asyncio
from .index import Room as IndexRoom


class Room:
    def __init__(self, room: IndexRoom):
        self._room = room
        self.broadcast_key = None
        self.stream_url = None
        self.live_id = 0
        self.is_live = False

    def __getattr__(self, name):
        return getattr(self._room, name)

    def subscribe(self, broadcast):
        pass

    # TODO: come up with a better name
    async def receive(self, message):
        # TODO: receive a message from the broadcaster
        # save it to a file or something
        print(self.get_info(), message)

    def download(self):
        pass


