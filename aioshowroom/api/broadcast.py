
from random import random
import asyncio
import time
import logging
import json
import aiohttp

from .session import ClientSession
broadcast_logger = logging.getLogger('aioshowroom.api.broadcast')


class ShowroomBroadcast:
    def __init__(self, session: ClientSession = None):
        self.broadcast_host = None
        self.websocket = None
        if not session:
            # is it possible to create this outside an async function?
            session = ClientSession()
        self.session = session

        self.subscribers = dict()
        self._ws_semaphore = asyncio.Semaphore(100)

    # what's the best way to organise this? should the session be passed in the constructor?
    # should it have its own session separate from the API?
    # note that the listener needs to be in the same task thread as the connection
    # do we need to start listening here, and dump the messages into a Queue?
    # I'm not sure how to do it otherwise, I can't be waiting on the messages in the main task thread
    # async def connect(self, host):
    #     self.websocket = self.session.ws_connect(host)

    async def subscribe(self, room):
        key = room.broadcast_key
        self.subscribers[key] = room
        while key == room.broadcast_key:
            async with self._ws_semaphore:
                msg = f'SUB\t{key}'
                await self.websocket.send_str(msg)
                broadcast_logger.debug(
                    '/t'.join((
                        str(int(time.time())),
                        msg,
                        room.room_url_key
                    )))
            # on the web, the socket pretty consistently refreshes after 11 minutes or so
            # however on one occasion I have seemingly missed the live start notification with a roughly 10 minute sleep
            await asyncio.sleep(8*60 + random()*120)
        broadcast_logger.debug(
            '/t'.join((
                str(int(time.time())),
                'UNSUB', key,
                room.room_url_key
            )))

    async def listen(self, host: str = "wss://online.showroom-live.com"):
        async with self.session.ws_connect(host) as self.websocket:
            async for msg in self.websocket:
                if msg.type is aiohttp.WSMsgType.TEXT:
                    msg_string = msg.data
                    broadcast_logger.debug(
                        '/t'.join((
                            str(int(time.time())),
                            msg_string,
                        )))
                    msg_method, msg_key, msg_text = msg_string.split('\t')
                    msg_data = json.loads(msg_text)
                    # TODO: does this need to use create_task? this would be a decentish experiment to see
                    # if tasks need to be gathered/waited on to run
                    asyncio.create_task(self.subscribers[msg_key].receive(msg_data))
                yield msg

    async def send(self, msg):
        # TODO:
        await self.websocket.send_str(msg)

    # probably the room watcher needs to handle this
    # async def subscribe(self, key):
