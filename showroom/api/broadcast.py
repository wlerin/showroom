# connecting to and handling Showroom's websocket broadcast server
# this may not be possible in the client? a lot of the on_message etc. handling requires
# communication with Watcher, etc.
# heavily based on the commentlogger implementation by tamagoyanki
# I suppose I can document what each comment type means here? and create some kind of queue that
# Watcher and other objects can consume?
# But there still some necessary interoperability, especially when waiting on a stream to start

import datetime
import json
import os
import queue
import time
import logging  # not async safe?
import random
import threading

import aiohttp
import asyncio

from json import JSONDecodeError


socket_logger = logging.getLogger('showroom.websocket')

# None - some kind of colour thing? rgb = (255,255,255)
# maybe it's the telop colouring? idk
# 1 - comment
# 2 - gift
# 3 - voting start
# 4 - voting result
# 6 - no idea? has a user_id, a field labelled "at" which is also 6. in other types, at=0
# 18 - these are notifications about users, i.e. first visits, new followers, level ups
# -- there's both a Japanese message ('m') and an English message ('me')
# 100 - ??? seems to get spammed after a live ends?
# 101 - indicates end of live
# 104 - indicates start of live (if watching stopped room)
# This won't actually get used in this first version, but I do hope to expand the list of message types and
# eventually handle them properly
MESSAGE_TYPE_MAP = {
    None: "",
    1: "comment",
    2: "gift",
    3: "vote start",
    4: "vote result",
    5: None,
    6: "absolute terror",  # I have no idea
    8: "telop",
    # unknown significance
    # four fields, n, at, ai, s
    # at and s are usually 0
    # if ai == 3, then at == 20 and s == 10
    # if ai == 501, then at == 12
    # n seems to range from 1 to 10
    # 10 is by far the most common followed by 1, then 2, then the rest mostly in ascending order
    17: None,
    18: "viewer status update",
    100: "live wait",  # sent periodically, no rhyme or reason
    101: "live end",
    104: "live start",
    # the following appear to deal with purchases of advertised goods e.g. photobooks
    401: None,
    402: None,
    403: None
}


class ShowroomWebSocket:
    comment_id_pattern = "{created_at}_{user_id}"

    # This version shouldn't be messing with the watcher etc. at all
    # Does it need a session? Probably?
    def __init__(self, session, room_name,
                 server_key, server_host='online.showroom-live.com', server_port=8080,
                 resubscription_interval=None):
        self._session = session
        self._room_name = room_name
        self._key = server_key
        self._host = server_host
        self._port = server_port

        # resubscribe between 5 to 10 minutes later
        # caller can set this lower based on priority, but probably won't bother
        self._resubscription_interval = resubscription_interval or 300+random.randint(0, 300)

        # TODO: make this an actual, thread-safe queue
        # TODO: append a stop signal once messages end (e.g. None)
        self._message_queue = queue.Queue()

        self._thread = None
        self._thread_interval = None
        self.ws = None
        self._quit = False

    def get(self, block=False):
        msg = self._message_queue.get(block=block)
        self._message_queue.task_done()
        return msg

    def start(self):
        if not self._thread:
            self._thread = threading.Thread(target=self.run, name=f'{self._room_name} Message Socket')
            self._thread.start()

    def run(self):
        def ws_on_message(ws, message):
            """ WebSocket callback """
            # "created at" has no millisecond part, so we record the precise time here
            now = int(time.time()*1000)
            idx = message.find('{')
            if idx < 0:
                socket_logger.error('no JSON message - {}'.format(message))
                return
            message = message[idx:]

            try:
                data = json.loads(message)
            except JSONDecodeError as e:
                socket_logger.debug('JSONDecodeError, broken message: {}'.format(message))
                # try to fix
                message += '","t":"1"}'
                try:
                    data = json.loads(message)
                except JSONDecodeError:
                    socket_logger.error('JSONDecodeError, failed to fix broken message: {}'.format(message))
                    return
                socket_logger.debug('broken message, JSONDecodeError is fixed: {}'.format(message))

            data['received_at'] = now

            # Some useful info in the message:
            # ['t']  message type, determine the message is comment, telop, or gift
            # ['cm'] comment
            # ['ac'] name
            # ['u']  user_id
            # ['av'] avatar_id
            # ['g'] gift_id
            # ['n'] gift_num
            
            message_type = int(data.get('t'))

            # ignore
            if message_type == 1:
                comment = data['cm']
                if len(comment) < 3 and comment.isdecimal() and int(comment) <= 50:
                    return
            # either the stream ended, or a new one is starting, either way this socket should close
            elif message_type in (101, 104):
                self._quit = True
            # elif message_type == '100':
            #     pass
            self._message_queue.put_nowait(data)

        def ws_on_error(ws, error):
            socket_logger.error('websocket on error: {} - {}'.format(type(error).__name__, error))

        def ws_on_close(ws):
            """ WebSocket callback """
            # socket_logger.debug('websocket closed')
            self._quit = True

        def interval_send():
            """
            interval thread to send message and to close WebSocket
            """

            count = self._resubscription_interval
            while True:
                # check whether to quit every sec
                if self._quit:
                    break

                # send bcsvr_key every 60 secs
                if count >= self._resubscription_interval:
                    count = 0
                    try:
                        self.ws.send(f'SUB\t{self._key}')
                    except WebSocketConnectionClosedException as e:
                        socket_logger.debug(
                            'WebSocket closed before sending message. {} Closing interval thread now...'.format(e))
                        break

                time.sleep(1)
                count += 1

            # close WebSocket
            if self.ws is not None:
                self.ws.close()
                self.ws = None

        def ws_on_open(ws):
            """ WebSocket callback """

            # keep sending bcsvr_key to prevent disconnection
            self._thread_interval = threading.Thread(target=interval_send,
                                                     name=f'{self._room_name} Message Interval',)
            self._thread_interval.start()

        def ws_start(ws_uri, on_open=ws_on_open, on_message=ws_on_message, on_error=ws_on_error, on_close=ws_on_close):
            self.ws = websocket.WebSocket()
            try:
                self.ws.connect(ws_uri)
            except Exception as e:
                on_error(self.ws, e)
                return

            socket_logger.debug('sending opening message')
            on_open(self.ws)

            buffer = b""
            buffered_opcode = ABNF.OPCODE_TEXT
            while not self._quit:
                try:
                    frame = self.ws.recv_frame()
                except WebSocketConnectionClosedException as e:
                    socket_logger.debug('ws_start: WebSocket Closed')
                    self._quit = True
                    break
                except Exception as e:
                    on_error(self.ws, e)
                    self._quit = True
                    break

                """
                Fragmented frame example: For a text message sent as three fragments, 
                the 1st fragment: opcode = 0x1 (OPCODE_TEXT) and FIN bit = 0, 
                the 2nd fragment: opcode = 0x0 (OPCODE_CONT) and FIN bit = 0, 
                the last fragment: opcode = 0x0 (OPCODE_CONT) and FIN bit = 1. 
                """
                if frame.opcode in (ABNF.OPCODE_TEXT, ABNF.OPCODE_BINARY, ABNF.OPCODE_CONT):
                    buffer += frame.data
                    if frame.opcode != ABNF.OPCODE_CONT:
                        buffered_opcode = frame.opcode
                    else:
                        socket_logger.debug('ws_start: fragment message: {}'.format(frame.data))

                    # it's either a last fragmented frame, or a non-fragmented single message frame
                    if frame.fin == 1:
                        data = buffer
                        buffer = b""
                        if buffered_opcode == ABNF.OPCODE_TEXT:
                            message = ""
                            try:
                                message = data.decode('utf-8')
                            except UnicodeDecodeError as e:
                                message = data.decode('latin-1')
                                socket_logger.debug('ws_start: UnicodeDecodeError, decoded as latin-1: {}'.format(message))
                            except Exception as e:
                                on_error(self.ws, e)

                            on_message(self.ws, message)

                        elif buffered_opcode == ABNF.OPCODE_BINARY:
                            socket_logger.debug('ws_start: received unknown binary data: {}'.format(data))

                elif frame.opcode == ABNF.OPCODE_CLOSE:
                    # socket_logger.debug('ws_start: received close opcode')
                    # self.ws.close() will try to send close frame, so we skip sending close frame here
                    break

                elif frame.opcode == ABNF.OPCODE_PING:
                    socket_logger.debug('ws_start: received ping, sending pong')
                    if len(frame.data) < 126:
                        self.ws.pong(frame.data)
                    else:
                        socket_logger.debug('ws_start: ping message too big to send')

                elif frame.opcode == ABNF.OPCODE_PONG:
                    socket_logger.debug('ws_start: received pong')

                else:
                    socket_logger.error('ws_start: unknown frame opcode = {}'.format(frame.opcode))

            on_close(self.ws)
            self.ws.close()

        # a lot of the work in the original here is going to be performed by the caller instead
        # self._recording = True
        # self.ws_send_txt = 'SUB\t' + info['bcsvr_key']
        websocket.enableTrace(False)

        ws_start(f'wss://{self._host}',
                 on_open=ws_on_open, on_message=ws_on_message,
                 on_error=ws_on_error, on_close=ws_on_close)

        if self._thread_interval is not None:
            self._thread_interval.join()

    def quit(self):
        self._quit = True
        self._thread.join()
        if self._thread_interval is not None:
            self._thread_interval.join()

    def wait(self):
        self._thread.join()

    def empty(self):
        return self.ws.empty()
