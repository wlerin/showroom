# scraping comments
import datetime
import json
import os
import threading
import time

# Type “pip install websocket-client” to install.
import websocket  # this is to record comments on real time
import math
import logging
from json import JSONDecodeError
from websocket import WebSocketConnectionClosedException

from showroom.constants import TOKYO_TZ, FULL_DATE_FMT
from showroom.utils import format_name
from requests.exceptions import HTTPError

# TODO: save comments, stats, telop(s)
# {
#     "comment_log": [],
#     "telop": {
#         "latest": {
#             "text": "",
#             "created_at": ""
#         },
#         "older": [
#             {
#                 "text": "",
#                 "created_at": ""
#             }
#         ]
#     },
#     "live_info": {
#         # stuff like view count over time etc.
#     }
# }

'''
Option 1:
2 separate "loggers", one for comments, one for stats/telop
The *only* reason to do this is to allow grabbing just stats and telop instead of all three.

So I'm not going to do that. What's option 2.

Options 2:
StatsLogger, CommentsLogger, RoomLogger:
StatsLogger records just stats and telop
'''

cmt_logger = logging.getLogger('showroom.comments')


def convert_comments_to_danmaku(startTime, commentList,
                                fontsize=18, fontname='MS PGothic', alpha='1A',
                                width=640, height=360):
    """
    Convert comments to danmaku (弾幕 / bullets) subtitles

    :param startTime: comments recording start time (timestamp in milliseconds)
    :param commentList: list of showroom messages
    :param fontsize = 18
    :param fontname = 'MS PGothic'
    :param alpha = '1A'     # transparency '00' to 'FF' (hex string)
    :param width = 640      # video screen height
    :param height = 360     # video screen width

    :return a string of danmaku subtitles
    """

    # slotsNum: max number of comment line vertically shown on screen
    slotsNum = math.floor(height / fontsize)
    travelTime = 8 * 1000  # 8 sec, bullet comment flight time on screen

    # ass subtitle file header
    danmaku = "[Script Info]\n"
    danmaku += "ScriptType: v4.00+\n"
    danmaku += "Collisions: Normal\n"
    danmaku += "PlayResX: " + str(width) + "\n"
    danmaku += "PlayResY: " + str(height) + "\n\n"
    danmaku += "[V4+ Styles]\n"
    danmaku += "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n"
    danmaku += "Style: danmakuFont, " + fontname + ", " + str(fontsize) + \
               ", &H00FFFFFF, &H00FFFFFF, &H00000000, &H00000000, 1, 0, 0, 0, 100, 100, 0.00, 0.00, 1, 1, 0, 2, 20, 20, 20, 0\n\n"
    danmaku += "[Events]\n"
    danmaku += "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n"

    # each comment line on screen can be seen as a slot
    # each slot will be filled with the time which indicates when the bullet comment will disappear on screen
    # slot[0], slot[1], slot[2], ...: for the comment lines from top to down
    slots = []
    for i in range(slotsNum):
        slots.append(0)

    previousTelop = ''

    for data in commentList:
        m_type = str(data['t'])
        if m_type != '1' and m_type != '8':
            # not a comment and not a telop
            continue

        comment = ''
        if m_type == '8':  # telop
            telop = data['telop']
            if telop is not None and telop != previousTelop:
                previousTelop = telop
                # show telop as a comment
                comment = 'Telop: 【' + telop + '】'
            else:
                continue

        else:  # comment
            comment = data['cm']

        # compute current relative time
        t = data['received_at'] - startTime

        # find available slot vertically from up to down
        selectedSlot = 0
        isSlotFound = False
        for j in range(slotsNum):
            if slots[j] <= t:
                slots[j] = t + travelTime  # replaced with the time that it will finish
                isSlotFound = True
                selectedSlot = j
                break

        # when all slots have larger times, find the smallest time and replace the slot
        if not isSlotFound:
            minIdx = 0
            for j in range(1, slotsNum):
                if slots[j] < slots[minIdx]:
                    minIdx = j

            slots[minIdx] = t + travelTime
            selectedSlot = minIdx

        # calculate bullet comment flight positions, from (x1,y1) to (x2,y2) on screen

        # extra flight length so a comment appears and disappears outside of the screen
        extraLen = math.ceil(len(comment) / 2.0)

        x1 = width + extraLen * fontsize
        y1 = (selectedSlot + 1) * fontsize
        x2 = 0 - extraLen * fontsize
        y2 = y1

        def msecToAssTime(uTime):
            """ convert milliseconds to ass subtitle format """
            msec = uTime % 1000
            msec = int(round(msec / 10.0))
            uTime = math.floor(uTime / 1000.0)
            s = int(uTime % 60)
            uTime = math.floor(uTime / 60.0)
            m = int(uTime % 60)
            h = int(math.floor(uTime / 60.0))
            msf = ("00" + str(msec))[-2:]
            sf = ("00" + str(s))[-2:]
            mf = ("00" + str(m))[-2:]
            hf = ("00" + str(h))[-2:]
            return hf + ":" + mf + ":" + sf + "." + msf

        # build ass subtitle script
        sub = "Dialogue: 3," + msecToAssTime(t) + "," + msecToAssTime(t + travelTime)
        # alpha: 00 means fully visible, and FF (ie. 255 in decimal) is fully transparent.
        sub += ",danmakuFont,,0000,0000,0000,,{\\alpha&H" + alpha + "&\\move("
        sub += str(x1) + "," + str(y1) + "," + str(x2) + "," + str(y2)
        sub += ")}" + comment + "\n"

        danmaku += sub
    # end of for
    return danmaku


class CommentLogger(object):
    comment_id_pattern = "{created_at}_{user_id}"

    def __init__(self, room, client, settings, watcher):
        self.room = room
        self.client = client
        self.settings = settings
        self.watcher = watcher

        self.last_update = datetime.datetime.fromtimestamp(10000, tz=TOKYO_TZ)
        self.update_interval = self.settings.comments.default_update_interval

        self.comment_log = []
        self.comment_ids = set()
        self._thread = None
        self.comment_count = 0
        self.ws = None
        self.ws_startTime = 0
        self.ws_send_txt = ''
        self._thread_interval = None
        self._isQuit = False
        self._isRecording = False

    @property
    def isRecording(self):
        return self._isRecording

    def start(self):
        if not self._thread:
            self._thread = threading.Thread(target=self.run, name='{} Comment Log'.format(self.room.name))
            self._thread.start()

    def run(self):
        """
        Record comments and save as niconico danmaku (弾幕 / bullets) subtitle ass file
        """

        def ws_on_message(ws, message):
            """ WebSocket callback """
            # "created at" has no millisecond part, so we record the precise time here
            now = int(time.time() * 1000)

            idx = message.find("{")
            if idx < 0:
                cmt_logger.error('no JSON message - {}'.format(message))
                return
            message = message[idx:]
            try:
                data = json.loads(message)
            except JSONDecodeError as e:
                cmt_logger.debug('broken message, JSON decode error: {}'.format(e))
                cmt_logger.debug('--> {}'.format(message))
                # try to fix
                message += '","t":"1"}'
                try:
                    data = json.loads(message)
                except JSONDecodeError:
                    cmt_logger.error('failed to fix broken message, JSON decode error: {}'.format(message))
                    return
                cmt_logger.debug('--> fix passed: {}'.format(message))

            # add current time
            data['received_at'] = now

            # Some useful info in the message:
            # ['t']  message type, determine the message is comment, telop, or gift
            # ['cm'] comment
            # ['ac'] name
            # ['u']  user_id
            # ['av'] avatar_id
            # ['g'] gift_id
            # ['n'] gift_num

            # type of the message
            m_type = str(data['t'])  # could be integer or string

            if m_type == '1':  # comment
                comment = data['cm']

                # skip counting for 50
                if len(comment) < 3 and comment.isdecimal() and int(comment) <= 50:
                    # s1 = '⑷'; s2 = u'²'; s3 = '❹'
                    # print(s1.isdigit())  # True
                    # print(s2.isdigit())  # True
                    # print(s1.isdecimal())  # False
                    # print(s2.isdecimal())  # False
                    # int(s1)  # ValueError
                    # int(s2)  # ValueError
                    pass
                else:
                    comment = comment.replace('\n', ' ')  # replace line break to a space
                    # cmt_logger.info('{}: {}'.format(self.room.name, comment))
                    data['cm'] = comment
                    self.comment_log.append(data)
                    self.comment_count += 1

            elif m_type == '2':  # gift
                pass

            elif m_type == '8':  # telop
                self.comment_log.append(data)
                if data['telop'] is not None:  # could be null
                    # cmt_logger.info('{}: telop = {}'.format(self.room.name, data['telop']))
                    pass

            elif m_type == '11':  # cumulated gifts report
                pass

            elif m_type == '101':  # indicating live finished
                self.comment_log.append(data)
                self._isQuit = True

            else:
                self.comment_log.append(data)

        def ws_on_error(ws, error):
            """ WebSocket callback """
            cmt_logger.error('websocket on error: {} - {}'.format(type(error).__name__, error))

        def ws_on_close(ws):
            """ WebSocket callback """
            cmt_logger.debug('websocket closed')
            self._isQuit = True

        def interval_send(ws):
            """
            interval thread to send message and to close WebSocket
            """
            count = 60
            while True:
                # check whether to quit every sec
                if self._isQuit:
                    break

                # send bcsvr_key every 60 secs
                if count >= 60:
                    count = 0
                    try:
                        # cmt_logger.debug('sending {}'.format(self.ws_send_txt))
                        ws.send(self.ws_send_txt)
                    except WebSocketConnectionClosedException as e:
                        cmt_logger.debug(
                            'WebSocket closed before sending message. {} Closing interval thread now...'.format(e))
                        break

                time.sleep(1)
                count += 1

            # close WebSocket
            if ws is not None:
                ws.close()
                ws = None
            cmt_logger.debug('interval thread finished')

        def ws_on_open(ws):
            """ WebSocket callback """
            self.ws_startTime = int(time.time() * 1000)
            cmt_logger.debug('websocket on open')

            # keep sending bcsvr_key to prevent disconnection
            self._thread_interval = threading.Thread(target=interval_send,
                                                     name='{} Comment Log interval'.format(self.room.name), args=(ws,))
            self._thread_interval.start()

        def ws_start(ws_uri, on_open=ws_on_open, on_message=ws_on_message,
                     on_error=ws_on_error, on_close=ws_on_close):
            """ WebSocket main loop """
            self.ws = websocket.WebSocket()
            # connect
            try:
                self.ws.connect(ws_uri)
            except Exception as e:
                on_error(self.ws, e)
                return

            on_open(self.ws)

            while True:
                if self._isQuit:
                    break

                try:
                    frame = self.ws.recv_frame()
                except WebSocketConnectionClosedException as e:
                    cmt_logger.debug('WebSocket Closed')
                    break
                except Exception as e:
                    on_error(self.ws, e)
                    break

                if frame.opcode != websocket.ABNF.OPCODE_TEXT:
                    cmt_logger.debug('ignored frame opcode = {}'.format(websocket.ABNF.OPCODE_MAP[frame.opcode]))
                    cmt_logger.debug('--> {}'.format(frame.data))
                    continue

                data = frame.data
                message = ''
                try:
                    message = data.decode('utf-8')
                except UnicodeDecodeError as e:
                    message = data.decode('latin-1')
                    cmt_logger.debug('decoded as latin-1: {}'.format(message))
                except Exception as e:
                    on_error(self.ws, e)

                on_message(self.ws, message)

            on_close(self.ws)
            self.ws.close()

        # Get live info from https://www.showroom-live.com/api/live/live_info?room_id=xxx
        # If a room closes and then reopen on live within 30 seconds (approximately),
        # the broadcast_key from https://www.showroom-live.com/api/live/onlives
        # will not be updated with the new key. It's the same situation that when a
        # room live is finished, /api/live/onlives will not update its onlives list within
        # about 30 seconds. So here it's better to get accurate broadcast_key
        # from /api/live/live_info
        try:
            info = self.client.live_info(self.room.room_id) or []
        except HTTPError as e:
            # TODO: log/handle properly
            cmt_logger.error('HTTP Error while getting live_info for {}: {}'.format(self.room.handle, e))
            return

        if len(info['bcsvr_key']) == 0:
            cmt_logger.debug('not on live, no bcsvr_key.')
            return

        #        # TODO: allow comment_logger to trigger get_live_status ?
        #        last_counts = []
        #        max_interval = self.settings.comments.max_update_interval
        #        min_interval = self.settings.comments.min_update_interval

        _, destdir, filename = format_name(self.settings.directory.data,
                                           self.watcher.start_time.strftime(FULL_DATE_FMT),
                                           self.room, ext=self.settings.ffmpeg.container)
        # TODO: modify format_name so it doesn't require so much hackery for this
        filename = filename.replace(self.settings.ffmpeg.container, ' comments.json')
        filenameAss = filename.replace(' comments.json', 'ass')
        destdir += '/comments'
        # TODO: only call this once per group per day
        os.makedirs(destdir, exist_ok=True)
        outfile = '/'.join((destdir, filename))
        outfileAss = '/'.join((destdir, filenameAss))

        #        def add_counts(count):
        #            return [count] + last_counts[:2]

        cmt_logger.info("Recording comments for {}".format(self.room.name))

        #        while self.watcher.is_live():
        #            count = 0
        #            seen = 0
        #            # update comments
        #            try:
        #                data = self.client.comment_log(self.room.room_id) or []
        #            except HTTPError as e:
        #                # TODO: log/handle properly
        #                print('HTTP Error while getting comments for {}: {}'.format(self.room.handle, e))
        #                break
        #            for comment in data:
        #                if len(comment['comment']) < 4 and comment['comment'].isdigit():
        #                    continue
        #                cid = self.comment_id_pattern.format(**comment)
        #                if cid not in self.comment_ids:
        #                    self.comment_log.append(comment)
        #                    self.comment_ids.add(cid)
        #                    count += 1
        #                else:
        #                    seen += 1
        #
        #                if seen > 5:
        #                    last_counts = add_counts(count)
        #                    break
        #
        #            # update update_interval if needed
        #            highest_count = max(last_counts, default=10)
        #            if highest_count < 7 and self.update_interval < max_interval:
        #                self.update_interval += 1.0
        #            elif highest_count > 50 and self.update_interval > min_interval:
        #                self.update_interval *= 0.5
        #            elif highest_count > 20 and self.update_interval > min_interval:
        #                self.update_interval -= 1.0
        #
        #            current_time = datetime.datetime.now(tz=TOKYO_TZ)
        #            timediff = (current_time - self.last_update).total_seconds()
        #            self.last_update = current_time
        #
        #            sleep_timer = max(0.5, self.update_interval - timediff)
        #            time.sleep(sleep_timer)

        self._isRecording = True
        self.ws_send_txt = 'SUB\t' + info['bcsvr_key']
        websocket.enableTrace(False)  # False: disable trace outputs

        ws_start('ws://' + info['bcsvr_host'] + ':' + str(info['bcsvr_port']),
                 on_open=ws_on_open, on_message=ws_on_message,
                 on_error=ws_on_error, on_close=ws_on_close)

        if self._thread_interval is not None:
            self._thread_interval.join()

        # sorting
        self.comment_log = sorted(self.comment_log, key=lambda x: x['received_at'])

        with open(outfile, 'w', encoding='utf8') as outfp:
            #            json.dump({"comment_log": sorted(self.comment_log, key=lambda x: x['created_at'], reverse=True)},
            #                      outfp, indent=2, ensure_ascii=False)
            json.dump(self.comment_log, outfp, indent=2, ensure_ascii=False)

        if self.comment_count > 0:
            # convert comments to danmaku
            assTxt = convert_comments_to_danmaku(self.ws_startTime, self.comment_log,
                                                 fontsize=18, fontname='MS PGothic', alpha='1A',
                                                 width=640, height=360)
            with open(outfileAss, 'w', encoding='utf8') as outfpAss:
                outfpAss.write(assTxt)
            cmt_logger.info('Completed {}'.format(outfileAss))

        else:
            cmt_logger.info('No comments to save for {}'.format(self.room.name))

        self._isRecording = False

    def quit(self):
        """
        To quit comment logger anytime (to close WebSocket, save file and finish job)
        """
        self._isQuit = True
        self._thread.join()
        if self._thread_interval is not None:
            self._thread_interval.join()


class RoomScraper:
    comment_id_pattern = "{created_at}_{user_id}"

    def __init__(self, room, client, settings, watcher, record_comments=False):
        self.room = room
        self.client = client
        self.settings = settings
        self.watcher = watcher

        self.last_update = datetime.datetime.fromtimestamp(10000, tz=TOKYO_TZ)
        self.update_interval = self.settings.comments.default_update_interval

        self.comment_log = []
        self.comment_ids = set()
        self._thread = None

        self.record_comments = record_comments

    def start(self):
        if not self._thread:
            if self.record_comments:
                self._thread = threading.Thread(target=self.record_with_comments,
                                                name='{} Room Log'.format(self.room.name))
            else:
                self._thread = threading.Thread(target=self.record,
                                                name='{} Room Log'.format(self.room.name))
            self._thread.start()

    def _fetch_comments(self):
        pass

    def _parse_comments(self, comment_log):
        pass

    def _fetch_info(self):
        "https://www.showroom-live.com/room/get_live_data?room_id=76535"
        pass

    def _parse_info(self, info):
        result = {
            # TODO: check for differences between result and stored data
            # some of this stuff should never change and/or is useful in the Watcher
            "live_info": {
                "created_at": info['live_res'].get('created_at'),
                "started_at": info['live_res'].get('started_at'),
                "live_id": info['live_res'].get('live_id'),
                "comment_num": info['live_res'].get('comment_num'),  # oooohhhhhh
                # "chat_token": info['live_res'].get('chat_token'),
                "hot_point": "",
                "gift_num": "",
                "live_type": "",
                "ended_at": "",
                "view_uu": "",
                "bcsvr_key": "",
            },
            "telop": info['telop'],
            "broadcast_key": "",  # same as live_res.bcsvr_key
            "online_user_num": "",  # same as live_res.view_uu
            "room": {
                "last_live_id": "",
            },
            "broadcast_port": 8080,
            "broadcast_host": "onlive.showroom-live.com",
        }
        pass

    def record_with_comments(self):
        # TODO: allow comment_logger to trigger get_live_status ?
        last_counts = []
        max_interval = self.settings.comments.max_update_interval
        min_interval = self.settings.comments.min_update_interval

        _, destdir, filename = format_name(self.settings.directory.data,
                                           self.watcher.start_time.strftime(FULL_DATE_FMT),
                                           self.room, self.settings.ffmpeg.container)
        # TODO: modify format_name so it doesn't require so much hackery for this
        filename = filename.replace('.{}'.format(self.settings.ffmpeg.container), ' comments.json')
        destdir += '/comments'
        # TODO: only call this once per group per day
        os.makedirs(destdir, exist_ok=True)
        outfile = '/'.join((destdir, filename))

        def add_counts(count):
            return [count] + last_counts[:2]

        print("Recording comments for {}".format(self.room.name))

        while self.watcher.is_live():
            count = 0
            seen = 0
            # update comments
            try:
                data = self.client.comment_log(self.room.room_id) or []
            except HTTPError as e:
                # TODO: log/handle properly
                print('HTTP Error while getting comments for {}: {}\n{}'.format(self.room.handle, e, e.response.content))
                break

            for comment in data:
                cid = self.comment_id_pattern.format(**comment)
                if cid not in self.comment_ids:
                    self.comment_log.append(comment)
                    self.comment_ids.add(cid)
                    count += 1
                else:
                    seen += 1

                if seen > 5:
                    last_counts = add_counts(count)
                    break

            # update update_interval if needed
            highest_count = max(last_counts, default=10)
            if highest_count < 7 and self.update_interval < max_interval:
                self.update_interval += 1.0
            elif highest_count > 50 and self.update_interval > min_interval:
                self.update_interval *= 0.5
            elif highest_count > 20 and self.update_interval > min_interval:
                self.update_interval -= 1.0

            current_time = datetime.datetime.now(tz=TOKYO_TZ)
            timediff = (current_time - self.last_update).total_seconds()
            self.last_update = current_time

            sleep_timer = max(0.5, self.update_interval - timediff)
            time.sleep(sleep_timer)

        with open(outfile, 'w', encoding='utf8') as outfp:
            json.dump({"comment_log": sorted(self.comment_log, key=lambda x: x['created_at'], reverse=True)},
                      outfp, indent=2, ensure_ascii=False)

    def record(self):
        pass

    def join(self):
        pass
