# scraping comments
import datetime
import json
import os
import threading
import time

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

    def start(self):
        if not self._thread:
            self._thread = threading.Thread(target=self.run, name='{} Comment Log'.format(self.room.name))
            self._thread.start()

    def run(self):
        # TODO: allow comment_logger to trigger get_live_status ?
        last_counts = []
        max_interval = self.settings.comments.max_update_interval
        min_interval = self.settings.comments.min_update_interval

        _, destdir, filename = format_name(self.settings.directory.data,
                                           self.watcher.start_time.strftime(FULL_DATE_FMT),
                                           self.room, ext=self.settings.ffmpeg.container)
        # TODO: modify format_name so it doesn't require so much hackery for this
        filename = filename.replace(self.settings.ffmpeg.container, ' comments.json')
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
                print('HTTP Error while getting comments for {}: {}'.format(self.room.handle, e))
                break
            for comment in data:
                if len(comment['comment']) < 4 and comment['comment'].isdigit():
                    continue
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

    def join(self):
        self._thread.join()


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
