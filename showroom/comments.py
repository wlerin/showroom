# scraping comments
import datetime
import json
import os
import threading
import time

# Type “pip install websocket-client” to install.
import websocket    # this is to record comments on real time
import math

from json import JSONDecodeError

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

# from https://stackoverflow.com/questions/2697039/python-equivalent-of-setinterval
class setInterval :
    def __init__(self,interval,action) :
        self.interval=interval
        self.action=action
        self.stopEvent=threading.Event()
        thread=threading.Thread(target=self.__setInterval)
        thread.start()

    def __setInterval(self) :
        nextTime=time.time()+self.interval
        while not self.stopEvent.wait(nextTime-time.time()) :
            nextTime+=self.interval
            self.action()

    def cancel(self, name) :
        self.stopEvent.set()
        print(name + ' interval thread closed')


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

        self.ws = None
        self.ws_startTime = 0
        self.ws_send_txt = ''
        self.ws_inter = None

    def start(self):
        if not self._thread:
            self._thread = threading.Thread(target=self.run, name='{} Comment Log'.format(self.room.name))
            self._thread.start()

    
    def run(self):
        # TODO: record real time comments and save as niconico danmaku (弾幕 / bullets) subtitle ass file
        
        
        # convert milliseconds to ass subtitle fromat
        def secToFormatStr(uTime):
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
            
            return hf+":"+mf+":"+sf+"."+msf        
        
        # convert comments to danmaku (弾幕 / bullets) subtitles
        def convert_comments_to_danmaku(startTime, commentList, fontsize, fontname, alpha, width, height):
            
            # startTime = self.ws_startTime    # comments recording start time
            # commentList = self.comment_log
            # fontsize = 18
            # fontname = 'MS PGothic'
            # alpha = '1A'                     # transparency '00' to 'FF' (hex string)
            # width = 640                      # screen height
            # height = 360                     # screen width
            


            # slotsNum: max number of comment line vertically shown on screen
            slotsNum = math.floor(height / fontsize)
            travelTime = 8*1000;    # 8 sec, bullet comment flight time on screen
        
            # ass subtitle file header
            danmaku = "[Script Info]" + "\n"
            danmaku += "ScriptType: v4.00+" + "\n"
            danmaku += "Collisions: Normal" + "\n"
            danmaku += "PlayResX: " + str(width) + "\n"
            danmaku += "PlayResY: " + str(height) + "\n"
            danmaku += "" + "\n"
            danmaku += "[V4+ Styles]" + "\n"
            danmaku += "Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding" + "\n"
            danmaku += "Style: danmakuFont, "+fontname+", "+str(fontsize)+", &H00FFFFFF, &H00FFFFFF, &H00000000, &H00000000, 1, 0, 0, 0, 100, 100, 0.00, 0.00, 1, 1, 0, 2, 20, 20, 20, 0" + "\n"
            danmaku += "" + "\n"
            danmaku += "[Events]" + "\n"
            danmaku += "Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text" + "\n"
            

            # each comment line on screen can be seen as a slot
            # each slot will be filled with the time which indidates when the bullet comment will disappear on screen
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
                if m_type == '8':   # telop
                    telop = data['telop']
                    if telop is not None and telop != previousTelop:
                        previousTelop = telop
                        # show telop as a comment
                        comment = 'Telop: 【' + telop + '】'
                    else:
                        continue
                    
                else:   # comment
                    comment = data['cm']
                
                
                # compute current relative time
                t = data['received_at'] - startTime
                
                # find available slot vertically from up to down
                selectedSlot = 0
                isSlotFound = False
                for j in range(slotsNum):
                    if slots[j] <= t:
                        slots[j] = t + travelTime    # replaced with the time that it will finish
                        isSlotFound = True
                        selectedSlot = j
                        break
                    
                # when all slots have larger times, find the smallest time and replace the slot
                if isSlotFound != True:
                    minIdx = 0
                    for j in range(1, slotsNum):
                        if slots[j] < slots[minIdx]:
                            minIdx = j
                        
                    slots[minIdx] = t + travelTime
                    selectedSlot = minIdx
                
                
                
                # TODO: calculate bullet comment flight positions, from (x1,y1) to (x2,y2) on screen

                # extra flight length so a comment appears and disappears outside of the screen
                extraLen = math.ceil(len(comment) / 2.0)                    

                x1 = width + extraLen * fontsize
                y1 = (selectedSlot+1) * fontsize
                x2 = 0 - extraLen * fontsize
                y2 = y1
                
                # build ass subtitle script
                s = "Dialogue: 3," + secToFormatStr(t) + "," + secToFormatStr(t + travelTime)
                # alpha: 00 means fully visible, and FF (ie. 255 in decimal) is fully transparent.
                s += ",danmakuFont,,0000,0000,0000,,{\\alpha&H"+alpha+"&\\move("
                s += str(x1) + "," + str(y1) + "," + str(x2) + "," + str(y2)
                s += ")}" + comment + "\n"    
            
                danmaku += s
            
            # end of for loop
            
            return danmaku
        
        

        
        def send_bcsvr_key():
            #print(self.room.name+' sending: ' + self.ws_send_txt)
            ws.send(self.ws_send_txt)  


        # websocket callback
        def ws_on_message(ws, message):
            # "created at" has no millisecond part, so we record the precise time here
            now = int(time.time()*1000)
            #print(self.room.name + ': ' +str(now) + " - " + message)

            idx = message.find("{")
            #print(idx)
                
            if idx < 0:
                print(self.room.name + ': no JSON message - ' + message)
                return
            
            message = message[idx:]
            #print(message)
            data = {}
            try:
                data = json.loads(message)

            except JSONDecodeError as e:
                print('JSON decoding error while getting comments from {}: {}'.format(self.room.name, e))
                print('--> ' +message)
                return
            
            # add current time
            data['received_at'] = now

            #self.comment_log.append(data)
            #print('m_type = ' + str(data['t']))
            
            ''' JASON data:
            ['t']  message type
            
            ['cm'] comment
            ['ac'] name
            ['u']  user_id
            ['av'] avatar_id
            
            ['g'] gift_id
            ['n'] gift_num
            '''
            
            # type of the message
            m_type = str(data['t'])     # could be integer or string          

            if m_type == '1':    # comment
                comment = data['cm']

                if comment.isdigit() and int(comment) <= 50:
                    # skip counting for 50
                    pass
                else:
                    comment = comment.replace('\n', ' ') # replace line break to a space
                    #print(self.room.name + ' ' + str(data['received_at']) + ': comment = ' + comment)
                    data['cm'] = comment
                    self.comment_log.append(data)
                    
                
            elif m_type == '2':    # gift
                pass
                
            elif m_type == '8':    # telop
                self.comment_log.append(data)
                if data['telop'] is not None:    # could be null
                    #print(self.room.name + ' ' + str(data['received_at']) + ': telop = ' + data['telop'])
                    pass             
                
            elif m_type == '11':    # cumulated gifts report
                pass                
            
            elif m_type == '101':    # indicating live finished
                self.comment_log.append(data)
                # close connection
                if ws is not None:
                    ws.close()
                    
            else:
                #print(data)
                #self.comment_log.append(data)
                pass
            
            
        # websocket callback
        def ws_on_error(ws, error):
            print(self.room.name + ' websocket error: ' + error)
        
        # websocket callback
        def ws_on_close(ws):
            print(self.room.name + ' websocket closed')
            self.ws_inter.cancel(self.room.name)
        
        # websocket callback
        def ws_on_open(ws):
            self.ws_startTime = int(time.time()*1000)
            print(self.room.name + ' websocket on open')

            # sending bcsvr_key so the server knows which room it is
            send_bcsvr_key()  
            
            # keep sending bcsvr_key to prevent disconnection
            self.ws_inter = setInterval(60, send_bcsvr_key)


            
        # get live info
        try:
            info = self.client.live_info(self.room.room_id) or []
        except HTTPError as e:
            # TODO: log/handle properly
            print('HTTP Error while getting live_info for {}: {}'.format(self.room.handle, e))
            return        
        
        #print(info)
        '''
        example from https://www.showroom-live.com/api/live/live_info?room_id=176314
        {'age_verification_status': 0, 
         'video_type': 0, 
         'enquete_gift_num': 0, 
         'is_enquete': False, 
         'bcsvr_port': 8080, 
         'live_type': 0, 
         'is_free_gift_only': False, 
         'bcsvr_host': 'online.showroom-live.com', 
         'live_id': 8355177, 
         'is_enquete_result': False, 
         'live_status': 2, 
         'room_name': 'ㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤㅤ', 
         'room_id': 176314, 
         'bcsvr_key': '7f7d69:YRpc2pNb', 
         'background_image_url': None}
        '''
        bcsvr_host = info['bcsvr_host']
        bcsvr_port = info['bcsvr_port']
        bcsvr_key = info['bcsvr_key']
        self.ws_send_txt = 'SUB\t' + bcsvr_key  # not sure why, but this is the text to send
        
#        print(bcsvr_host)
#        print(bcsvr_port)
#        print(bcsvr_key)
#        print(send_txt)

      
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


        print("Recording comments for {}".format(self.room.name))


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


        websocket.enableTrace(False)    # disable outputs
        ws = websocket.WebSocketApp('ws://' + bcsvr_host + ':' + str(bcsvr_port),
                            on_message = ws_on_message,
                            on_error = ws_on_error,
                            on_close = ws_on_close)
        ws.on_open = ws_on_open
        self.ws = ws
        self.ws.run_forever()  
        

        self.comment_log = sorted(self.comment_log, key=lambda x: x['received_at'])


        with open(outfile, 'w', encoding='utf8') as outfp:
#            json.dump({"comment_log": sorted(self.comment_log, key=lambda x: x['created_at'], reverse=True)},
#                      outfp, indent=2, ensure_ascii=False)
            json.dump(self.comment_log, outfp, indent=2, ensure_ascii=False)
            
        # convert comments to danmaku (弾幕 / bullets) subtitle ass file
        assTxt = convert_comments_to_danmaku(self.ws_startTime, self.comment_log, 18, 'MS PGothic', '1A', 640, 360)
        # suggested inputs:
        # startTime = self.ws_startTime    # comments recording start time
        # commentList = self.comment_log
        # fontsize = 18
        # fontname = 'MS PGothic'
        # alpha = '1A'                     # transparency '00' to 'FF' (hex string)
        # width = 640                      # screen height
        # height = 360                     # screen width            

        with open(outfileAss, 'w', encoding='utf8') as outfpAss:
            outfpAss.write(assTxt)

            
            
    def join(self):
        if self.ws is not None:
            self.ws.close()
            
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
