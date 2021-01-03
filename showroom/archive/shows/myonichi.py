import os
import json
import re
import shutil
from collections import OrderedDict as od
from datetime import datetime

from bs4 import BeautifulSoup as Soup

from showroom.api.session import ClientSession as Session
from showroom.settings import ShowroomSettings
from showroom.archive.constants import JAPANESE_INDEX as JI

myonichi_re = re.compile(r'(?P<month>\d{1,2})月(?P<day>\d{1,2})日\((?P<weekday>\w)\)\s?(?P<name>[\w ]+)\s?[（\(](?P<team>[\wー ]+)[）\)]')
file_re = re.compile(r'(?P<date>\d{6}) Showroom - SP AKB48 no Myonichi Yoroshiku! (?P<time>\d{4,6}).mp4$')
text = ""  # text from here: https://www.showroom-live.com/room/profile?room_id=92289

settings = ShowroomSettings.from_file()
os.environ.update(settings.environment)

# this is such an awful design
def update(data, datapath):
    myou = data
    old_names = {e['date']: e['jpnName'] for e in myou.values()}

    s = Session()
    r = s.get('https://www.showroom-live.com/room/profile?room_id=92289')

    soup = Soup(r.text, 'lxml')

    text = soup.select_one('div#js-room-profile-detail > ul.room-profile-info').select('> li')[1].p.text
    text = re.sub(r'\(\(', '(', text)
    lines = [e for e in text.split('\n') if e.strip()]

    curr_month = None
    curr_year = datetime.now().year
    for line in lines:
        m = myonichi_re.match(line.strip())
        if not m:
            # print(line)
            continue

        m = m.groupdict()
        month, day = int(m['month']), int(m['day'])
        if month != curr_month:
            if curr_month is not None and month > curr_month:
                # print(curr_year, curr_month, month, day, line)
                curr_year -= 1
                curr_month = month
            else:
                curr_month = month


        date = '{:04d}-{:02d}-{:02d}'.format(curr_year, month, day)

        if ' ' in m['team']:
            group, team = m['team'].split(' ', 1)
        elif len(m['team']) > 5 and '48' in m['team']:
            ri = m['team'].index('48') + len('48')
            group, team = m['team'][:ri].strip(), m['team'][ri:].strip()
        else:
            group, team = m['team'], ""
        myou[date] = dict(
            date=date,
            jpnName=m['name'].strip(),
            jpnGroup=group,
            jpnTeam=team,
            weekday=m['weekday']
        )
    # TODO: sort it in the other direction

    for date, val in myou.items():
        if val['jpnName'] == '張織 慧':
            val['jpnName'] = '張 織慧'
        room = JI.find_room(name=val['jpnName'])
        if not room:
            # this allows me to manually change the jpnName to fix inconsistencies in the source
            if date in old_names:
                alt_name = old_names[date]
                room = JI.find_room(name=alt_name)
                if room: 
                    val['jpnName'] = alt_name
        if room:
            # This bypasses the Room/RoomOld translation and looks directly at the underlying data
            # Which means Team will be wrong, except when it's right
            val['engName'] = room['engName']
            val['engTeam'] = room['engTeam']
            try:
                val['engGroup'] = room['engGroup']
            except KeyError:
                val['engGroup'], val['engTeam'] = val['engTeam'].split(' ', 1)
            val['roomId'] = room.room_id
        else:
            print('Couldn\'t find {} in index'.format(val['jpnName']))
            val['engName'] = "Unknown"

    # outfile = 'myounichi_episodes.json'
    # outdir = '/home/wlerin/Annex/Showroom'  # TODO: read from settings
    # outpath = '/'.join([outdir, outfile])

    dates = sorted(myou.keys())

    data = od()
    for date in dates:
        data[date] = myou[date]

    with open(datapath, 'w', encoding='utf8') as outfp:
        json.dump(data, outfp, indent=2, ensure_ascii=False)

    return data


def rename(srcpath, destpath, data):
    import glob
    import os

    episodes = sorted(data.keys())
    name_pattern = '{date} Showroom - AKB48 no Myonichi Yoroshiku! #{ep} ({name}).mp4'
    long_date_pattern = '20{}-{}-{}'

    for file in sorted(glob.glob('{}/*.mp4'.format(srcpath))):
        match = file_re.match(os.path.basename(file))
        date = match.groupdict()['date']
        long_date = long_date_pattern.format(*[date[i:i+2] for i in range(0, 6, 2)])
        new_file = name_pattern.format(
            date=date,
            ep=episodes.index(long_date)+1,
            name=data[long_date]['engName'],
        )
        shutil.move(
            file,
            '{}/{}'.format(destpath, new_file)
        )

