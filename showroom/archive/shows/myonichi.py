import re
import json
# from showroom.settings import ShowroomSettings
from showroom.archive.constants import JAPANESE_INDEX as JI
from showroom.session import WatchSession as Session
from bs4 import BeautifulSoup as Soup
from collections import OrderedDict as od 

myonichi_re = re.compile(r'(?P<month>\d{1,2})月(?P<day>\d{1,2})日\((?P<weekday>\w)\) (?P<name>[\w ]+)（(?P<team>[\wー ]+)[）\)]')
file_re = re.compile(r'(?P<date>\d{6}) Showroom - SP AKB48 no Myonichi Yoroshiku! (?P<time>\d{4,6}).mp4$')
text = ""  # text from here: https://www.showroom-live.com/room/profile?room_id=92289

# settings = ShowroomSettings()



def update(data):
    myou = data

    s = Session()
    r = s.get('https://www.showroom-live.com/room/profile?room_id=92289')
    soup = Soup(r.text, 'lxml')

    text = soup.select_one('div#js-room-profile-detail > ul.room-profile-info').select('> li')[1].p.text
    lines = [e for e in text.split('\n') if e.strip()]

    for line in lines:
        m = myonichi_re.match(line.strip())
        if not m:
            # print(line)
            continue
        m = m.groupdict()
        date = '2017-{:02d}-{:02d}'.format(int(m['month']),
                           int(m['day']))
        group, team = m['team'].split(' ', 1)
        myou[date] = dict(
            date=date,
            jpnName=m['name'],
            jpnGroup=group,
            jpnTeam=team,
            weekday=m['weekday']
        )
    # TODO: sort it in the other direction

    for date, val in myou.items():
        room = JI.find_room(name=val['jpnName'])
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
            val['engName'] = "Unknown"

    outfile = 'myounichi_episodes.json'
    outdir = '/home/wlerin/Annex/Showroom'  # TODO: read from settings
    outpath = '/'.join([outdir, outfile])

    dates = sorted(myou.keys())

    data = od()
    for date in dates:
        data[date] = myou[date]

    with open(outpath, 'w', encoding='utf8') as outfp:
        json.dump(data, outfp, indent=2, ensure_ascii=False)

    return data


def rename(srcpath, destpath, data):
    import glob
    import os

    episodes = sorted(data.keys())
    name_pattern = '{date} Showroom - AKB48 no Myonichi Yoroshiku! #{ep} ({name}).mp4'
    long_date_pattern = '20{}-{}-{}'

    for file in glob.glob('{}/*.mp4'.format(srcpath)):
        match = file_re.match(os.path.basename(file))
        date = match.groupdict()['date']
        long_date = long_date_pattern.format(*[date[i:i+2] for i in range(0, 6, 2)])
        new_file = name_pattern.format(
            date=date,
            ep=episodes.index(long_date)+1,
            name=data[long_date]['engName'],
        )
        os.replace(
            file,
            '{}/{}'.format(destpath, new_file)
        )

