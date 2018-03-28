from showroom.archive import trim
# from showroom.settings import settings
import json
import re
import glob
import os
from showroom.utils import iso_date_to_six_char
# data_dir = settings.directory.data
# default_work_dir = '/'.join([data_dir, 'Kimi Dare/working'])

episode_list_path = '/'.join([
    # data_dir,
    'kimi_dare_episodes.json'])
episode_list_url = 'https://pastebin.com/raw/J9xEdxHE'

# episode update regexes
_week_re = re.compile(r'\-\- Week (\d+) \-\-')
_episode_info_re = re.compile(r'''
    (?P<date>\d{4}\-\d{2}\-\d{2})
    \s
    \((?P<day_of_week>\w+)\)
    \s
    Kimi\ Dare\ Episode
    \s 
    \#(?P<episode_no>\d+) 
''', re.VERBOSE)


def update_episode_list(data):
    import requests
    r = requests.get(episode_list_url)

    line_no = 0
    week_no = 0

    episode = {}

    def add_episode(episode, data):
        if 'date' in episode:
            data['episodes'][iso_date_to_six_char(episode['date'])] = episode
        else:
            data['episodes']['unknown_date'].append(episode)

    for line in r.text.splitlines(keepends=False):
        # 5 cases
        line_no += 1
        line = line.strip()

        if line.startswith('--'):
            match = _week_re.match(line)
            if match:
                week_no = match.groups()[0]
            elif 'Christmas Break' in line:
                week_no = "Christmas"
            else:
                print('False Week Detected on line {}'.format(line_no))
            continue
        # elif line == '19:00':
        #     continue
        # elif line.startswith('MC'):
        #     continue
        elif ':' in line:
            continue
        elif not line:
            if episode:
                add_episode(episode, data)
                episode = {}
        elif line[0].isdigit():
            if "Break" in line:
                continue
            match = _episode_info_re.match(line)
            if match:
                episode['date'], episode['day_of_week'], episode['number'] = match.groups()
                episode['week'] = week_no
            else:
                print('Failed to read line {}'.format(line_no))
        else:
            if '(' in line:
                line = line.split('(')[0].strip()
            episode['members'] = line.split(', ')

    if episode:
        add_episode(episode, data)

    with open(episode_list_path, 'w', encoding='utf8') as outfp:
        json.dump(data, outfp, indent=2, ensure_ascii=False)


def load_episode_list():
    try:
        with open(episode_list_path, encoding='utf8') as infp:
            data = json.load(infp)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {"episodes": {'unknown_date': []}}

    return data


def get_trim_pts(srcpath):
    max_pts_time = trim.detect_first_scene(srcpath, threshold=10.0)
    return trim.detect_start_iframe(srcpath, max_pts_time)  # .get("pkt_pts_time", 0.0)


def trim_episodes(work_dir, dest_dir, data):
    files = glob.glob('/'.join([work_dir, '/*SP Kimi Dare*.mp4']))

    for srcpath in files:
        srcfile = os.path.basename(srcpath)
        date = srcfile[:6]
        episode = data['episodes'].get(date)
        if not episode:
            print('No episode information for {}'.format(srcfile))
            continue
        destfile = f'{date} Showroom - AKB48 no Kimi Dare #{episode["number"]} ({", ".join(episode["members"])}).mp4'
        destpath = '/'.join([dest_dir, destfile])
        normsrcpath = os.path.normpath(srcpath)
        normdestpath = os.path.normpath(destpath)
        # print(normsrcpath, normdestpath)

        print('Checking {}'.format(srcfile))
        start_time = get_trim_pts(normsrcpath)

        # TODO: detect end of audio
        print('Trimming {}'.format(srcfile))
        trim.trim_video(normsrcpath, normdestpath, start_time)


def kimi_dare_dispatch(**kwargs):
    data = load_episode_list()
    if kwargs.get('update'):
        update_episode_list(data)

    trim_dir = kwargs.get('trim_dir')
    if trim_dir:
        if os.path.isdir(trim_dir):
            dest_dir = kwargs.get('output_dir') or trim_dir
            trim_episodes(trim_dir, dest_dir, data)
        else:
            print('{} is not a directory'.format(trim_dir))
            return


