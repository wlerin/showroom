import os
import glob
import json  # TODO: use social48.json
from math import floor
from collections import OrderedDict

from .probe import probe_video
from .constants import GOOD_HEIGHTS, ENGLISH_INDEX
from .models import VideoGroup


# TODO: make this configurable without modifying source
GOOD_SUBFOLDERS = ['Draft']


def check_group(target_dir, prefix, target_ext='mp4'):
    # based on concat's resize_videos and generate_concat_files
    group_name = target_dir.strip('/').split('/')[-1]
    max_gap = 300.0
    max_overlap = 5.0

    oldcwd = os.getcwd()

    try:
        os.chdir(target_dir)
    except NotADirectoryError:
        return {}

    # we need to include subfolders for the final check
    # files = sorted(glob.glob('*.{}'.format(target_ext)))

    logfile = os.path.join(oldcwd, '{}_{}_check.log'.format(prefix, target_dir.split('/')[-2]))

    def get_start_seconds(file):
        time_str = file.rsplit(' ', 1)[1].split('.')[0]
        hours, minutes = int(time_str[:2]), int(time_str[2:4])
        try:
            seconds = int(time_str[4:6])
        except ValueError:
            seconds = 0
        return float(hours * 60 * 60 + minutes * 60 + seconds)

    def get_start_hhmm(seconds):
        hours = seconds / (60 * 60)
        minutes = (hours - floor(hours)) * 60
        return '{:02d}{:02d}'.format(floor(hours), floor(minutes))

    member_dict = {}
    found_files = []
    for root, dirs, files in os.walk('.'):
        dirnames = dirs[:]
        for dirname in dirnames:
            if dirname not in GOOD_SUBFOLDERS:
                dirs.remove(dirname)
        for file in files:
            if file.endswith(target_ext):
                # trim the ./ , change this if the method above changes
                found_files.append(os.path.join(root, file)[2:])

    for file in found_files:
        # get duration by decoding:
        # $ ffmpeg -i input.webm -f null -
        # ...
        # frame=206723 fps=1390 q=-0.0 Lsize=N/A time=00:57:28.87 bitrate=N/A speed=23.2x
        filename = os.path.basename(file)
        member_name = filename.rsplit(' ', 1)[0]
        if member_name not in member_dict:
            member_dict[member_name] = {'files': [], 'streams': []}

        new_video = {'start_time': get_start_seconds(file),
                     'file': {'name': file,
                              'size': os.path.getsize(file)}}

        probe_results = probe_video(file, stream="",
                                    entries=('codec_name', 'codec_type',
                                             'duration', 'height', 'avg_frame_rate', 'bit_rate', 'nb_frames'))

        # ignore empty results (CalledProcessError or unreadable json)
        if probe_results:
            temp = {}
            try:
                for e in probe_results:
                    stream_type = e.pop('codec_type')
                    temp[stream_type] = e
                v_info, a_info = temp['video'], temp['audio']
            except KeyError as e:
                with open(logfile, 'a', encoding='utf8') as outfp:
                    print('Probe of {} failed'.format(filename), e, file=outfp)
                new_video['valid'] = False
            else:
                new_video['video'] = {'duration': float(v_info.get('duration', 0)),
                                      'height': int(v_info.get('height', 0)),
                                      'avg_frame_rate': v_info.get('avg_frame_rate', ""),
                                      'bit_rate': int(v_info.get('bit_rate', 0)),
                                      'frames': int(v_info.get('nb_frames', 0))}
                new_video['audio'] = {'duration': float(a_info.get('duration', 0)),
                                      'bit_rate': int(a_info.get('bit_rate', 0))}
                if (new_video['video']['duration'] >= 0.001
                    and (new_video['video']['height'] in GOOD_HEIGHTS or 'Kimi Dare' in member_name)):
                    new_video['valid'] = True
                else:
                    with open(logfile, 'a', encoding='utf8') as outfp:
                        print('{} is invalid'.format(file), file=outfp)
                    new_video['valid'] = False
        else:
            new_video['valid'] = False
        member_dict[member_name]['files'].append(new_video)

    # TODO: URGENT check validity
    # TODO: compare durations of audio and video streams
    def new_stream(member_name, first_video):
        stream_name = '{} {}'.format(member_name, get_start_hhmm(first_video['start_time']))

        info = {'files': [],
                'heights': set(),
                'start_time': first_video['start_time'],
                'end_time': first_video['start_time'],
                'total_size': 0,
                'total_frames': 0,
                'total_duration': 0,
                'total_gaps': 0.0}

        update_stream(info, first_video)
        return stream_name, info

    def update_stream(stream, new_file):
        info = stream
        info['files'].append(new_file)
        if new_file['valid']:
            info['heights'].add(new_file['video']['height'])
            gap = new_file['start_time'] - info['end_time']
            info['total_gaps'] += gap if gap > 0.01 else 0.0
            info['end_time'] = new_file['start_time'] + new_file['video']['duration']
            info['total_size'] += new_file['file']['size']
            info['total_frames'] += new_file['video']['frames']
            info['total_duration'] += new_file['video']['duration']

    stream_catalogue = {}
    for member_name in member_dict.keys():
        member = member_dict[member_name]
        member['files'].sort(key=lambda x: x['start_time'])
        try:
            stream_name, working = new_stream(member_name, member['files'][0])
        except IndexError:
            with open(logfile, 'a', encoding='utf8') as outfp:
                print('No files found for {}'.format(member_name), file=outfp)
            # add member to index anyway
            continue
        for item in member['files'][1:]:
            # new stream
            if item['start_time'] >= working['end_time'] + max_gap:
                stream_catalogue[stream_name] = working
                stream_name, working = new_stream(member_name, item)
            else:
                if item['start_time'] < working['end_time'] - max_overlap:
                    with open(logfile, 'a', encoding='utf8') as outfp:
                        print('{} overlaps {}'.format(item['file']['name'],
                                                      working['files'][-1]['file']['name']),
                              file=outfp)

                update_stream(working, item)
        stream_catalogue[stream_name] = working

    os.chdir(oldcwd)

    return group_name, stream_catalogue


def check(target, prefix):
    results = []
    for group_dir in glob.glob('{}/*'.format(target)):
        if os.path.isdir(group_dir):
            group_name, streams = check_group(group_dir, prefix)
            if streams:
                results.append(VideoGroup(group_name, streams))
    return results


def format_results(results, partial=False):
    formatted_results = OrderedDict((
        ("partial", partial),
        ("rooms", None)
    ))
    groups = []
    for group in results:
        rooms = {}
        for stream_name in sorted(group.streams.keys()):
            s = group.streams[stream_name]
            files = []
            for file in s['files']:
                f = []
                for key in ('file', 'video', 'audio'):
                    try:
                        f.append((key, OrderedDict(sorted(file[key].items()))))
                    except KeyError:
                        f.append((key, {}))
                f.append(('valid', file['valid']))
                files.append(OrderedDict(f))
            room = ENGLISH_INDEX.find_room(file_name=stream_name+'.mp4')
            if room:
                if room.room_id not in rooms:
                    rooms[room.room_id] = OrderedDict((('name', room.name),
                                                       ('group', room.group),
                                                       ('room_id', room.room_id),
                                                       ('priority', room.priority),
                                                       ('handle', room.handle),
                                                       ('total_duration', None),
                                                       ('streams', [])))
                rooms[room.room_id]['streams'].append(OrderedDict((('stream_name', stream_name),
                                                                   ('files', files),
                                                                   ('start_time', s['start_time']),
                                                                   ('end_time', s['end_time']),
                                                                   ('heights', sorted(s['heights'])),
                                                                   ('total_size', s['total_size']),
                                                                   ('total_frames', s['total_frames']),
                                                                   ('total_duration', s['total_duration']),
                                                                   ('total_gaps', s['total_gaps'])
                                                                   )))
            else:
                print('Failed to locate room matching {}'.format(stream_name))
        groups.append((group.name, OrderedDict(rooms)))
    formatted_results['groups'] = OrderedDict(groups)

    for group_name, group in formatted_results['groups'].items():
        for room_id, room in group.items():
            room['total_duration'] = sum((e['total_duration'] for e in room['streams']))

    return formatted_results


def check_dirs(*, dirs=(), output_dir='.', prefix='main', partial=False):
    for target_dir in dirs:
        results = format_results(check(target_dir, prefix), partial=partial)
        os.makedirs(output_dir, exist_ok=True)

        date = target_dir.rsplit('/', 1)[-1]
        outfile = os.path.join(output_dir, "{}_{}_check.json".format(prefix, date))

        with open(outfile, 'w', encoding='utf8') as outfp:
            json.dump(results, outfp, ensure_ascii=False, indent=2)

        # there should be no empty directories
        if not results['partial']:
            for group, rooms in results['groups'].items():
                if not rooms:
                    return False
        return True


def post_merge_check(check_file):
    with open(check_file, encoding='utf8') as infp:
        data = json.load(infp)

    results = []
    for group in data['groups']:
        for room in data['groups'][group].values():
            for stream in room['streams']:
                total_video_duration = sum(file['video']['duration'] for file in stream['files'] if file['valid'])
                total_audio_duration = sum(file['audio']['duration'] for file in stream['files'] if file['valid'])
                if not (-1.0 < total_video_duration - total_audio_duration < 1.0):
                    results.append((
                        stream['stream_name'], 
                        "audio: {}".format(total_audio_duration), 
                        "video: {}".format(total_video_duration),
                        "difference: {}".format(total_audio_duration - total_video_duration)
                        ))

    with open(check_file.replace('_check.json', '_desync.log'), 'w', encoding='utf8') as outfp:
        for result in results:
            print(*result, sep='\n', end='\n\n', file=outfp)


def check_final(*, dirs=(), output_dir='.'):
    if isinstance(dirs, str):
        dirs = (dirs,)
    check_dirs(dirs=dirs, output_dir=output_dir, prefix='final')
    for target in dirs:
        name = target.rsplit('/', 1)[-1]
        outfile = os.path.join(output_dir, "final_{}_check.json".format(name))
        post_merge_check(outfile)

    
     