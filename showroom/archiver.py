#!/usr/bin/env python3

# Checks downloaded files, compares across multiple archive folders
# only
import os
# TODO: Use Click instead? actually subparsers might be sufficient
import argparse
import json
import glob
from subprocess import check_output, run, CalledProcessError
from collections import namedtuple, OrderedDict
from math import floor
import datetime

VideoGroup = namedtuple('VideoGroup', ['name', 'streams'])

GOOD_HEIGHTS = (198, 360, 396, 720)
STREAM_FOUND = True
STREAM_NOT_FOUND = False


def probe_video(filename, stream='v', entries=()):
    try:
        results = check_output([
            "ffprobe",
            '-loglevel', '16',
            '-show_entries', 'stream={}'.format(','.join(entries)),
            '-select_streams', stream,
            '-i', filename,
            '-of', 'json'
        ],
            universal_newlines=True
        )
    except CalledProcessError:
        return None
    else:
        try:
            return json.loads(results)['streams']
        except IndexError:
            return None


def check_group(target_dir, prefix, target_ext='mp4'):
    # based on concat's resize_videos and generate_concat_files
    group_name = target_dir.strip('/').split('/')[-1]
    max_gap = 300.0
    max_overlap = 5.0

    oldcwd = os.getcwd()
    os.chdir(target_dir)
    files = sorted(glob.glob('*.{}'.format(target_ext)))

    logfile = os.path.join(oldcwd, '{}_{}_check.log'.format(prefix, target_dir.split('/')[-2]))

    def get_start_seconds(file):
        time_str = file.rsplit(' ', 1)[1].split('.')[0]
        hours, minutes = int(time_str[:2]), int(time_str[2:4])
        try:
            seconds = int(time_str[4:6])
        except IndexError:
            seconds = 0
        return float(hours * 60 * 60 + minutes * 60 + seconds)

    def get_start_hhmm(seconds):
        hours = seconds / (60 * 60)
        minutes = (hours - floor(hours)) * 60
        return '{:02d}{:02d}'.format(floor(hours), floor(minutes))

    member_dict = {}
    for file in files:
        # get duration by decoding:
        # $ ffmpeg -i input.webm -f null -
        # ...
        # frame=206723 fps=1390 q=-0.0 Lsize=N/A time=00:57:28.87 bitrate=N/A speed=23.2x
        member_name = file.rsplit(' ', 1)[0]
        if member_name not in member_dict:
            member_dict[member_name] = {'files': [], 'streams': []}

        new_video = {'start_time': get_start_seconds(file),
                     'file': {'name': file,
                              'size': os.path.getsize(file)}}

        probe_results = probe_video(file, stream="",
                                    entries=('codec_name', 'codec_type',
                                             'duration', 'height', 'avg_frame_rate', 'bit_rate', 'nb_frames'))
        if not probe_results:
            new_video['valid'] = False
        else:
            temp = {}
            try:
                for e in probe_results:
                    stream_type = e.pop('codec_type')
                    temp[stream_type] = e
                v_info, a_info = temp['video'], temp['audio']
            except KeyError as e:
                with open(logfile, 'a', encoding='utf8') as outfp:
                    print('Probe of {} failed'.format(file), e, file=outfp)
                new_video['valid'] = False
            else:
                new_video['file'] = {'name': file,
                                     'size': os.path.getsize(file)}
                new_video['video'] = {'duration': float(v_info.get('duration', 0)),
                                      'height':   int(v_info.get('height', 0)),
                                      'avg_frame_rate': v_info.get('avg_frame_rate', ""),
                                      'bit_rate': int(v_info.get('bit_rate', 0)),
                                      'frames': int(v_info.get('nb_frames', 0))}
                new_video['audio'] = {'duration': float(a_info.get('duration', 0)),
                                      'bit_rate': int(a_info.get('bit_rate', 0))}
                if (new_video['video']['duration'] >= 0.001
                        and (new_video['video']['height'] in GOOD_HEIGHTS)):
                    new_video['valid'] = True
                else:
                    with open(logfile, 'a', encoding='utf8') as outfp:
                        print('{} is invalid'.format(file), file=outfp)
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
        results.append(VideoGroup(*check_group(group_dir, prefix)))
    return results


def format_results(results):
    formatted_results = []
    for group in results:
        streams = []
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
            streams.append((stream_name, OrderedDict((('files', files),
                                                      ('start_time', s['start_time']),
                                                      ('end_time', s['end_time']),
                                                      ('heights', sorted(s['heights'])),
                                                      ('total_size', s['total_size']),
                                                      ('total_frames', s['total_frames']),
                                                      ('total_duration', s['total_duration']),
                                                      ('total_gaps', s['total_gaps'])
                                                      ))))
        formatted_results.append((group.name, OrderedDict(streams)))
    return OrderedDict(formatted_results)


def analyse_dirs(*, dirs=(), output_dir='.', prefix='main'):
    for target_dir in dirs:
        results = format_results(check(target_dir, prefix))
        os.makedirs(output_dir, exist_ok=True)

        date = target_dir.rsplit('/', 1)[1]
        outfile = os.path.join(output_dir, "{}_{}_check.json".format(prefix, date))

        with open(outfile, 'w', encoding='utf8') as outfp:
            json.dump(results, outfp, ensure_ascii=False, indent=2)


def compare_archives(main_file, add_files, with_web=False):
    # The idea is to compare the main file to any additional files,
    # and/or against sr.gutas.net, and print a human readable summary of
    # files in need of replacing/repair in the main archive
    # for now, mimics the process I use when checking by hand
    # TODO: read room_ids from the index and associate them to streams

    # Formatting results is only necessary before printing them out to file
    with open(main_file, encoding='utf8') as infp:
        main_data = json.load(infp)
    # temp = []
    # for group in main_data.keys():
    #     temp.append(VideoGroup(group, main_data[group]))
    # main_data = format_results(temp)

    add_data = {}
    for add_file in add_files:
        prefix = add_file.rsplit('/', 1)[1].split('_', 1)[0]
        with open(add_file, encoding='utf8') as infp:
            add_data[prefix] = json.load(infp)

    if 'main' in add_data:
        print('Using non-main check results as base')

    compare_results = {}
    for group_name in main_data:
        main_group = main_data[group_name]
        compare_results[group_name] = []
        for prefix in add_data:
            try:
                add_group = add_data[prefix][group_name]
            except KeyError:
                print('{} was not found in {}'.format(group_name, prefix))
                continue

            # identify related streams in both sources
            main_streams = sorted((stream, main_group[stream], STREAM_NOT_FOUND) for stream in main_group)
            add_streams = sorted((stream, add_group[stream], STREAM_NOT_FOUND) for stream in add_group)

            len_main = len(main_streams)
            len_add = len(add_streams)
            if len_main != len_add:
                print('Streams in base: {}   Streams in {}: {}'.format(len_main, prefix, len_add))

            main_index = add_index = 0
            while True:
                main_member, main_time = main_streams[main_index].rsplit(' ', 1)
                add_member, add_time = add_streams[add_index].rsplit(' ', 1)

                # this logic will only work when there are but two sources to compare between
                preferred = 'main'
                # This assumes both sources use the same index
                # If not there will be problems, hence the need for room_ids
                if main_member == add_member:
                    # within 1 minute of each other
                    # there really shouldn't be more than a minute difference unless one or the other failed
                    if -1 <= int(main_time) - int(add_time) <= 1:
                        if int(add_time) > int(main_time):
                            preferred = prefix
                    # within 5 minutes of each other
                    elif -5 <= int(main_time) - int(add_time) <= 5:
                        if int(add_time) > int(main_time):
                            preferred = prefix
                    # in the same hour
                    elif -100 <= int(main_time) - int(add_time) <= 100:
                        pass
                    # fully separate streams, or failed recording on one end
                    else:
                        pass


def build_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='command_name')

    parser_check = subparsers.add_parser('check', help='Check directories, saving results to a file')
    parser_check.add_argument('dirs', nargs='+', help='Directories to check')
    parser_check.add_argument('--prefix', type=str, help='Archive name to prefix before check file', default='main')
    parser_check.add_argument('--output-dir', '-o', help='Output directory for results', default='.')
    parser_check.set_defaults(command=analyse_dirs)

    parser_compare = subparsers.add_parser('compare', help='Compare check results, needs either 2+ files, or '
                                           'the --with-web switch to compare against sr.gutas.net, '
                                           'or both. NOT IMPLEMENTED')
    parser_compare.add_argument('main_file', help='Main archive check results')
    parser_compare.add_argument('add_files', nargs='*', help='Files to compare against')
    parser_compare.add_argument('--with-web', action='store_true', help='Also check against sr.gutas.net')
    parser_compare.set_defaults(command=compare_archives)

    return parser


def main():
    main_args = ('command', 'command_name')

    parser = build_parser()
    args = parser.parse_args()
    kwargs = {k: v for k, v in vars(args).items() if k not in main_args}
    args.command(**kwargs)


if __name__ == "__main__":
    main()
