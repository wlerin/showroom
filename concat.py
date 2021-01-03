#!/usr/bin/env python3

"""
python3 concat.py [-h] [--generate] [--merge] [--both] [--max-gap MAX_GAP]
                 [-e EXT]
                 [TARGET_DIR]

Generates concat files for merging. Creates separate videos for separate
broadcasts and incompatible resolutions, ignores very broken videos.

positional arguments:
  TARGET_DIR         Optional. defaults to the current working directory.

optional arguments:
  -h, --help         show this help message and exit
  --generate         generates concat files in TARGET_DIR, runs by default
  --merge            merges videos in TARGET_DIR according to existing concat
                     files
  --both             both generates concat files and merges videos
  --max-gap MAX_GAP  maximum gap between merged videos, in seconds. anything
                     larger is treated as a separate broadcast
  -e EXT             extension to merge, defaults to mp4

When merging, watch the output for "Non-monotonous DTS in output stream" -- A
few of these are harmless but a wall of them means that video is probably
corrupted.



scene detection with ffprobe
https://lists.ffmpeg.org/pipermail/ffmpeg-user/2012-November/011101.html

ffprobe -show_frames -of compact=p=0 -f lavfi \
"movie=$F,select=gt(scene\,.8)" | gsed -r \
's/.*pkt_pts_time=([0-9.]{8,})\|.*/\1/' >> scenes-0.8

that doesn't work though.


https://pypi.python.org/pypi/PySceneDetect/
https://pyscenedetect.readthedocs.io/en/latest/features/
"""

import os
import glob
import json
import shutil
import argparse
from math import floor
from subprocess import check_output, run, CalledProcessError

from showroom.settings import settings as config

# known resolutions:
# 352x198
# 640x360
# 704x396
# 960x540 (march kimi dare episodes)
# 1280x720 (a single kimi dare episode)
# 1920x1080 (ann vr)
# GOOD_HEIGHTS = (180, 198, 270, 360, 396, 720, 1080)
BAD_HEIGHTS = (540,)

# bitrate for upscaled videos, this is actually a bit too high
DEF_BITRATE = '300k'

# old version
"""
def create_concat_files(target_dir, target_ext):
    oldcwd = os.getcwd()
    os.chdir(target_dir)


    # TODO: use ffprobe to separate files with incompatible resolutions and those with a gap greater than ~10 minutes

    files = sorted(glob.glob('{}/*.{}'.format(target_dir, target_ext)))

    member_dict = {}
    for file in files:
        member_name = file.rsplit(' ', 1)[0]
        if member_name not in member_dict:
            member_dict[member_name] = []
        member_dict[member_name].append(file)

    concat_files = {}
    for key in member_dict.keys():
        filename = key +' ' + member_dict[key][0].rsplit(' ', 1)[1][:4] + '.mp4.concat'
        text = ""
        for item in member_dict[key]:
            text += "file '" + item + "'\n"
        concat_files.update({filename:text})

    for key in concat_files.keys():
        with open(key, 'w', encoding='utf8') as outfp:
            _ = outfp.write(concat_files[key])

    os.chdir(oldcwd)
"""

"""
{
    "member1" : [
        video1,
        video2,
        video3,
        video4
    ],
    "member2" : [

    ]
}

video: {
    "start_time" : parsed from file name; in seconds,
    "duration"   : parsed from ffprobe,
    "height"     : parsed from ffprobe,
    "valid"      : true or false (false for stuff with no video content),
    "file"       : location of file
}
"duration"
"height"

sample ffprobe output:
{
    "programs": [

    ],
    "streams": [
        {
            "height": 198,
            "duration": "499.654000"
        }
    ]
}
for member in members:


"""

# TODO: set this in some other module, perhaps constants
if os.name == 'nt':
    _iswin32 = True
else:
    _iswin32 = False

_ffmpeg = config.ffmpeg.path
_ffprobe = os.path.join(os.path.split(_ffmpeg)[0], 'ffprobe')


def probe_file(filename):
    if _iswin32:
        extra_args = dict(shell=True)
    else:
        extra_args = dict()

    # So, I need to get both audio and video stream data
    # Simplest way to do that is to fetch all the streams
    # and map the audio stream to an audio key and the video to a video key etc.
    try:
        data = check_output(
            [
                _ffprobe,
                '-loglevel', '16',
                # '-show_entries', 'stream={}'.format(','.join(streams)),
                # '-select_streams', 'v,a',
                '-show_streams',
                '-i', filename,
                '-of', 'json'
            ],
            universal_newlines=True,
            **extra_args
        )
    except CalledProcessError:
        return None
    try:
        streams = json.loads(data)['streams']
    except KeyError:
        # TODO: log this
        return None

    results = {}

    for stream in streams:
        if stream['codec_type'] == 'video':
            if 'video' in results:
                # TODO: log this
                print('Found multiple video streams in {}, ignoring extra stream info'.format(filename))
            else:
                results['video'] = stream
        elif stream['codec_type'] == 'audio':
            if 'audio' in results:
                print('Found multiple audio streams in {}, ignoring extra stream info'.format(filename))
            else:
                results['audio'] = stream
        else:
            print('Found unknown stream type in {}: {}'.format(filename, stream['codec_type']))
    if len(results) == 1:
        print('Found only one stream in', filename)
        print(json.dumps(results, indent=2))
    return results


def get_source_videos(target_ext):
    # TODO: properly support ts -> mp4 conversions
    # going from ts -> mp4 requires more logic than this (in particular, need to check video and audio codecs)
    # also, completed files should be excluded from this, no?
    files = sorted(glob.glob('*.{}'.format(target_ext)))
    if target_ext == 'mp4' and len(files) == 0:
        # kludge to support ts source files
        files = sorted(glob.glob('*.{}'.format('ts')))
    return files


def resize_videos(target_dir, target_ext, copytb=1, target_bitrate='300k'):
    # TODO: scale up to the tallest video in a "stream"
    oldcwd = os.getcwd()
    os.chdir(target_dir)
    files = get_source_videos(target_ext)

    members = set()
    to_resize = []

    for file in files:
        results = probe_file(file)
        if results:
            if float(results['video']['duration']) >= 0.001 and int(results['video']['height']) == 198:
                to_resize.append(file)

    if len(to_resize) > 0:
        os.makedirs('resized', exist_ok=True)
    else:
        os.chdir(oldcwd)
        return

    codecs = {'mp4': 'libx264', 'webm': 'libvpx'}

    video_codec = codecs[target_ext]

    # the concat demuxer is not sufficient to merge files resized this way
    for file in to_resize:
        low_res_file = 'resized/' + file.replace('.' + target_ext, '_198p.' + target_ext)
        shutil.move(file, low_res_file)
        members.add(file.rsplit(' ', 1)[0])
        run([_ffmpeg,
             '-copytb', str(copytb),
             '-hide_banner', '-nostats',
             '-i', low_res_file,
             '-c:v', video_codec,
             # '-maxrate', str(target_bitrate),
             # '-bufsize', BUFSIZE,
             # '-crf', '18',
             '-vsync', '0',  # leave timestamps unchanged
             '-refs', '1',  # single reference frame, like the original
             '-copyts',
             '-b:v', target_bitrate,
             '-vf', 'scale=-1:360',  # 'scale=-1:360,mpdecimate',
             '-c:a', 'copy', file])

    with open('resized.json', 'w', encoding='utf8') as outfp:
        json.dump(sorted(members), outfp, indent=2, ensure_ascii=False)

    os.chdir(oldcwd)


def generate_concat_files(target_dir, target_ext, max_gap):
    oldcwd = os.getcwd()
    os.chdir(target_dir)

    max_gap = float(max_gap)

    try:
        with open('resized.json', encoding='utf8') as infp:
            resized_members = tuple(json.load(infp))
    except FileNotFoundError:
        resized_members = ()

    # TODO: deal with leftovers (from after 24:00)
    files = get_source_videos(target_ext)

    def get_start_seconds(file):
        time_str = file.rsplit(' ', 1)[1].split('.')[0]
        hours, minutes, seconds = int(time_str[:2]), int(time_str[2:4]), int(time_str[4:6])
        return float(hours * 60 * 60 + minutes * 60 + seconds)

    def get_start_hhmm(seconds):
        hours = seconds / (60 * 60)
        minutes = (hours - floor(hours)) * 60
        return '{:02d}{:02d}'.format(floor(hours), floor(minutes))

    member_dict = {}
    for file in files:
        streams = probe_file(file)
        if not streams:
            continue

        member_name = file.rsplit(' ', 1)[0]
        if member_name not in member_dict:
            member_dict[member_name] = []

        new_video = {"start_time": get_start_seconds(file)}

        # try:
        #     stream = json.loads(results)['streams'][0]
        # except IndexError:
        #     new_video['valid']  = False
        #     print('failed to load ffprobe results')
        #     print(results)
        # else:
        new_video['file'] = file
        new_video['duration'] = float(streams['video']['duration'])
        new_video['bit_rate'] = int(streams['video']['bit_rate'])
        new_video['height'] = int(streams['video']['height'])
        new_video['audio_sample_rate'] = int(streams['audio']['sample_rate'])
        if new_video['duration'] >= 0.001:
            if new_video['height'] in BAD_HEIGHTS and new_video['duration'] < 90 and new_video['bit_rate'] < 10000:
                new_video['valid'] = False
            else:
                new_video['valid'] = True
        else:
            new_video['valid'] = False

        if new_video['valid']:
            member_dict[member_name].append(new_video)

    concat_files = {}

    def new_concat_file(member, first_video):
        # decide between .proto and .concat based on presence of member_name in resized.json
        if member in resized_members:
            info_ext = 'proto'
        else:
            info_ext = 'concat'
        filename = '{} {}.{}.{}'.format(member, get_start_hhmm(first_video['start_time']), target_ext, info_ext)
        info = {'files': []}
        info['height'] = first_video['height']
        info['audio_sample_rate'] = first_video['audio_sample_rate']
        info['last_time'] = first_video['start_time'] + first_video['duration']
        info['files'].append(first_video['file'])
        return filename, info

    for member in member_dict.keys():
        """
        file_specifier (name + hhmm) : {
            height : 360 or 198,
            last_time : start_time + duration of most recently processed video
            files: [
                list of files
            ]
        }
        """
        try:
            filename, working = new_concat_file(member, member_dict[member][0])
        except IndexError:
            # no valid videos
            print('Failed to read videos for {}'.format(member))
            print(member_dict)
            continue
        for item in member_dict[member][1:]:
            if (item['start_time'] >= working['last_time'] + max_gap
                    or item['height'] != working['height']
                    or item['audio_sample_rate'] != working['audio_sample_rate']):
                if filename in working:
                    # This needs to be dealt with by hand for now
                    print('Tried to add duplicate concat file name: {}'.format(filename))
                    raise FileExistsError
                concat_files[filename] = working
                filename, working = new_concat_file(member, item)
            else:
                if item['start_time'] < working['last_time'] - 5.0:
                    print('{} overlaps {}'.format(item['file'], working['files'][-1]))
                    # these have to be dealt with manually
                working['files'].append(item['file'])
                working['last_time'] = item['start_time'] + item['duration']
        concat_files[filename] = working

    for file in concat_files.keys():
        # skip singleton videos
        # if len(concat_files[file]['files']) == 1:
        #    continue
        text = ""
        for item in concat_files[file]['files']:
            text += "file '" + item + "'\n"
        with open(file, 'w', encoding='utf8') as outfp:
            outfp.write(text)

    os.chdir(oldcwd)


"""
#!/bin/bash

# for f in ./*.mp4; do echo "file '$f'" >> mylist.txt; done
# for f in ./*.concat; do echo "$( basename $f )"; done
# for f in ./*.concat; do g="\"$( basename "$f" .mp4)\""; echo $f; echo $g; done
# echo "\"$( basename ./160612\ Showroom\ -\ AKB48\ Team\ K\ Tano\ Yuka\ 124028.mp4 .mp4)\""
for f in ./*.concat; do
    g=$( basename "$f" .concat);
    #ffmpeg -copytb 1 -f concat -i "$f" -vf "pad=width=640:height=360:(ow-iw)/2:(oh-ih)/2:color=black" -movflags +faststart "$g";
    ffmpeg -copytb 1 -f concat -i "$f" -movflags +faststart -c copy "$g";
done
"""


def merge_videos(target_dir, output_dir, copyts=False, copytb=1):
    oldcwd = os.getcwd()
    os.chdir(target_dir)

    os.makedirs(output_dir, exist_ok=True)
    bTempFiles = False

    for ext in ('concat', 'proto'):
        for concat_file in glob.glob('*.' + ext):
            outfile = '{}/{}'.format(output_dir, os.path.splitext(concat_file)[0])
            instructions = ['-hide_banner', '-nostats',
                            # '-report',
                            # 'file=logs/concat-{}.log:level=40'.format(os.path.splitext(concat_file)[0]),
                            '-copytb', str(copytb)]
            with open(concat_file, encoding='utf8') as infp:
                data = infp.read()
            if data.count('file \'') == 0:
                print("Empty concat file: {}".format(concat_file))
                continue
            if data.count('file \'') == 1:
                src = data[5:].strip('\'\n./')
                instructions.extend(['-i', src])
            elif ext == 'concat':
                instructions.extend(['-auto_convert', '1', '-f', 'concat', '-safe', '0', '-i', concat_file])
                # ts source kludge
                with open(concat_file, encoding='utf8') as infp:
                    for line in concat_file:
                        if line.strip().endswith('.ts\''):
                            instructions.extend(['-bsf:a', 'aac_adtstoasc'])
                            break
                        else:
                            break
            else:
                os.makedirs('temp', exist_ok=True)
                src_videos = []

                for line in data.split('\n'):
                    if line.strip():
                        src_videos.append(line.strip()[6:-1])  # skip blank lines

                bTempFiles = True
                temp_videos = []
                for video in src_videos:
                    tempfile = 'temp/' + video + '.ts'
                    run([_ffmpeg,
                         '-i', video,
                         '-c', 'copy',
                         '-bsf:v', 'h264_mp4toannexb',
                         '-f', 'mpegts',
                         tempfile])
                    temp_videos.append(tempfile)
                videostring = 'concat:' + '|'.join(temp_videos)

                instructions.extend(['-i', videostring, '-bsf:a', 'aac_adtstoasc'])

            if copyts:
                instructions.append('-copyts')

            run([_ffmpeg,
                 *instructions,
                 '-movflags', '+faststart',
                 '-c', 'copy', outfile])

            if bTempFiles:
                for tempfile in glob.glob('temp/*.ts'):
                    os.remove(tempfile)
                bTempFiles = False

    os.chdir(oldcwd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generates concat files for merging. Creates separate videos for separate broadcasts and \
        incompatible resolutions, ignores very broken videos.",
        epilog="When merging, watch the output for \"Non-monotonous DTS in output stream\" -- A few of these are \
        harmless but a wall of them means that video is probably corrupted.")
    parser.add_argument("--resize", action='store_true',
                        help='!!EXPERIMENTAL!! resizes 198p videos in TARGET_DIR to 360p, '
                             'saves the old videos in a new "resized" subdirectory. Only supports h264 (MP4) and vpx (WEBM)')
    parser.add_argument("--generate", action='store_true', help='generates concat files in TARGET_DIR, runs by default')
    parser.add_argument("--merge", action='store_true',
                        help='merges videos in TARGET_DIR according to existing concat files')
    parser.add_argument("--both", action='store_true', help='both generates concat files and merges videos')
    parser.add_argument("--aggressive", action='store_true', help='!!EXPERIMENTAL!! resizes, generates, and merges')
    parser.add_argument("target_dir", nargs='?', default='.',
                        help='Optional. defaults to the current working directory.', metavar='TARGET_DIR')
    parser.add_argument("--max-gap", type=float, default=600.0,
                        help='maximum gap between merged videos, in seconds. anything larger is treated as a separate \
                        broadcast. default = 600.0')
    parser.add_argument("-e", dest='ext', default='mp4', help='extension to merge, defaults to mp4')
    parser.add_argument("--copytb", type=int, choices=[-1, 0, 1], default=1,
                        help='it may be useful to try setting this to 0 or -1 if a video has timing issues.'
                             'Defaults to %(default)s')
    parser.add_argument('--copyts', action='store_true', help='Try setting this if there\'s a lot of DTS adjustment. '
                                                              'Only affects merges.')
    parser.add_argument("--output-dir", "-o", dest='output_dir', type=str, default='.',
                        help='Optional, defaults to target directory. Note that relative paths will be relative to \
                        the target directory, not the current working directory', metavar='OUTPUT_DIR')
    parser.add_argument("--bitrate", "-b", type=str, default=DEF_BITRATE,
                        help='Bitrate for resizing. Defaults to %(default)s')
    parser.add_argument("--use-concat-protocol", action="store_true",
                        help="!!EXPERIMENTAL!! Uses ffmpeg's concat protocol"
                             " instead of the concat demuxer to allow merging videos with differing timebases (as result from"
                             " --resize). Creates temporary intermediate .ts files. Used automatically with --aggressive")
    args = parser.parse_args()

    if args.resize or args.aggressive:
        resize_videos(target_dir=args.target_dir, target_ext=args.ext,
                      copytb=args.copytb, target_bitrate=args.bitrate)
    if args.generate or args.both or args.aggressive:
        generate_concat_files(target_dir=args.target_dir, target_ext=args.ext,
                              max_gap=args.max_gap)
    if (args.merge or args.both) and not args.use_concat_protocol:
        merge_videos(target_dir=args.target_dir, output_dir=args.output_dir, copyts=args.copyts,
                     copytb=args.copytb)
    if args.aggressive or ((args.merge or args.both) and args.use_concat_protocol):
        merge_videos(target_dir=args.target_dir, output_dir=args.output_dir, copyts=args.copyts,
                     copytb=args.copytb)

# 2017-02-02
# Making Aggressive Concat saner

# resize creates a json
# this lists all the names that have been resized. it kind of matters which videos if there was more than one broadcast in a given day,
# but i'm not going to worry about that right now
# 