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


"""

import os
import glob
import argparse
from subprocess import check_output, run, CalledProcessError
import json
from math import floor

# known resolutions:
# 352x198
# 640x360
# 704x396
# 1280x720 (a single kimi dare episode)
GOOD_HEIGHTS = (198, 360, 396, 720)

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

def probe_file(filename, streams):
    try:
        results = check_output([
            "ffprobe",
            '-loglevel', '16',
            '-show_entries', 'stream={}'.format(','.join(streams)),
            '-select_streams', 'v',
            '-i', filename,
            '-of', 'json'
        ],
            universal_newlines=True
        )
    except CalledProcessError:
        return None
    else:
        return results

def resize_videos(target_dir, target_ext, copytb=1, target_bitrate='300k'):

    oldcwd = os.getcwd()
    os.chdir(target_dir)
    files = sorted(glob.glob('*.{}'.format(target_ext)))


    to_resize=[]
    for file in files:
        results = probe_file(file, streams=('duration', 'height'))
        if results:
            try:
                stream = json.loads(results)['streams'][0]
            except IndexError:
                continue
            if float(stream['duration']) >= 0.001 and int(stream['height']) == 198:
                to_resize.append(file)

    if len(to_resize) > 0:
        os.makedirs('resized', exist_ok=True)
    else:
        return
    codecs = {'mp4': 'libx264', 'webm': 'libvpx'}

    video_codec = codecs[target_ext]

    for file in to_resize:
        low_res_file = 'resized/' + file.replace('.' + target_ext, '_198p.' + target_ext)
        os.replace(file, low_res_file)
        run(['ffmpeg',
             '-copytb', str(copytb),
             '-i', low_res_file,
             '-c:v', video_codec,
             '-c:b', str(target_bitrate),
             '-crf', '18',
             '-vf', 'scale=-1:360',
             '-c:a', 'copy', file])

    os.chdir(oldcwd)


def generate_concat_files(target_dir, target_ext, max_gap):
    oldcwd = os.getcwd()
    os.chdir(target_dir)
    
    max_gap = float(max_gap)
    
    # TODO: deal with leftovers (from after 24:00)
    files = sorted(glob.glob('*.{}'.format(target_ext)))
    
    def get_start_seconds(file):
        time_str = file.rsplit(' ', 1)[1].split('.')[0]
        hours, minutes, seconds = int(time_str[:2]), int(time_str[2:4]), int(time_str[4:6])
        return float(hours*60*60 + minutes*60 + seconds)
    
    def get_start_hhmm(seconds):
        hours = seconds/(60*60)
        minutes = (hours - floor(hours))*60
        return '{:02d}{:02d}'.format(floor(hours), floor(minutes))
    
    member_dict = {}
    for file in files:
        results = probe_file(file, streams=('duration', 'height'))
        if not results:
            continue

        member_name = file.rsplit(' ', 1)[0]
        if member_name not in member_dict:
            member_dict[member_name] = []
        new_video = {"start_time": get_start_seconds(file)}
        try:
            stream = json.loads(results)['streams'][0]
        except IndexError:
            new_video['valid']  = False
        else:
            new_video['file']     = file
            new_video['duration'] = float(stream['duration'])
            new_video['height']   = int(stream['height'])
            if new_video['duration'] >= 0.001 and (new_video['height'] in GOOD_HEIGHTS):
                new_video['valid']  = True
            else:
                new_video['valid']  = False
        
        if new_video['valid']:
            member_dict[member_name].append(new_video)

    concat_files = {}
    
    def new_concat_file(member, first_video):
        filename = '{} {}.{}.concat'.format(member, get_start_hhmm(first_video['start_time']), target_ext)
        info = {'files': []}
        info['height'] = first_video['height']
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
            continue
        for item in member_dict[member][1:]:
            if (item['start_time'] >= working['last_time'] + max_gap
                    or item['height'] != working['height']):
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


def merge_videos(target_dir, output_dir, copytb=1):
    oldcwd = os.getcwd()
    os.chdir(target_dir)

    os.makedirs(output_dir, exist_ok=True)

    for concat_file in glob.glob('*.concat'):
        outfile = '{}/{}'.format(output_dir, os.path.splitext(concat_file)[0])
        
        instructions = ['-copytb', str(copytb)]
        
        # wish i didn't have to read it twice...
        with open(concat_file, encoding='utf8') as infp:
            data = infp.read()
        if data.count('file \'') == 1:
            src = data[5:].strip('\'\n./')
            instructions.extend(['-i', src])
        elif data.count('file \'') > 1:
            instructions.extend(['-f', 'concat', '-safe', '0', '-i', concat_file])
        else:
            print("Empty concat file")
            raise FileNotFoundError
        
        run(['ffmpeg',
            *instructions,
            '-movflags', '+faststart',
            '-c', 'copy', outfile])
    
    os.chdir(oldcwd)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generates concat files for merging. Creates separate videos for separate broadcasts and \
        incompatible resolutions, ignores very broken videos.",
        epilog="When merging, watch the output for \"Non-monotonous DTS in output stream\" -- A few of these are \
        harmless but a wall of them means that video is probably corrupted.")
    parser.add_argument("--resize", action='store_true', help='resizes 198p videos in TARGET_DIR to 360p, '
                        'saves the old videos in a new "resized" subdirectory. Only supports h264 (MP4) and vpx (WEBM)')
    parser.add_argument("--generate", action='store_true', help='generates concat files in TARGET_DIR, runs by default')
    parser.add_argument("--merge", action='store_true',
                        help='merges videos in TARGET_DIR according to existing concat files')
    parser.add_argument("--both", action='store_true', help='both generates concat files and merges videos')
    parser.add_argument("--aggressive", action='store_true', help='resizes, generates, and merges')
    parser.add_argument("target_dir", nargs='?', default='.',
                        help='Optional. defaults to the current working directory.', metavar='TARGET_DIR')
    parser.add_argument("--max-gap", type=float, default=300.0,
                        help='maximum gap between merged videos, in seconds. anything larger is treated as a separate \
                        broadcast. default = 300.0')
    parser.add_argument("-e", dest='ext', default='mp4', help='extension to merge, defaults to mp4')
    parser.add_argument("--copytb", type=int, choices=[-1, 0, 1], default=1,
                        help='it may be useful to try setting this to 0 or -1 if a video has timing issues')
    parser.add_argument("--output-dir", "-o", dest='output_dir', type=str, default='.',
                        help='Optional, defaults to target directory. Note that relative paths will be relative to \
                        the target directory, not the current working directory', metavar='OUTPUT_DIR')
    parser.add_argument("--bitrate", "-b", type=str, default='300k',
                        help='Target bitrate for resizing. Defaults to 300k')
    args = parser.parse_args()

    if args.resize or args.aggressive:
        resize_videos(target_dir=args.target_dir, target_ext=args.ext,
                      copytb=args.copytb, target_bitrate=args.bitrate)
    if args.generate or args.both or args.aggressive:
        generate_concat_files(target_dir=args.target_dir, target_ext=args.ext,
                              max_gap=args.max_gap)
    if args.merge or args.both or args.aggressive:
        merge_videos(target_dir=args.target_dir, output_dir=args.output_dir,
                     copytb=args.copytb)

