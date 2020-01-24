import os
import json
from subprocess import check_output, DEVNULL, CalledProcessError
from .constants import ffmpeg


ffprobe = os.path.join(os.path.split(ffmpeg)[0], 'ffprobe')
if ffmpeg.endswith('.exe'):
    ffprobe += '.exe'

def get_iframes(filename, read_interval=None):
    """

    Args:
        filename: 
        read_interval: string specifying a read_interval, see  
            https://ffmpeg.org/ffprobe.html

    Returns:
        list of iframe dicts
    """
    args = [
        "-show_frames",
        "-select_streams", "v"
    ]

    if read_interval:
        args.extend(["-read_intervals", read_interval])

    try:
        results = check_output([
            "ffprobe",
            '-loglevel', '16',
            *args,
            '-i', filename,
            '-of', 'json'
        ],
            universal_newlines=True,
            stderr=DEVNULL,
            stdin=DEVNULL
        )
    except CalledProcessError:
        return None
    else:
        r = json.loads(results)
        return [frame['pkt_pts_time'] for frame in r['frames'] if frame['pict_type'].upper() == 'I']


def get_iframes2(filename, read_interval=None):
    """
    Get all iframes in a video.
        
    :param filename: path to video
    :param read_interval: A sequence of ffmpeg intervals separated by "," 
        see read_intervals here: https://www.ffmpeg.org/ffprobe.html for more information
    :return: a list of iframe timestamps, as strings
        e.g. ["0.033333", "1.133333", "2.233333", ...]
    """
    args = [
        ffprobe,
        "-loglevel", "16",
        "-show_packets",
        "-select_streams", "v",
        "-show_entries", "packet=pts_time,flags"
    ]
    if read_interval:
        args.extend(["-read_intervals", read_interval])
    args.extend(["-i", filename, "-of", "json"])
    try:
        results = check_output([
            *args
        ], universal_newlines=True)
    except CalledProcessError:
        return None
    else:
        r = json.loads(results)
        iframes = [float(packet['pts_time']) for packet in r['packets'] if 'K' in packet['flags']]
        return iframes


def probe_video(filename, stream='v', entries=()):
    try:
        results = check_output([
            ffprobe,
            '-loglevel', '16',
            '-show_entries', 'stream={}'.format(','.join(entries)),
            '-select_streams', stream,
            '-i', filename,
            '-of', 'json'
        ],
            universal_newlines=True,
            stderr=DEVNULL,
            stdin=DEVNULL
        )
    except CalledProcessError:
        return None
    else:
        try:
            return json.loads(results)['streams']
        except IndexError:
            return None


def probe_video2(filename):
    """
    Leaves it up to the caller to filter the returned info
    """
    try:
        results = check_output([
            ffprobe,
            '-loglevel', '16',
            '-show_streams',
            '-show_format',
            '-i', filename,
            '-of', 'json'
        ],
            universal_newlines=True,
            stderr=DEVNULL,
            stdin=DEVNULL
        )
    except CalledProcessError:
        return None
    else:
        return json.loads(results)
