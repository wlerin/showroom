import json
from subprocess import check_output, DEVNULL, CalledProcessError


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
        return [frame for frame in r['frames'] if frame['pict_type'].upper() == 'I']


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
