import json
from subprocess import check_output, DEVNULL, CalledProcessError


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
