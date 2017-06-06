# profile pic downloader
from showroom.session import WatchSession
from .constants import ENGLISH_INDEX
from showroom.utils.media import save_from_url, MediaFileExistsError, MediaURLNotFound
import os

_session = WatchSession()
_name_pattern = '{group} {team} {name}_{count:02d}.{ext}'


def get_profile_pic_url(room):
    room_id = room.room_id
    r = _session.json('https://www.showroom-live.com/room/get_live_data', params={"room_id": room_id})
    url = r.get('room').get('image_l')
    if url:
        return url.split('?')[0]
    else:
        return None


def name_profile_pic(room, ext, count):
    args = dict(group=room.group, team=room.team, name=room.name, count=count, ext=ext)

    return _name_pattern.format(**args)


def save_profile_pic(room, dest, count):
    url = get_profile_pic_url(room)
    if url:
        filename = name_profile_pic(room, url.split('.')[-1], count)
        destpath = '{}/{}'.format(dest, filename)
        save_from_url(url, destpath, skip_exists=True)


def scrape_profile_pics(profile_dir, photo_num=1):
    for room in ENGLISH_INDEX.room_dict.values():
        save_profile_pic(room, profile_dir, photo_num)
