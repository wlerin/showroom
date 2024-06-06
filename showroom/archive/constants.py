from showroom.settings import settings
from showroom.index import ShowroomIndex

# these might only be needed in check.py
print(settings.directory.index)
ENGLISH_INDEX = ShowroomIndex(settings.directory.index, language='eng')
JAPANESE_INDEX = ShowroomIndex(settings.directory.index, language='jpn')

# GOOD_HEIGHTS = (180, 198, 270, 360, 396, 720, 1080)
# heights known to signify bad streams, still won't fail a stream unless it fails other tests
BAD_HEIGHTS = (540,)
STREAM_FOUND = True
STREAM_NOT_FOUND = False
# TODO: allow the user to set this
MAX_GAP = 300.0

ffmpeg = settings.ffmpeg.path
