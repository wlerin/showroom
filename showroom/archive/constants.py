from showroom.settings import settings
from showroom.index import ShowroomIndex

# these might only be needed in check.py
ENGLISH_INDEX = ShowroomIndex(settings.directory.index, language='eng')
JAPANESE_INDEX = ShowroomIndex(settings.directory.index, language='jpn')

GOOD_HEIGHTS = (198, 360, 396, 720)
STREAM_FOUND = True
STREAM_NOT_FOUND = False
# TODO: allow the user to set this
MAX_GAP = 300.0
