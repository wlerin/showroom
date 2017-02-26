import pytz

TOKYO_TZ = pytz.timezone('Asia/Tokyo')
HHMM_FMT = '%H:%M'
FULL_DATE_FMT = '%Y-%m-%d %H:%M:%S'

WATCHSECONDS = (600, 420, 360, 360, 300, 300, 240, 240, 180, 150)

MODE_TO_STATUS = {'download': 'downloading', 'live': 'live', 'schedule': 'scheduled', 'watch': 'watching'}