import datetime

TOKYO_TZ = datetime.timezone(datetime.timedelta(hours=9), name='Asia/Tokyo')
HHMM_FMT = '%H:%M'
FULL_DATE_FMT = '%Y-%m-%d %H:%M:%S'

MODE_TO_STATUS = {'download': 'downloading', 'live': 'live', 'schedule': 'scheduled', 'watch': 'watching'}