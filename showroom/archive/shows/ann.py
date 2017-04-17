from showroom.archive import trim
from showroom.settings import settings
import json
import re
import glob
import os
from showroom.utils import iso_date_to_six_char

episode_list_url = 'https://pastebin.com/raw/h7Pc6JRs'
_episode_info_re = ''