import json
import os

from .utils.appdirs import AppDirs

try:
    from yaml import load as yaml_load, dump as yaml_dump, YAMLError
except ImportError:
    useYAML = False
else:
    useYAML = True
    try:
        from yaml import CLoader as YAMLLoader, CDumper as YAMLDumper
    except ImportError:
        from yaml import Loader as YAMLLoader, Dumper as YAMLDumper

__all__ = ['ShowroomSettings', 'settings']

ARGS_TO_SETTINGS = {
    "record_all": "filter.all",
    "output_dir": "directory.output",
    "data_dir": "directory.data",
    "index_dir": "directory.index",
    "config": "file.config",
    "max_priority": "throttle.max.priority",
    "max_watches": "throttle.max.watches",
    "max_downloads": "throttle.max.downloads",
    "live_rate": "throttle.rate.live",
    "schedule_rate": "throttle.rate.schedule",
    "names": "filter.wanted",
    "logging": "ffmpeg.logging",
    "noisy": "feedback.console",
    "comments": "comments.record"
}

_dirs = AppDirs('Showroom', appauthor=False)

# TODO: refactor this into data.path, index.path, config.path, etc. ?
# TODO: paths should automatically call expanduser, but they need to be marked as such
DEFAULTS = {
    "directory": {
        "data": os.path.expanduser('~/Downloads/Showroom'),
        "output": '{data}',
        "index": 'index',
        "log": _dirs.user_log_dir,
        "config": _dirs.user_config_dir,
        # This setting is NOT respected by Downloader (it uses {output}/active always)
        "temp": '{data}/active'
    },
    "file": {
        "config": '{directory.config}/showroom.conf',
        "schedule": '{directory.data}/schedule.json',
        "completed": '{directory.data}/completed.json'
    },
    "throttle": {
        "max": {
            "downloads": 80,
            "watches": 50,
            "priority": 80
        },
        "rate": {
            "upcoming": 180.0,
            "onlives": 7.0,
            "watch": 2.0,
            "live": 60.0
        },
        "timeout": {
            "download": 23.0
        }
    },
    "ffmpeg": {
        "logging": False,
        "path": "ffmpeg",
    },
    "filter": {
        "all": False,
        "wanted": [],
        "unwanted": []
    },
    "feedback": {
        "console": False,  # this actually should be a loglevel
        "write_schedules_to_file": True
    },
    "system": {
        "make_symlinks": True,
        "symlink_dirs": ('log', 'config')
    },
    "comments": {
        "record": False,
        "default_update_interval": 7.0,
        "max_update_interval": 30.0,
        "min_update_interval": 2.0,
        "max_priority": 100
    },
    "environment": {}
}
_default_args = {
    "record_all": False,
    "comments": False,
    "noisy": False,
    "logging": False,
    "names": []
}

DEFAULTS_NEW = {
    "data": {
        "path": os.path.expanduser('~/Downloads/Showroom'),
    },
    "output": {
        # TODO: remove this?
        "path": '{data.path}',
    },
    "index": {
        "path": '{data.path}/index',
    },
    "log": {
        "path": _dirs.user_log_dir,
    },
    "config": {
        # Is there a point to including this?
        "path": _dirs.user_config_dir + '/showroom.conf',
    },
    "temp": {
        # TODO: Fix downloader so it respects this
        "path": '{data}/active'
    },
    "file": {
        "config": '{config.path}/showroom.conf',
        "schedule": '{data.path}/schedule.json',
        "completed": '{data.path}/completed.json'
    },
    "throttle": {
        "max": {
            "downloads": 80,
            "watches": 50,
            "priority": 80
        },
        "rate": {
            "upcoming": 180.0,
            "onlives": 7.0,
            "watch": 2.0,
            "live": 60.0
        },
        "timeout": {
            "download": 23.0
        }
    },
    "ffmpeg": {
        "logging": False,
        "path": "ffmpeg",
    },
    "filter": {
        "all": False,
        "wanted": [],
        "unwanted": []
    },
    "feedback": {
        "console": False,  # this actually should be a loglevel
        "write_schedules_to_file": True
    },
    "system": {
        # TODO: Fix this to work with the new paths
        "make_symlinks": True,
        "symlink_dirs": ('log', 'config')
    },
    "comments": {
        "record": False,
        "default_update_interval": 7.0,
        "max_update_interval": 30.0,
        "min_update_interval": 2.0,
        "max_priority": 100
    },
    "environment": {}
}

def _clean_args(args):
    new_args = {}
    for k, v in vars(args).items():
        if _default_args.get(k) != v:
            new_args[k] = v
    return new_args


def load_config(path):
    # TODO: support old-style setting names? i.e. pass them through ARGS_TO_SETTINGS ?
    data = {}
    yaml_err = ""
    if useYAML:
        try:
            # this assumes only one document
            with open(path, encoding='utf8') as infp:
                data = yaml_load(infp, Loader=YAMLLoader)
        except FileNotFoundError:
            return {}
        except YAMLError as e:
            yaml_err = 'YAML parsing error in file {}'.format(path)
            if hasattr(e, 'problem_mark'):
                mark = e.problem_mark
                yaml_err + '\nError on Line:{} Column:{}'.format(mark.line + 1, mark.column + 1)
        else:
            return _convert_old_config(data)
    try:
        with open(path, encoding='utf8') as infp:
            data = json.load(infp)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as e:
        if useYAML and yaml_err:
            print(yaml_err)
        else:
            print('JSON parsing error in file {}'.format(path),
                  'Error on Line: {} Column: {}'.format(e.lineno, e.colno), sep='\n')

    data = _convert_old_config(data)

    # if 'directory' in data:
    #     for k, v in data['directory'].items():
    #         data['directory'][k] = os.path.expanduser(v)

    return data


def _convert_old_config(config_data):
    new_data = config_data.copy()
    for key in config_data.keys():
        if key in ARGS_TO_SETTINGS:
            # what will SettingsDict do with stuff like:
            # {"directory": {"data": "data"},
            #  "directory.data": "data"}
            new_key = ARGS_TO_SETTINGS[key]
            new_data[new_key] = config_data[key]
        else:
            new_data[key] = config_data[key]
    return new_data


# inherit from mapping or dict?
class SettingsDict(dict):
    """
    Holds a mutable collection of items, all addressable either by .name or by
    [key].

    Each key must be either a string or an int, in the case of ints, the key will
    be converted to a string. i.e. sd[0] is the same as sd['0']

    Actually though do I basically just want a SimpleNamespace?
    """
    def __init__(self, sub_dict: dict, top=None):
        super().__init__()
        sub_dict = sub_dict.copy()

        self._dict = {}
        self._formatting = False

        if top is None:
            self._top = self
        else:
            self._top = top
        self._dict.update(self.__wrap_dicts(sub_dict))

    def __repr__(self):
        r = []
        for key in self.keys():
            r.append('{k}: {v}'.format(k=repr(key), v=repr(self[key])))
        return '{' + ', '.join(r) + '}'

    def __wrap_dicts(self, dct):
        for key in dct:
            dct[key] = self.__wrap(dct[key])
        return dct

    def __wrap_lists(self, lst):
        for i in range(len(lst)):
            lst[i] = self.__wrap(lst[i])
        return lst

    def __wrap(self, item):
        if type(item) is not type(self):
            # too much redundancy
            if isinstance(item, dict):
                item = SettingsDict(item, self._top)
            elif isinstance(item, list):
                # tuples can't be changed, sets can't hold unhashable types
                item = self.__wrap_lists(item)
        return item

    def __getattr__(self, name):
        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

    def __getitem__(self, key):
        if super().__contains__(key) or key.startswith('_'):
            return super().__getitem__(key)
        elif '.' in key:
            key, subkeys = key.split('.', 1)
            val = self._dict[key][subkeys]
        else:
            val = self._dict.get(key)
            if val and key == 'path':
                # atm this only works for ffmpeg.path, where it isn't usually needed
                # in the future I will fix this so directory.data -> data.path etc.
                # and home-relative paths can finally be used
                val = os.path.expanduser(val)
        if self._top._formatting and isinstance(val, str) and '{' in val:
            try:
                return val.format(**self._dict)
            except KeyError:
                return val.format(**self._top._dict)

        return val

    def __setitem__(self, key, value):
        if super().__contains__(key) or key.startswith('_'):
            super().__setitem__(key, value)
        elif '.' in key:
            key, subkeys = key.split('.', 1)
            self._dict[key][subkeys] = self.__wrap(value)
        else:
            self._dict[key] = self.__wrap(value)

    def __delitem__(self, key):
        if super().__contains__(key) or key.startswith('_'):
            super().__delitem__(key)
        elif '.' in key:
            key, subkeys = key.split('.', 1)
            del self._dict[key][subkeys]
        else:
            del self._dict[key]

    def __iter__(self):
        return (k for k in self._dict)

    def __len__(self):
        return len(self._dict)

    def __contains__(self, item):
        # TODO: work with dot notation too
        return item in self._dict

    def keys(self):
        return self._dict.keys()

    def items(self):
        for key in self._dict.keys():
            yield key, self[key]

    def update(self, other=None, **kwargs):
        if other is not None:
            for key, o_val in other.items():
                s_val = self[key] if key in self else None
                if isinstance(o_val, dict) and isinstance(s_val, SettingsDict):
                    s_val.update(o_val)
                elif o_val is not None:
                    self[key] = o_val
        for key in kwargs:
            s_val = self.get(key)
            o_val = kwargs[key]
            if isinstance(o_val, SettingsDict) and isinstance(s_val, SettingsDict):
                s_val.update(o_val)
            elif o_val is not None:
                self[key] = o_val


class ShowroomSettings(SettingsDict):
    def __init__(self, settings_dict: dict=DEFAULTS):
        self._formatting = False
        super().__init__(settings_dict, top=self)
        self._formatting = True
        # TODO: add some properties like e.g. curr_time, curr_date that can be including in formatting specifiers

    # TODO: check that this all works
    @classmethod
    def from_file(cls, path=None):
        new = cls(DEFAULTS)

        if not path:
            path = new.file.config

        config_data = load_config(path)
        new.update(config_data)

        new.makedirs(new)

        return new

    @classmethod
    def from_args(cls, args):
        args = _clean_args(args)

        new = cls.from_file(path=args.get('config', None))

        # TODO: translate args to settings keys
        args_data = {}
        for key in args:
            if args[key] is not None and key in ARGS_TO_SETTINGS:
                new_key = ARGS_TO_SETTINGS[key]
                args_data[new_key] = args[key]

        # for k, v in args_data.items():
        #     if k.startswith('directory'):
        #         args_data[k] = os.path.expanduser(v)

        new.update(args_data)

        new.makedirs(new)


        return new

    @staticmethod
    def makedirs(settings):
        links = []
        for dir_key, dir_path in settings.directory.items():
            os.makedirs(dir_path, exist_ok=True)
            if dir_key in settings.system.symlink_dirs:
                links.append((dir_key, dir_path))

        if settings.system.make_symlinks:
            for item in links:
                # symlinks the log, index, and config folders to the data directory
                # TODO: whenever these three directories are changed, remove the old symlinks
                # and create new ones
                dest_path = os.path.join(settings.directory.data, item[0])
                if os.path.exists(dest_path):
                    continue
                else:
                    os.symlink(os.path.abspath(item[1]), dest_path, target_is_directory=True)


settings = ShowroomSettings.from_file()

# old defaults, for reference
"""
DEFAULTS = {"output_dir":        "output",
            "index_dir":         "index",
            "log_dir":           "logs",
            "config_dir":        _dirs.user_config_dir,
            "config_file":       'config.json',
            # TODO: allow using {output_dir}/data or similar directly in config file
            "data_dir":          None,  # if None, defaults to {output_dir}/data
            # "schedule_file":     'schedule.json',
            # "completed_file":    'completed.json',
            "record_all":        False,
            # maximums
            "max_downloads":      80,
            "max_watches":        50,
            "max_priority":       80,
            # manager rate control
            "upcoming_rate":   180.0,  # check api/live/upcoming
            "onlives_rate":       7.0,  # check api/live/onlives
            # watcher rate control
            "watch_rate":        2.0,  # check if watched room is live
            "live_rate":        60.0,  # check if unwanted stream is still live
            "download_timeout":  23.0,  # too short and we lose the start of the stream
            # end of day
            # "end_hour":            4,
            # "resume_hour":         5,
            # ffmpeg flags
            "ffmpeg_logging":   True,
            # TODO: proper verbosity levels
            "noisy":            False,
            'write_schedules_to_file': True}
"""


# sample yaml config file, mostly without values
"""
directory:
    data: null  # ~/Downloads/Showroom
    output: {data}
    index: index
    log: null
    config: null
    temp: {data}/active
file:
    config: showroom.conf
    schedule: schedule.json
    completed: completed.json
throttle:
    max:
        downloads: 80
        watches:   50
        priority:  80
    rate:
        upcoming: 180.0
        onlives:    7.0
        watch:      2.0
        live:      60.0
    timeout:
        download:  23.0
ffmpeg:
    logging: true
filter:
    all: false
    wanted: []
    unwanted: []
feedback:
    console: false
    write_schedules_file: true
"""