
# Showroom Live Watcher

![Python 3.5+](https://img.shields.io/badge/Python-3.5%2B-green.svg)
![License: MIT](https://img.shields.io/badge/license-MIT_License-blue.svg)

## Known Issues

1. Most of the help text is outdated (e.g. `--noisy` is required for it to
print to the console *at all*)
2. Most of the instructions below are also outdated. (Usage and Installation 
still work but they are missing a lot of information.)
3. pyyaml is not in requirements.txt (needed for config files)
4. config file format is not fixed

## Usage

Basic usage remains the same as previous versions, except that `--all`
is once again not the default mode. To run the script and download
all streaming rooms to the data directory (by default ~/Downloads/Showroom)
enter:

    python showroom.py --all

(Using the showroom virtual environment if it was configured.)

To download only specific members' rooms, use:

    python showroom.py "Member Name" ["Another Member Name" ...]

To set a different data directory, use:

    python showroom.py --data-dir <data directory> [--all or "Member Name"]
For additional options, type:

    python showroom.py --help

Or take a gander at the start.sh script.

## Installation

Requires FFmpeg, Python 3.5+, and Requests

#####  1. Install ffmpeg

Either download prebuilt binaries from
[the FFmpeg website](http://ffmpeg.org/download.html) or your
distro's package manager, or compile it from source. If you do choose to
[compile it yourself](https://trac.ffmpeg.org/wiki/CompilationGuide)
 (unnecessary for this script)
you *must* use the `--enable-librtmp` build flag. It is also highly
recommended to use `--enable-openssl`
in addition to the codecs suggested in the FFmpeg compilation guide.
Each requires the relevant system libraries (librtmp, openssl, etc.)
to be installed before building.

Libav will not be supported.

Currently this script does not respect user defined executable paths
(e.g. ~/.local/bin) so only ffmpeg executables located in the system
path will be used. This will change in the future, and an option to
specify the location of ffmpeg will be added.

##### 2. Install Python 3.5+

Download and install from
[the Python website](https://www.python.org/downloads/)
or your distro's package manager. Python 3.6.x is strongly recommended,
both because future versions of the script may make use of the new
features added in 3.6, and for speed improvements.

##### 3. (Optional) Setup a virtual environment

Using a **virtual environment** rather than
installing packages into your system environment is also ***strongly
recommended***. The recommended tool for this is
[virtualenvwrapper](https://virtualenvwrapper.readthedocs.io/en/latest/),
although many other alternatives exist.
Follow the [Installation Guide](https://virtualenvwrapper.readthedocs.io/en/latest/install.html)
through to the Quick-Start section, then set up a virtual environment for
Showroom using:

    mkvirtualenv showroom --python=python3

It will be automatically activated after creation, but to activate it
again in the future, use:

    workon showroom

and deactivate with:

    deactivate

All calls to `pip` beyond this point should be made with the virtual
environment active.

##### 4. Download Showroom Live Watcher

Clone the repository using git:

    git clone https://github.com/wlerin/showroom.git

Or use the Download as ZIP button above.

##### 5. Install Required Python Packages

- [Requests](http://docs.python-requests.org/en/master/)

Both of these packages can be installed by running:

    pip install -r requirements.txt

in the showroom-dev directory.

##### 6. (Optional) Install PyYAML

- [PyYAML](http://pyyaml.org/wiki/PyYAMLDocumentation)

Required if you want to format your config files using YAML instead
of JSON. In the future this may also be an option for output. You
may wish to install libyaml as well (see the
[libYAML](http://pyyaml.org/wiki/LibYAML) docs) for faster parsing.

    pip install pyyaml

##### 7. (Optional) Install index_maker Dependencies

- [BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/)

Used by by index_maker to generate new Room entries. (index_maker is
not yet included in this repository)

    pip install beautifulsoup4

- [lxml](http://lxml.de/)

Fast XML parser used by BeautifulSoup (but not required for it). Needs
the libxml2 and libxslt C libraries installed.

    pip install lxml


## Configuration (EXPERIMENTAL)

Showroom can read its configuration from a file, by default called
"showroom.conf", located in one of the following locations:

    Mac OS X:     ~/Library/Application Support/Showroom/
    Unix:         ~/.config/Showroom/
    Win 7+:       C:\Users\<username>\AppData\Local\Showroom\

Technically you can then define a different config directory, but this
does nothing. You can also start the script with the
--config option to manually specify a config file. The config file must
be formatted in either JSON or, if PyYAML was installed, YAML.

Sample YAML Config File:

```yaml
directory:
    data: null  # ~/Downloads/Showroom
    output: "{data}"
    index: index
    log: null
    config: null
    temp: "{data}/active"
file:
    config: "{directory.config}/showroom.conf"
    schedule: "{directory.data}/schedule.json"
    completed: "{directory.data}/completed.json"
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
    logging: false
filter:
    all: false
    wanted: []
    unwanted: []
feedback:
    console: false
    write_schedules_to_file: true
```

All fields are optional and will be filled in with default values if
omitted or set to `null`. (In fact, the values given here are default
values, except where the defaults require further processing.) If
necessary, other fields can be referenced using `"{key[.subkey]}"`
syntax, but be careful to avoid recursive references. I.e. don't set
config to `"{data}"` and data to `"{config}"`. References will be
resolved either "locally" (e.g. `"{data}"`) or from the outermost scope
(e.g. `"{directory.data}"`). Using these references for non-string values
is not yet supported.

TODO: description of config fields


## New Data Files

Several new data files are now stored in the data directory.

#### schedule.json

The script now tracks all scheduled and live rooms (whether or not they
are being downloaded) and prints information about them to a file
called "schedule.json", where they can be read by other programs.
The file contains a JSON array, where each item is a JSON object
containing the following fields:

```json
{
   "name": "Member Name",
   "live": true | false,
   "status": "scheduled" | "watching" | "live" | "downloading",
   "start_time": "YYYY-MM-DD HH:mm:ss",
   "streaming_urls": { "hls_url": "http(s)://...",
                      "rtmp_url": "rtmp://..."},
   "room": {...} # same as the room data stored in index files
}
```

More fields may be added in the future.

#### filter.json

TODO

#### completed.json

TODO


## Archive Checks

TODO
