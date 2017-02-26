from argparse import ArgumentParser
import threading
from queue import Queue, Empty as QueueEmpty
import time
import sys
import os
from io import UnsupportedOperation

# This seems like a waste of an import
from .constants import TOKYO_TZ, HHMM_FMT, FULL_DATE_FMT
from .control import ShowroomLiveControllerThread as ShowroomController
from .exceptions import ShowroomStopRequest
from .settings import ShowroomSettings, DEFAULTS
from .index import ShowroomIndex
import logging
import datetime

# build settings and index objects from arguments
# build controller
# start controller
# translate command line instructions to controller commands
cli_logger = logging.getLogger('showroom.cli')


class BasicCLI(object):
    @staticmethod
    def build_parser():
        parser = ArgumentParser(description="Watches Showroom for live videos and downloads them \
                                             when they become available. Most options only apply in --all mode",
                                epilog="The max-* options, parser, index, and output-dir haven't been \
                                             fully tested yet. A new indexing system is currently in use, but \
                                             no command-line arguments to control it yet exist.")
        parser.add_argument('names', nargs='*',
                            help='A quoted Member Name to watch. Accepts a list of names, separated by spaces. '
                                 'Currently, the Member Name must match the English Name (engName) key exactly.')
        parser.add_argument('--all', '-a', action='store_true', dest='record_all',
                            help='Watch the main showroom page for live shows and record all of them.')
        parser.add_argument('--output-dir', '-o',
                            help='Directory in which to store active and completed downloads. \
                                Defaults to "{directory[output]}"'.format(**DEFAULTS))
        parser.add_argument('--config', help="Path to config file")
        parser.add_argument('--data-dir', '-d',
                            help='Data directory. Defaults to "{directory[data]}"'.format(**DEFAULTS))
        parser.add_argument('--index', '-i', dest="index_dir",
                            help='Path to an index directory, containing room information in json files \
                                with a jdex extension. Defaults to "{directory[index]}"'.format(**DEFAULTS))
        parser.add_argument('--max-downloads', '-D', type=int,
                            help='Maximum number of concurrent downloads. \
                                Defaults to {throttle[max][downloads]}'.format(**DEFAULTS))
        parser.add_argument('--max-watches', '-W', type=int,
                            help='Maximum number of rooms to watch at once (waiting for them to go live). \
                                Defaults to {throttle[max][watches]}'.format(**DEFAULTS))
        parser.add_argument('--max-priority', '-P', type=int,
                            help='Any members with priority over this value will be ignored. \
                                Defaults to {throttle[max][priority]}'.format(**DEFAULTS))
        parser.add_argument('--live-rate', '-R', dest="onlives_rate", type=float,
                            help='Seconds between each poll of ONLIVES. \
                                Defaults to {throttle[rate][onlives]}'.format(**DEFAULTS))
        parser.add_argument('--schedule-rate', '-S', dest="upcoming_rate", type=float,
                            help='Seconds between each check of the schedule. \
                                Defaults to {throttle[rate][upcoming]}'.format(**DEFAULTS))
        '''
        # TODO: Allow the user to provide a schedule with different start and end hours per day.
        # Or else instead of stopping entirely, slow down polling during off hours.
        parser.add_argument('--end_hour', default=END_HOUR, type=int,
                    help='Hour to stop recording (will actually stop 10 minutes later). \
                        Defaults to %(default)s')
        parser.add_argument('--resume_hour', default=RESUME_HOUR, type=int,
                    help='Hour to resume recording (will actually start 10 minutes earlier). \
                        Defaults to %(default)s')
        '''
        # TODO: handle names in arg parser
        parser.add_argument('--logging', action='store_true', help="Turns on ffmpeg logging.")
        parser.add_argument('--noisy', action='store_true', help="Print download links when downloads start")

        return parser

    # TODO: MessageHandler class that parses a message object and returns the desired string
    # based on the stored query
    @staticmethod
    def _parse_index_filter_list(filter_list):
        if len(filter_list['unwanted']) == 0:
            return "Downloading all rooms."
        elif len(filter_list['wanted']) == 0:
            return "Not downloading any rooms."
        elif len(filter_list['wanted']) > len(filter_list['unwanted']):
            # TODO: word wrap?
            names = ', '.join(filter_list['unwanted'])
            return "Not downloading the following rooms:\n{}".format(names)
        else:
            names = ', '.join(filter_list['wanted'])
            return "Downloading the following rooms:\n{}".format(names)

    # TODO: have these return a single string instead of printing directly
    @staticmethod
    def _parse_scheduled_rooms(scheduled):
        def print_status(item):
            if item['mode'] in ('live', 'download'):
                return " (LIVE)"
            else:
                return ""

        output = ["{start} {group} {name}{status}".format(start=e['start_time'].strftime(HHMM_FMT),
                                                          group=e['room']['group'],
                                                          name=e['room']['name'],
                                                          status=print_status(e))
                  for e in scheduled]
        print('----------\n{} Scheduled Rooms:'.format(len(output)))
        print(*output, sep='\n')
        print()

    @staticmethod
    def _parse_live_rooms(lives):
        def print_status(item):
            if item['mode'] == 'download':
                return " (DOWNLOADING)"
            else:
                return ""

        output = ["{start} {group} {name}{status}".format(start=e['start_time'].strftime(HHMM_FMT),
                                                          group=e['room']['group'],
                                                          name=e['room']['name'],
                                                          status=print_status(e))
                  for e in lives]
        print('----------\n{} LIVE ROOMS:'.format(len(output)))
        print(*output, sep='\n')
        print()

    @staticmethod
    def _parse_download_rooms(downloads):
        output = ["{start} {group} {name}\n".format(start=e['start_time'].strftime(HHMM_FMT),
                                                    group=e['room']['group'],
                                                    name=e['room']['name'])
                  for e in downloads]
        print('----------\n{} Downloading Rooms:'.format(len(output)))
        print(*output, sep='\n')
        print()

    @staticmethod
    def _parse_download_links(downloads):
        def print_status(item):
            if item['mode'] == 'download':
                return ""
            else:
                return " (not downloading)"

        output = ["{start} {group} {name}{status}\n"
                  "{web_url}\n{rtmp_url}".format(start=e['start_time'].strftime(HHMM_FMT),
                                                 group=e['room']['group'],
                                                 name=e['room']['name'],
                                                 status=print_status(e),
                                                 web_url=e['room']['web_url'],
                                                 rtmp_url=e['download']['streaming_urls']['rtmp_url'])
                  for e in downloads]
        print('----------\nDOWNLOAD LINKS:')
        print(*output, sep='\n')
        print()

    def __init__(self):
        args = self.build_parser().parse_args()

        if args:
            self.settings = ShowroomSettings.from_args(args)
        else:
            self.settings = ShowroomSettings()

        # does this work? what is it relative to?
        self.index = ShowroomIndex(self.settings.directory.index, record_all=self.settings.filter.all)
        # DEBUG
        cli_logger.debug('Index has {} rooms'.format(len(self.index)))

        self.control_thread = ShowroomController(self.index, self.settings)
        self.input_queue = InputQueue()

        if 'all' in args and args.all:
            self.control_thread.index.filter_all()
        else:
            self.control_thread.index.filter_add(args.names)

        self._time = datetime.datetime.fromtimestamp(0.0, tz=TOKYO_TZ)

        # TODO: This needs to be revised
        self.query_dict = {"index_filter_list": self._parse_index_filter_list,
                           "schedule": self._parse_scheduled_rooms,
                           "lives": self._parse_live_rooms,
                           "downloads": self._parse_download_rooms,
                           "downloads_links": self._parse_download_links}

    def start(self):
        self.input_queue.start()
        self.control_thread.start()

    def run(self):
        """Do stuff."""
        while True:
            try:
                self.read_commands()
            except ShowroomStopRequest:
                print("Exiting...")
                return

            # Automatic hourly schedule updates
            # curr_time = datetime.datetime.now(tz=TOKYO_TZ)
            # if (curr_time - self._time).total_seconds() > 3600.0:
            #     self._time = curr_time
            #     print(curr_time.strftime("\n\n%H:%M"))
            #     self.control_thread.send_command('schedule')

            time.sleep(0.2)

            self.get_messages()

    def read_commands(self):
        while not self.input_queue.empty():
            try:
                line = self.input_queue.get(block=False)
            except QueueEmpty:
                break
            else:
                self.parse_command(line)

    # TODO: CommandHandler class?
    def parse_command(self, line):
        # here we take every allowed command and try to translate it to a call on the control_thread
        # we need to construct a language though...
        # set and get are obvious
        # todo: more robust translation
        ct = self.control_thread
        send = ct.send_command
        line = line.lower()

        if line.startswith('index'):
            if 'index filter' in line:
                if "filter all" in line:
                    send('index_filter', "all")
                elif "filter none" in line:
                    send('index_filter', "none")
                elif "filter add" in line:
                    names = line.split('filter add')[-1].strip()
                    split_names = [e.strip() for e in names.split(',')]

                    send('index_filter', add=split_names)
                    print("Turning on downloads for the following rooms:\n" + ', '.join(names).title())
                elif "filter remove" in line:
                    names = line.split('filter remove')[-1].strip()
                    split_names = [e.strip() for e in names.split(',')]
                    send('index_filter', remove=split_names)
                    # TODO: print a log info message when this actually gets done,
                    # as chances are the results won't be 100% exactly what's printed here
                    print("Turning off downloads for the following rooms:\n" + ', '.join(names).title())
            elif 'index update' in line:
                if "update from web" in line:
                    send('index_update', src="web")
                else:
                    send('index_update')
        # TODO: other set commands
        elif line.startswith("get"):
            if 'get index filter' in line:
                send('index_filter')
            elif 'get schedule' in line:
                send('schedule')
            elif 'get live' in line:
                send('lives')
            elif 'get download' in line:
                send('downloads')
            elif 'get links' in line:
                # i want the same content but in a different format, what's the right way to do this?
                send('downloads_links')

        elif line.startswith('schedule'):
            send('schedule')
        elif line.startswith('live'):
            send('lives')
        elif line.startswith('download'):
            if 'links' in line:
                send('downloads_links')
            else:
                send('downloads')
        elif line.startswith('links'):
            send('downloads_links')

        elif line.strip() == 'help':
            print("""
                The following commands are recognised:
                  index filter all  --  selects all rooms for downloading
                  index filter none --  selects no rooms for downloading
                  index filter add name1, name2, name3...
                  index filter remove name1, name2, name3...
                                    -- add or remove rooms from the download list
                                    -- name must match exactly (case insensitive)
                  index update      -- locally update the index
                  index update from web -- update the index from github (NOT IMPLEMENTED)
                  get index filter  -- returns info about the filter
                  get schedule      -- prints a schedule
                  get live          -- prints currently live rooms
                  get downloads     -- prints current downloads
                  get links         -- prints live rooms with links
                  stop              -- stop activity (program will continue running)
                  start             -- restart activity
                  quit              -- stop activity and exit
                  help              -- this text
                """)
        # TODO: test these
        elif line.strip() == 'stop':
            "--stop--"
            ct.stop()
            ct.join()
            print('Stopped')
        elif line.strip() == 'start':
            "--start--"
            ct.start()
            print('Started')
        elif line.strip() == 'quit':
            "--quit--"
            print('Quitting...')
            ct.stop()
            self.input_queue.stop()
            ct.join()
            raise ShowroomStopRequest

    def get_messages(self):
        messages = self.control_thread.get_messages()
        for msg in messages:
            self.parse_message(msg)

    def parse_message(self, msg):
        query = msg.query
        message = msg.content
        if query in self.query_dict:
            text = self.query_dict[query](message)
            if text:
                print(text)


class InputQueue(Queue):
    def __init__(self):
        super().__init__()
        self.STDIN = None
        self.input_thread = None

    def read_commands(self):
        while True:
            try:
                # DEBUG
                # print('waiting for line')
                line = self.STDIN.readline()
                # DEBUG
                # print('read line')
            except ValueError:
                # tried to read from a closed STDIN
                return
            if line:
                self.put(line)
            time.sleep(0.1)

    def start(self):
        # make an alias of stdin so that we can close it later
        # TODO: allow taking input from other sources?
        try:
            fileno = sys.stdin.fileno()
        except UnsupportedOperation:
            # trying to run this from idle?
            raise

        if fileno is not None:
            self.STDIN = os.fdopen(os.dup(fileno))
        else:
            self.STDIN = None  # this is a failure state!

        self.input_thread = threading.Thread(target=self.read_commands)
        self.input_thread.daemon = True
        self.input_thread.start()

    def stop(self):
        # the alternative is sending SIGKILL or something
        if self.STDIN:
            self.STDIN.close()
        self.input_thread.join()
        self.STDIN = None
        self.input_thread = None
        # clear the queue?
