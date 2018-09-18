import os
import logging
import time
from queue import Empty as QueueEmpty
from multiprocessing import Process, Queue
from threading import Thread
from itertools import count

from .message import ShowroomMessage
from .settings import ShowroomSettings
from .index import ShowroomIndex
from .core import WatchManager
from .exceptions import ShowroomStopRequest

control_logger = logging.getLogger("showroom.control")


class BaseShowroomLiveController(object):
    def __init__(self, index: ShowroomIndex=None, settings: ShowroomSettings=None, record_all=False):
        # TODO: proper docstring
        super(BaseShowroomLiveController, self).__init__()

        self.command_queue = Queue()
        self.message_queue = Queue()

        if not settings:
            self.settings = ShowroomSettings.from_file()
        else:
            self.settings = settings

        # TODO: where is the best place for this? I put it here because
        # controller could potentially change output dir and so needs to be
        # the one to handle recreating the folders.
        # it could also be done in ShowroomSettings if I give that object more power
        # os.makedirs(self.settings.directory.temp, exist_ok=True)

        if not index:
            self.index = ShowroomIndex(self.settings.directory.index, record_all=record_all)
        else:
            self.index = index

        self._instance = None

        self.manager = WatchManager(self.index, self.settings)

        self.counter = count()
        # aliases
        self.send = self.send_command
        self.get = self.get_messages

        # TODO: make maintenance more intelligent

    # instance (thread or process) wrapping methods
    def start(self):
        # TODO: are there any other conditions required to restart the loop?
        raise NotImplementedError

    def is_alive(self):
        return self._instance.is_alive()

    def join(self, timeout=0):
        return self._instance.join(timeout=timeout)

    def run(self):
        control_logger.debug("Running ShowroomLiveController")

        # start index update tasks (runs in separate thread)
        self.index.start()
        while True:

            # TODO: check if time for maintenance, if so do maintenance then schedule next
            # if self.resume_time > self.time.time() > self.end_time:
            #     sleep_seconds = (datetime.datetime.combine(self.time, self.resume_time)
            #                      - self.time).total_seconds() + 1.0
            #     print('Time is {}, sleeping for {} seconds, until {}'.format(strftime(self.time, '%H:%M'),
            #                                                                  sleep_seconds,
            #                                                                  strftime(self.resume_time, '%H:%M')))
            #     self.scheduler.reset_ticks()
            #     time.sleep(sleep_seconds)

            self.manager.tick()

            while not self.command_queue.empty():
                control_logger.debug('Reading command queue')
                try:
                    ident, command, args, kwargs = self.command_queue.get(block=False)
                except QueueEmpty:
                    break
                else:
                    # TODO: check that command is valid and allowed
                    if command[0] == '_':
                        control_logger.warn('Forbidden command: {}'.format(command))
                        continue
                    # TODO: lookup command in a dictionary instead of this
                    msg = ShowroomMessage(ident, command)
                    cmd, *args2 = command.replace('/', '_').split('_')
                    try:
                        msg = getattr(self, '_' + cmd)(*(list(args) + args2), msg=msg, **kwargs)
                    except ShowroomStopRequest:
                        self.index.stop()
                        return
                    except AttributeError as e:
                        # invalid command
                        control_logger.debug('Unknown command: {}, {}'.format(command, e))
                    except TypeError as e:
                        # trying to call something besides a method
                        control_logger.debug('{} is not a command -- {}'.format(command, e))
                    else:
                        if msg is not None:
                            self.message_queue.put(msg)

            time.sleep(0.2)

    def send_command(self, command, *args, **kwargs):
        ident = next(self.counter)
        self.command_queue.put((ident, command, args, kwargs))
        return ident

    def get_messages(self):
        messages = []
        while not self.message_queue.empty():
            try:
                msg = self.message_queue.get(block=False)
            except QueueEmpty:
                break
            else:
                if msg:
                    messages.append(msg)
        return messages

    def stop(self):
        self.command_queue.put((next(self.counter), "stop", [], {}))

    def _stop(self, *args, msg=None, **kwargs):
        # TODO: log stopping
        self.manager.stop()
        self.manager.write_completed()
        raise ShowroomStopRequest

    # commands
    # all commands either return None or a message: either a dict or a showroom Message

    # index commands
    # do these need to be made thread-safe? they mutate rooms... but nothing else should.
    def _index(self, *args, msg=None, **kwargs):
        if not args or args[0] == 'list':
            if msg:
                # return index list in message
                pass
        elif args[0] == 'filter':
            self._index_filter(*args[1:], **kwargs)
        elif args[0] == 'update':
            self._index_update(*args[1:], **kwargs)

    def _index_filter(self, *args, msg=None, **kwargs):
        b_updated = False
        if not args or args[0] == 'list':
            if msg:
                """Returns a dict of all wanted and unwanted rooms, by name.
                    {"index_filters": {"wanted": [...], "unwanted": [...]} }
                """
                return msg.set_content(self.index.filter_get_list())
            else:
                # raise message needed exception
                pass

        if "all" in args:
            """Turns downloading on for all rooms."""
            self.index.filter_all()
            # TODO: check if this actually changed anything
            # have index.filter return number of modified rooms?
            b_updated = True

        elif "none" in args:
            """Turns downloading off for all rooms."""
            self.index.filter_none()
            b_updated = True

        if "add" in kwargs:
            """Sets downloading on for specific rooms."""
            self.index.filter_add(kwargs["add"])
            b_updated = True

        if "remove" in kwargs:
            """Sets downloading off for specific rooms."""
            self.index.filter_remove(kwargs["remove"])
            b_updated = True

        if b_updated:
            self.manager.update_flag.set()

    def _index_update(self, *args, **kwargs):
        """Updates the index from either the local filesystem or a web source"""
        if 'web' in args:
            if 'src_url' in kwargs:
                self.index.update_from_web(kwargs['src_url'])
            else:
                self.index.update_from_web()
        self.index.update()

    def _index_update_from_web(self, src_url=None):
        """Updates the index from a remote source.

        Source must be a json like
            https://wlerin.github.io/showroom-index/list.json
        that points to a set of jdex files to update from.
        """
        self.index.update_from_web(src_url)

    # TODO: Messages require a unique identifier given them by the caller
    # room list commands
    def _get_rooms_by_mode(self, mode):
        rooms = []
        for watch in self.manager.watchers.get_by_mode(mode):
            rooms.append(watch.get_info())
        return sorted(rooms, key=lambda x: (x['start_time'], x['name']))

    # TODO: get these working again
    # "endpoints"
    # take arbitrary args and kwargs and parse through them for meaningful instructions
    def _schedule(self, *args, msg=None, **kwargs):
        if msg is not None:
            if args:
                pass
            if kwargs:
                # TODO: take other options
                pass
            msg.set_content(self._get_rooms_by_mode("working"))
            return msg

    def _lives(self, *args, msg=None, **kwargs):
        if msg is not None:
            msg.set_content(self._get_rooms_by_mode("live"))
            return msg

    def _downloads(self, *args, msg=None, **kwargs):
        if msg is not None:
            # the caller can then filter these to get links if desired...?
            msg.set_content(self._get_rooms_by_mode("download"))
            return msg


class ShowroomLiveControllerThread(BaseShowroomLiveController):
    def start(self):
        # TODO: are there any other conditions required to restart the loop?
        if not self._instance or not self._instance.is_alive():
            self._instance = Thread(target=self.run, name="ShowroomLiveController")
            self._instance.start()


class ShowroomLiveControllerProcess(BaseShowroomLiveController):
    def start(self):
        # TODO: are there any other conditions required to restart the loop?
        if not self._instance or not self._instance.is_alive():
            self._instance = Process(target=self.run, name="ShowroomLiveController")
            self._instance.start()


Controller = ShowroomLiveControllerThread
