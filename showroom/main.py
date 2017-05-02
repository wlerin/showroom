from showroom.interfaces import BasicCLI
import logging

# TODO: set log rotation time in config
from datetime import time as dt_time, datetime
from logging.handlers import TimedRotatingFileHandler
from showroom.constants import TOKYO_TZ, HHMM_FMT
import os


def main():
    bcli = BasicCLI()
    setup_logging(os.path.join(bcli.settings.directory.log, 'showroom.log'))
    bcli.start()
    bcli.run()
    logging.shutdown()


def tokyotime(obj, seconds=None):
    if seconds is None:
        return datetime.now(tz=TOKYO_TZ).timetuple()
    else:
        # Does this do what I want it to?
        return datetime.fromtimestamp(seconds, tz=TOKYO_TZ).timetuple()


def setup_logging(log_file):
    # TODO: more advanced filters, logging info like when rooms go live to console
    # https://docs.python.org/3/library/logging.config.html#logging-config-dictschema
    log_backup_time = dt_time(tzinfo=TOKYO_TZ)
    log_filter = logging.Filter(name="showroom")

    file_log_handler = TimedRotatingFileHandler(log_file, encoding='utf8',
                                                when='midnight', atTime=log_backup_time)
    file_log_formatter = logging.Formatter(fmt='%(asctime)s %(name)-12s %(levelname)-8s %(threadName)s:\n%(message)s',
                                           datefmt='%m-%d %H:%M:%S')
    file_log_handler.setFormatter(file_log_formatter)
    # leave this in local time?
    file_log_handler.addFilter(log_filter)
    file_log_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(fmt='%(asctime)s %(message)s', datefmt=HHMM_FMT)
    console_formatter.converter = tokyotime

    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    console_handler.addFilter(log_filter)

    logger = logging.getLogger('showroom')
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    # at this moment, there shouldn't be any handlers in the showroom logger
    # however, i can't preclude the possibility of there ever being such handlers
    for handler in (file_log_handler, console_handler):
        if handler not in logger.handlers:
            logger.addHandler(handler)
