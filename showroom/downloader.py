# Showroom Downloader
import subprocess
import threading
import datetime
import logging
import time
import os
import shutil

from .constants import TOKYO_TZ, FULL_DATE_FMT
from .utils import format_name, strftime

download_logger = logging.getLogger('showroom.downloader')


class Downloader(object):
    """
    Handles downloads for a parent Watcher.

    Created with a room, a client, and an output directory. Started with start(),
    then call wait() to wait on the underlying Popen process. Wait will return when the
    download process ends, either because the stream has completed, because it timed out,
    or because it was terminated from outside the thread. On POSIX systems, a negative
    return code from wait() signals termination/timeout, however this is not portable.

    Regardless of why the download finished, Watcher still needs to check live status, so
    the only reason why termination vs. completion matters is potentially responding to
    repeated timeouts (e.g. like that time all the streams failed for 4 hours)

    Attributes:
        destdir: final destination for the download
        tempdir: temporary ("active") directory
        outfile: name of the file being written to
        all_files: list of files this downloader has written, eventually will be logged
            when the stream completes

    NOTE: all_files is the only attribute that has any reason to be public

    Properties:
        stream_data: stream data returned by showroom
        protocol: protocol in use, either rtmp or hls (use enum?)
        rtmp_url, lhls_url, hls_url: separate handles for rtmp and hls urls
        timed_out: whether the last wait() timed out

    Methods: (remove this before release)
        start
        wait
        get_info
        is_running -- whether the child process is running
        stop, kill -- usually called from outside the current thread
        update_stream_url -- internal use only
        move_to_dest -- internal use only
        switch_protocol -- don't change protocol, change downloaders

    TODO:
        Logging (e.g. "download has started" or let Watcher handle this)
        Fix ffmpeg logging on Windows without pulling in PATH

    DONE:
        For now, instead of the below, just use rtmp streams:

            Separate downloaders for rtmp and hls streams? That is, if one is failing
            instead of switching the protocol, have Watcher pop off the failing stream
            and make a new downloader, handing the failing downloader off to some
            cleanup thread via queue. Or can we handle all cleanup here?

        Add a wait() function that wraps the internal Popen process and checks for fail
        states without bothering the wrapping Watcher. Raise on failure?

    TESTING:
        hls recording fails awfully. find out why
        For the failure detection to work properly, must ffmpeg be compiled with librtmp? (yes)
    """

    def __init__(self, room, client, settings, default_protocol='rtmp'):
        self._room = room
        self._client = client

        self._rootdir = settings.directory.output
        self._logging = settings.ffmpeg.logging
        self._ffmpeg_path = settings.ffmpeg.path
        self._ffmpeg_container = settings.ffmpeg.container

        self.destdir, self.tempdir, self.outfile = "", "", ""

        self._protocol = default_protocol
        self._rtmp_url = ""
        self._hls_url = ""
        self._lhls_url = ""
        self._stream_data = []

        self._process = None
        # self._timeouts = 0
        # self._timed_out = False
        self._pingouts = 0

        self._lock = threading.Lock()

        # Index of dead processes, list of tuples
        # (outfile, destdir, tempdir, process)
        self._dead_files = []

        # keep a list of previous outfile names
        self.all_files = []

    @property
    def rtmp_url(self):
        return self._rtmp_url

    @property
    def hls_url(self):
        return self._hls_url

    @property
    def lhls_url(self):
        return self._lhls_url

    @property
    def stream_url(self):
        return getattr(self, '_{}_url'.format(self.protocol))

    @property
    def protocol(self):
        return self._protocol

    def get_info(self):
        with self._lock:
            return {"streaming_urls": self._stream_data,
                    "protocol": self._protocol,
                    "filename": self.outfile,
                    "dest_dir": self.destdir,
                    "active": self.is_running(),
                    "timeouts": 0,
                    "pingouts": self._pingouts,
                    "completed_files": self.all_files.copy()}

    def is_running(self):
        """Checks if the child process is running."""
        if self._process:
            return self._process.poll() is None
        else:
            return False

    def switch_protocol(self):
        """Switches protocol between rtmp and hls."""
        with self._lock:
            if self.protocol == 'rtmp':
                self._protocol = 'hls'
            else:
                self._protocol = 'rtmp'

    def wait(self):
        """
        Waits for a download to finish.

        Returns:
            returncode of the child process, or None if a ping loop of death was detected.

            On POSIX systems, this will be a negative value if the process
            was terminated (e.g. by timeout) rather than exiting normally.

            Will wait progressively longer if the download keeps timing out.

        TODO:
            Detect ping loop of death ? Or is timeout sufficient?
            Check for other issues, e.g. black 540p
            Logging
            Reset _pingouts?
            I need to check for both pinging and a timeout
            Because the ping message comes from librtmp, and that might not be part
            of ffmpeg
            Check periodically that the stream is still live:
                I've had a couple zombie streams even with the new system
                (no ffmpeg logs, so no idea what happened)
        """
        num_pings = 0
        # Some streams seem to start fine with up to 4 pings before beginning download?
        # More investigation is needed
        max_pings = 1 + self._pingouts
        # timeout after 1 minute
        timeout = datetime.datetime.now() + datetime.timedelta(minutes=1)
        try:
            for line in self._process.stderr:
                # TODO: add mpegts or other variants depending on the container settings? or no?
                # if "Output #0, mp4" in line:
                if "Output #0" in line:
                    self._process.communicate()
                    self.move_to_dest()
                    self._pingouts = 0
                    break
                elif "HandleCtrl, Ping" in line:
                    num_pings += 1
                if num_pings > max_pings:
                    # The main issue with this is that the slain processes will not have their files moved
                    # But I think this is preferable to the other solutions I've come up with.
                    # For future reference, those were:
                    #
                    # 1) Sending SIGINT then continuing to read stderr until it exited (sometimes it doesn't)
                    # 2) Sending SIGINT, storing a reference to the process, then restarting the download.
                    #    This prevents the process from being garbage collected until the Watcher is
                    # 3) Sending SIGINT, then storing info about src and dest paths for the stopped download.
                    #    If a reference to the process is NOT stored, there's no way to be sure it has finished writing
                    #    (if it's writing at all). The only way was to give them a grace period and then just start
                    #    moving, but this adds undesirable time to the cleanup phase, when we may want to restart
                    #    a falsely completed Watcher asap.
                    # 4) Just moving the file straightaway. This is obviously bad since ffmpeg takes a few moments to
                    #    finish.
                    # NOTE: only option #1 was actually tried, the others were partially written before being
                    # abandoned as their problems became clear
                    #
                    # Two additional options exist (not mutually exclusive):
                    # 1) Passing the dead processes off to a queue and having another thread clean up.
                    # 2) Having regular maintenance sweep the active folder and move files it can be sure are done
                    #    to their proper folders.
                    #
                    # I *probably* need to use 1) eventually, especially once I figure out how to actually end
                    # stuck processes without killing the parent. But it requires a lot more code.
                    # Until then let's just see how this works.
                    #
                    # When that time does come, a Downloader copy constructor may be useful.
                    download_logger.debug("Download pinged {} times: Stopping".format(num_pings))
                    self._pingouts += 1
                    self.stop()

                    # close stderr to force the loop to exit
                    time.sleep(0.1)
                    self._process.stderr.close()
                    time.sleep(0.1)
                    # process will be garbage collected when the next one is started, or the Watcher dies
                    # self._process = None
                # This *should* work for newer builds of FFmpeg without librtmp.
                # Only question is whether 1 minute is too long (or too short).
                # UPDATE: Why doesn't this ever seem to work?
                # is it because FFmpeg freezes output and hangs now? so we're never getting another line to iterate over
                # elif datetime.datetime.now() > timeout:
                #     download_logger.debug("Download of {} timed out".format(self.outfile))
                #     self.stop()
                #     time.sleep(0.1)
                #     self._process.stderr.close()
                #     time.sleep(0.1)
                else:
                    time.sleep(0.2)

        except ValueError:
            download_logger.debug('ffmpeg stderr closed unexpectedly')

        # Is it possible for the process to end prematurely?
        return self._process.returncode

    def stop(self):
        """Stop an active download.

        Returns immediately, check is_running() for success.
        """
        # trying this instead of SIGTERM
        # http://stackoverflow.com/a/6659191/3380530
        # self._process.send_signal(SIGINT)
        # Or not. SIGINT doesn't exist on Windows
        self._process.terminate()

    def kill(self):
        """Kill an active download.

        Like stop, only tries to kill the process instead of just terminating it.
        Only use this as a last resort, as it will render any video unusable."""
        self._process.kill()

    def move_to_dest(self):
        """Moves output file to its final destination."""
        destpath = self._move_to_dest(self.outfile, self.tempdir, self.destdir)

        if destpath:
            self.all_files.append(destpath)
            download_logger.info('Completed {}'.format(destpath))

        with self._lock:
            self.outfile = ""

    @staticmethod
    def _move_to_dest(outfile, tempdir, destdir):
        srcpath = '{}/{}'.format(tempdir, outfile)
        destpath = '{}/{}'.format(destdir, outfile)
        download_logger.debug('File transfer: {} -> {}'.format(srcpath, destpath))
        if os.path.exists(destpath):
            raise FileExistsError
        else:
            try:
                shutil.move(srcpath, destpath)
            except FileNotFoundError:
                download_logger.debug('File not found: {} -> {}'.format(srcpath, destpath))
                return
            else:
                return destpath

    def update_streaming_url(self):
        data = self._client.streaming_url(self._room.room_id)
        self._stream_data = data
        download_logger.debug('{}'.format(self._stream_data))

        # TODO: it shouldn't still attempt to start up without a fresh url
        if not data:
            return

        rtmp_streams = []
        hls_streams = []
        lhls_streams = []
        # TODO: sort according to a priority list defined in config file
        # e.g. ('rtmp', 'lhls', 'hls'), or just "rtmp" (infer the others from defaults)
        #
        for stream in data:
            if stream['type'] == 'rtmp':
                rtmp_streams.append((int(stream['quality']), '/'.join((stream['url'], stream['stream_name']))))
            elif stream['type'] == 'hls':
                hls_streams.append((int(stream['quality']), stream['url']))
            elif stream['type'] == 'lhls':
                lhls_streams.append((int(stream['quality']), stream['url']))
        try:
            new_rtmp_url = sorted(rtmp_streams)[-1][1]
        except IndexError as e:
            # download_logger.warn("Caught IndexError while reading RTMP url: {}\n{}".format(e, data))
            new_rtmp_url = ""

        try:
            new_hls_url = sorted(hls_streams)[-1][1]
        except IndexError as e:
            # download_logger.warn("Caught IndexError while reading HLS url: {}\n{}".format(e, data))
            new_hls_url = ""

        try:
            new_lhls_url = sorted(lhls_streams)[-1][1]
        except IndexError as e:
            # download_logger.warn("Caught IndexError while reading HLS url: {}\n{}".format(e, data))
            new_lhls_url = ""

        with self._lock:
            self._rtmp_url = new_rtmp_url
            self._hls_url = new_hls_url
            self._lhls_url = new_lhls_url

    # def update_streaming_url_web(self):
    #     """Updates streaming urls from the showroom website.

    #     Fallback if api changes again

    #     But pretty sure this doesn't work anymore
    #     """
    #     # TODO: add an endpoint for fetching the browser page
    #     r = self._client._session.get(self._room.long_url)

    #     if r.ok:
    #         match = hls_url_re1.search(r.text)
    #         # TODO: check if there was a match
    #         if not match:
    #             # no url found in the page
    #             # probably the stream has ended but is_live returned true
    #             # just don't update the urls
    #             # except what happens if they are still "" ?
    #             return
    #         hls_url = match.group(0)
    #         rtmps_url = match.group(1).replace('https', 'rtmps')
    #         rtmp_url = "rtmp://{}.{}.{}.{}:1935/liveedge/{}".format(*match.groups()[1:])
    #         with self._lock:
    #             self._rtmp_url = rtmp_url
    #             self._hls_url = hls_url
    #             self._rtmps_url = rtmps_url

    # def update_streaming_url_old(self):
    #     """Updates streaming urls from the showroom website."""
    #     data = self.client.json('https://www.showroom-live.com/room/get_live_data',
    #                             params={'room_id': self._room.room_id},
    #                             headers={'Referer': self._room.long_url})
    #     if not data:
    #         pass  # how to resolve this? can it even happen without throwing an exception earlier?
    #
    #     # TODO: Check that strings aren't empty
    #     stream_name = data['streaming_name_rtmp']
    #     stream_url = data["streaming_url_rtmp"]
    #     new_rtmp_url = '{}/{}'.format(stream_url, stream_name)
    #     new_hls_url = data["streaming_url_hls"]
    #
    #     with self._lock:
    #         if new_rtmp_url != self.rtmp_url:
    #             # TODO: log url change
    #             # TODO: Trigger this message when the stream first goes live, from elsewhere
    #             # print('Downloading {}\'s Showroom'.format(self.room.name))
    #             # self.announce((self.web_url, self.stream_url))
    #             pass
    #
    #         if new_hls_url != self.hls_url:
    #             # TODO: log url change
    #             pass
    #
    #         self._rtmp_url = new_rtmp_url
    #         self._hls_url = new_hls_url

    def start(self):
        """
        Starts the download.

        Refreshes the streaming url, generates a new file name, and starts a new ffmpeg
        process.

        Returns:
            datetime object representing the time the download started
        """
        tokyo_time = datetime.datetime.now(tz=TOKYO_TZ)

        # TODO: Does this work on Windows now?
        env = os.environ.copy()

        # remove proxy information
        for key in ('http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY'):
            env.pop(key, None)

        self.update_streaming_url()

        # TODO: rework this whole process to include lhls, and make it configurable
        # and less braindead
        if not self._protocol:
            self._protocol = 'rtmp'
        if not self._ffmpeg_container:
            self._ffmpeg_container = 'mp4'
        extra_args = []
        # Fall back to HLS if no RTMP stream available
        # Better to do this here or in update_streaming_url?
        # There's a possible race condition here, if some external thread modifies either of these
        if not self._rtmp_url and self._protocol == 'rtmp':
            download_logger.warn('Using HLS downloader for {}'.format(self._room.handle))
            self._protocol = 'hls'

            # extra_args = []
        # force using TS container with HLS
        # this is causing more problems than it solves
        # if self.protocol in ('hls', 'lhls'):
        #     self._ffmpeg_container = 'ts'

        # 2020-01-10: those problems were preferrable to completely unwatchable streams
        if self.protocol in ('hls', 'lhls'):
            extra_args = ["-copyts"]
            if self._ffmpeg_container == 'mp4':
                extra_args.extend(["-bsf:a", "aac_adtstoasc"])

        # I don't think this is needed?
        # if self._ffmpeg_container == 'ts':
        #     extra_args.extend(['-bsf:v', 'h264_mp4toannexb'])
        # elif self._ffmpeg_container != 'mp4':
        #     # TODO: support additional container formats, e.g. FLV
        #     self._ffmpeg_container = 'mp4'
        temp, dest, out = format_name(self._rootdir,
                                      strftime(tokyo_time, FULL_DATE_FMT),
                                      self._room, ext=self._ffmpeg_container)

        with self._lock:
            self.tempdir, self.destdir, self.outfile = temp, dest, out

        if self._logging is True:
            log_file = os.path.normpath('{}/logs/{}.log'.format(self.destdir, self.outfile))
            env.update({'FFREPORT': 'file={}:level=40'.format(log_file)})
            # level=48  is debug mode, with lots and lots of extra information
            # maybe too much
        normed_outpath = os.path.normpath('{}/{}'.format(self.tempdir, self.outfile))

        self._process = subprocess.Popen([
            self._ffmpeg_path,
            # '-nostdin',
            # '-nostats',  # will this omit any useful information?
            '-loglevel', '40',  # 40+ required for wait() to check output
            '-copytb', '1',
            '-rw_timeout', str(10*10**6),
            '-i', self.stream_url,
            '-c', 'copy',
            *extra_args,
            normed_outpath
        ],
            stdin=subprocess.DEVNULL,
            stderr=subprocess.PIPE,  # ffmpeg sends all output to stderr
            universal_newlines=True,
            bufsize=1,
            env=env)