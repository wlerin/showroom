"""
Requires PySceneDetect, which in turn requires numpy and opencv compiled with ffmpeg support

steps: 
1) merge the video if necessary
2) find first scene change
3) find previous iframe
4) optionally trim silence from end of video?
5) cut the video

in the future, do this separately from archive merging, perhaps through a handler running in the Controller

https://superuser.com/questions/554620/how-to-get-time-stamp-of-closest-keyframe-before-a-given-timestamp-with-ffmpeg

ffprobe -select_streams v -show_frames <INPUT> 


"""

from types import SimpleNamespace
import subprocess
from showroom.archive.probe import get_iframes2
import os.path
from .constants import ffmpeg


class DumbNamespace(SimpleNamespace):
    def __getattr__(self, name):
        try:
            return super().__getattr__(name)
        except AttributeError:
            return None


def detect_first_scene(path, start_minutes=0, end_minutes=12, threshold=20.0):
    """
    Detect transition from static image into the actual program.
    
    Requires PySceneDetect and OpenCV compiled with FFmpeg support.
    
    :param path: path to file
    :param start_minutes: when to start looking
    :param end_minutes: when to stop looking
    :param threshold: how big a change in frames to detect
    :return: 
    """
    import scenedetect
    # detect_scenes_file is unfortunately not really designed to be used like this
    # it's tightly coupled to the command line arguments passed by scenedetect.cli
    # TODO: Rewrite the necessary PySceneDetect functions so they aren't retarded.
    # or write my own detector that stops after finding a match, see detect_threshold
    scene_detectors = scenedetect.detectors.get_available()
    args = DumbNamespace(threshold=threshold,
                         detection_method='content',
                         downscale_factor=2,
                         start_time=[0, start_minutes, 0],
                         duration=[0, end_minutes, 0],
                         quiet_mode=True,
                         # end custom arguments, begin defaults
                         min_scene_len=15,
                         frame_skip=0)
    scene_manager = scenedetect.manager.SceneManager(args=args, scene_detectors=scene_detectors)

    video_fps, frames_read, frames_processed = scenedetect.detect_scenes_file(path, scene_manager)

    scene_list_sec = [x / float(video_fps) for x in scene_manager.scene_list]

    return scene_list_sec[0]


def detect_start_iframe(path, max_pts_time):
    search_interval = '{}%{}'.format(max(0.0, max_pts_time - 60.0), max_pts_time)

    iframes = get_iframes2(path, search_interval)

    return iframes[-1]


def detect_end_of_video(path, min_pts_time):
    # find the
    pass


def detect_threshold(path):
    # find the ideal threshold to use for content detection
    # or alternatively write a different detector that looks at more than just two frames
    pass


def trim_video(srcpath, destpath, start_pts_time, end_pts_time=None):
    args = [ffmpeg]
    if not (start_pts_time is None or int(float(start_pts_time)) == 0):
        args.extend(['-ss', start_pts_time])
    args.extend(['-i', srcpath])
    if end_pts_time:
        args.extend(['-to', end_pts_time - start_pts_time])
    args.extend([
        '-c', 'copy',
        '-movflags', '+faststart',
        '-avoid_negative_ts', 'make_zero',
        destpath
    ])
    try:
        p = subprocess.Popen(
            args,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
    except TypeError:
        print(srcpath, destpath, args)
        raise
    result = p.communicate()
    # TODO: parse result?


def trim_videos(video_list, output_dir, trim_start):
    # find start iframe
    for video in video_list:
        read_interval = '%{}'.format(trim_start)
        iframes = get_iframes2(video, read_interval)
        start_pts = iframes[-1] if iframes else None
        video_name = os.path.split(video)[-1]
        output_path = os.path.join(output_dir, video_name)
        print('Trimming {} from {} -> {}'.format(video, start_pts, output_path))
        trim_video(video, output_path, start_pts)
