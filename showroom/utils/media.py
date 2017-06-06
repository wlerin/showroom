# copied from social48.tools
# in the future, import from that project directly

from mimetypes import guess_type
import requests
import tempfile
import os
import time
import hashlib
import shutil

__all__ = ['checksum', 'guess_mimetype', 'extension', 'save_from_url']

# TODO: rename this module so it doesn't clash with variables named "media"
_extension_lookup = {
    "image/jpeg": "jpg",
    "image/png": "png",
    "image/gif": "gif",
    "video/mp4": "mp4",
    "video/mpeg4": "mp4"
}


# http://stackoverflow.com/a/24847608/3380530
def hashsum(hash, filename):
    with open(filename, 'rb') as infp:
        for chunk in iter(lambda: infp.read(128 * hash.block_size), b""):
            hash.update(chunk)
    return hash.hexdigest()


def md5sum(filename):
    md5hash = hashlib.md5()
    return hashsum(md5hash, filename)


def blake2bsum(filename):
    b2bhash = hashlib.blake2b(digest_size=32)
    try:
        return hashsum(b2bhash, filename)
    except FileNotFoundError:
        return ""


checksum = blake2bsum


def guess_mimetype(url):
    """
    Tries to guess the file type based on extension, assumes jpeg if it can't
    """
    guess = guess_type(url)[0]
    return guess if guess else 'image/jpeg'


def extension(mimetype):
    return _extension_lookup.get(mimetype, 'UNKNOWN')


class MediaFileExistsError(FileExistsError):
    def __init__(self, *args, size_bytes, checksum, tempfile):
        super().__init__(self, *args)
        self.size_bytes = size_bytes
        self.checksum = checksum
        self.tempfile = tempfile


class MediaURLNotFound(Exception):
    pass


def save_from_url(url, outfile, tmpdir=None, skip_exists=False, raise_exists=False):
    """
    Simple media file saver.

    Saves directly accessed videos and images to a specified directory.

    :param url: URL to download from
    :param outfile: Destination to save to
    :param tmpdir: Temp directory (e.g. to utilise SSD storage)
    :param skip_exists: Checks if the file already exists before downloading, use with caution

    :return: A tuple with two elements:
        The bytes written if successful, else 0
        The checksum, if successful, else ""

    Raises:
        MediaURLNotFound: URL 404s
        MediaFileExistsError: 
            raised when raise_exists and the destination file exists and checksums do not match
            stores the tempfile path, file size, and checksum as tempfile, size_bytes, and checksum attributes
        Also may raise any exception raised by requests.Response.raise_for_status()
    """
    # TODO: return code
    if '/' in outfile:
        destdir = outfile.rsplit('/', 1)[0]
        os.makedirs(destdir, exist_ok=True)

    attempts = 0
    max_attempts = 5
    if skip_exists:
        if os.path.exists(outfile):
            return os.path.getsize(outfile), checksum(outfile)

    while True:
        try:
            r = requests.get(url, timeout=(3, 10), stream=True)
            r.raise_for_status()
        except requests.exceptions.HTTPError as e:
            # TODO: handle other meaningful error codes
            if e.response.status_code == 404:
                raise MediaURLNotFound
            attempts += 1
            error = str(e)
            wait = 2 ** attempts
        except (requests.exceptions.Timeout, requests.exceptions.ConnectTimeout) as e:
            attempts += 1
            error = str(e)
            wait = 2 ** attempts * 0.5
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            attempts += 1
            max_attempts += 1
            error = str(e)
            wait = 3 ** attempts * 5
        else:
            fno, tmppath = tempfile.mkstemp(dir=tmpdir)
            bytes_written = 0
            with open(tmppath, 'wb') as outfp:
                for chunk in r.iter_content(chunk_size=2048):
                    bytes_written += outfp.write(chunk)
            os.close(fno)
            cs = checksum(tmppath)

            # TODO: test if outfile exists, if so compare first
            if os.path.exists(outfile):
                if checksum(outfile) == cs:
                    os.remove(tmppath)
                else:
                    err = "{} exists.\nCheck {} for downloaded file".format(outfile, tmppath)
                    if raise_exists:
                        raise MediaFileExistsError(err, size_bytes=bytes_written, checksum=cs, tempfile=tmppath)
                    else:
                        print(err)
                    bytes_written = 0
            else:
                shutil.move(tmppath, outfile)
            return bytes_written, cs

        if attempts < max_attempts:
            time.sleep(wait)  # this definitely needs to happen in another thread
        else:
            return 0, ""
