import hashlib
import os
import mimetypes
from time import time
from zlib import adler32

from werkzeug.datastructures import Headers
from werkzeug.wrappers import Response
from werkzeug.wsgi import wrap_file


SEND_FILE_MAX_AGE_DEFAULT = 43200  # 12 hours, default from Flask.


def send_file(request, filename, attachment_filename):
    """Simplified from Flask to add appropriated headers."""
    headers = Headers()
    headers.add('Content-Disposition', 'attachment',
                filename=attachment_filename)
    headers['Content-Length'] = os.path.getsize(filename)
    data = wrap_file(request.environ, open(filename, 'rb'))
    mimetype = mimetypes.guess_type(attachment_filename)[0] \
        or 'application/octet-stream'
    response = Response(data, mimetype=mimetype, headers=headers,
                        direct_passthrough=True)
    response.last_modified = os.path.getmtime(filename)
    response.cache_control.public = True
    response.cache_control.max_age = SEND_FILE_MAX_AGE_DEFAULT
    response.expires = int(time() + SEND_FILE_MAX_AGE_DEFAULT)
    response.set_etag('%s-%s-%s' % (
        os.path.getmtime(filename),
        os.path.getsize(filename),
        adler32(filename.encode('utf-8')) & 0xffffffff
    ))
    return response


def generate_hash(query_string):
    """Custom hash to avoid long values."""
    return hashlib.md5(query_string.encode('utf-8')).hexdigest()[:10]


def guess_encoding(data):
    """Return the guessed encoding from data.

    chardet is not used because of weird results, we want to fallback on
    latin-1 if it fails to read ASCII and UTF-8, not some greek encoding!
    Source: http://unicodebook.readthedocs.io/guess_encoding.html

    Check for instance:
    https://www.data.gouv.fr/storage/f/2014-03-31T09-49-28/
    muni-2014-resultats-com-1000-et-plus-t2.txt
    """
    def _isASCII(data):
        try:
            data.decode('ASCII')
        except UnicodeDecodeError:
            return False
        else:
            return True

    def _isUTF8Strict(data):
        try:
            decoded = data.decode('UTF-8')
        except UnicodeDecodeError:
            return False
        else:
            for ch in decoded:
                if 0xD800 <= ord(ch) <= 0xDFFF:
                    return False
            return True

    if _isASCII(data):
        return 'ascii'
    elif _isUTF8Strict(data):
        return 'utf-8'
    else:
        return 'latin-1'
