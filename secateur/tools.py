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
