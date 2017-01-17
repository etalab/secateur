import json
import logging
import os
from http.client import ACCEPTED, CREATED, NOT_FOUND

from nameko.events import EventDispatcher
from nameko.rpc import rpc
from nameko.web.handlers import http

from .constants import RESULTS_FOLDER, STATUS_COMPLETE
from .logger import LoggingDependency
from .storages import RedisStorage
from .tools import generate_hash, send_file

log = logging.info
PRECONDITION_REQUIRED = 428


class HttpService(object):
    name = 'http_server'
    dispatch = EventDispatcher()
    storage = RedisStorage()
    logger = LoggingDependency(interval='ms')

    @http('GET', '/process')
    def process_url(self, request):
        force = bool(int(request.args.get('force', 0)))
        url = request.args.get('url')
        filters = list(zip(request.args.getlist('column'),
                           request.args.getlist('value')))
        log('Downloading url {url} to reduce with {filters}'.format(
            url=url, filters=filters))
        query_string = request.query_string.decode('utf-8')
        job_hash = generate_hash(query_string)
        url_hash = generate_hash(url)
        if force or not self.storage.get_status(job_hash):
            self.download({
                'url': url,
                'filters': filters,
                'job_hash': job_hash,
                'url_hash': url_hash,
                'force': force
            })
        return ACCEPTED, json.dumps({'hash': job_hash}, indent=2)

    @http('GET', '/status/<job_hash>')
    def check_status_from_hash(self, request, job_hash):
        log('Retrieving url hash {hash}'.format(hash=job_hash))
        status = self.storage.get_status(job_hash)
        if status is None:
            return NOT_FOUND, ''
        else:
            status = int(status)
        if status == STATUS_COMPLETE:
            return CREATED, ''
        else:
            return PRECONDITION_REQUIRED, ''

    @http('GET', '/file/<job_hash>')
    def retrieve_file_from_hash(self, request, job_hash):
        log('Retrieving file with hash {hash}'.format(hash=job_hash))
        if not int(self.storage.get_status(job_hash)) == STATUS_COMPLETE:
            return NOT_FOUND, ''
        csvfile_out = os.path.join(RESULTS_FOLDER, job_hash)
        attachment_filename = '{job_hash}.csv'.format(job_hash=job_hash)
        return send_file(
            request, csvfile_out, attachment_filename=attachment_filename)

    @rpc
    def download(self, job_data):
        log('Dispatching download of {url}'.format(url=job_data['url']))
        self.dispatch('url_to_download', job_data)
