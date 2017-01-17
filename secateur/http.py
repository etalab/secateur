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
        url = request.args.get('url')
        filters = list(zip(request.args.getlist('column'),
                           request.args.getlist('value')))
        log('Downloading url {url} to reduce with {filters}'.format(
            url=url, filters=filters))
        query_string = request.query_string.decode('utf-8')
        url_hash = generate_hash(query_string)
        if not self.storage.get_status(url_hash):
            self.download(url, filters, url_hash)
        return ACCEPTED, json.dumps({'hash': url_hash}, indent=2)

    @http('GET', '/status/<url_hash>')
    def check_status_from_hash(self, request, url_hash):
        log('Retrieving url hash {hash}'.format(hash=url_hash))
        status = self.storage.get_status(url_hash)
        if status is None:
            return NOT_FOUND, ''
        else:
            status = int(status)
        if status == STATUS_COMPLETE:
            return CREATED, ''
        else:
            return PRECONDITION_REQUIRED, ''

    @http('GET', '/file/<url_hash>')
    def retrieve_file_from_hash(self, request, url_hash):
        log('Retrieving file with hash {hash}'.format(hash=url_hash))
        if not int(self.storage.get_status(url_hash)) == STATUS_COMPLETE:
            return NOT_FOUND, ''
        csvfile_out = os.path.join(RESULTS_FOLDER, url_hash)
        attachment_filename = '{url_hash}.csv'.format(url_hash=url_hash)
        return send_file(
            request, csvfile_out, attachment_filename=attachment_filename)

    @rpc
    def download(self, url, filters, url_hash):
        log('Downloading {url}'.format(url=url))
        self.dispatch('url_to_download', (url, filters, url_hash))
