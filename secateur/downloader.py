import logging
import os
import re

import requests
from contextlib import closing

from nameko.events import event_handler, EventDispatcher

from .constants import SOURCES_FOLDER, STATUS_DOWNLOAD
from .logger import LoggingDependency
from .storages import RedisStorage

# See https://github.com/kvesteri/validators for reference.
url_pattern = re.compile(
    r'^[a-z]+://([^/:]+\.[a-z]{2,10}|([0-9]{{1,3}}\.)'
    r'{{3}}[0-9]{{1,3}})(:[0-9]+)?(\/.*)?$'
)

log = logging.info
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=50)
session.mount('http://', adapter)


class DownloaderService(object):
    name = 'url_downloader'
    dispatch = EventDispatcher()
    storage = RedisStorage()
    logger = LoggingDependency()

    @event_handler('http_server', 'url_to_download')
    def download_url(self, url_and_filters):
        url, filters, url_hash = url_and_filters
        log('Downloading {url}'.format(url=url))
        if not url_pattern.match(url):
            logging.error('Error with {url}: not a URL'.format(url=url))
            return
        file_name = os.path.join(SOURCES_FOLDER, url_hash)
        if os.path.exists(file_name):
            log('Fetching from cache {file_name}'.format(file_name=file_name))
            self.dispatch('file_to_reduce', (file_name, filters))
            return
        self.storage.set_status(url_hash, STATUS_DOWNLOAD)
        try:
            with closing(session.get(url, stream=True)) as response:
                # TODO: to prevent huge file download?
                # if int(response.headers['content-length']) < TOO_LONG:
                with open(file_name, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            f.write(chunk)
        except Exception as e:
            logging.error('Error with {url}: {e}'.format(url=url, e=e))
            return
        self.dispatch('file_to_reduce', (file_name, filters))
