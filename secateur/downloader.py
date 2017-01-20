import logging
import os
import re

import requests
from contextlib import closing

from nameko.events import event_handler, EventDispatcher

from .constants import SOURCES_FOLDER, STATUS_DOWNLOAD
from .logger import LoggingDependency
from .storages import RedisStorage
from .tools import file_exists

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

    def download_file_by_chunk(self, url, file_path, chunk_size=1024):
        """Download streamed `url` to `file_path` by `chunk_size`."""
        with closing(session.get(url, stream=True)) as response:
            # TODO: to prevent huge file download?
            # if int(response.headers['content-length']) < TOO_LONG:
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)

    @event_handler('http_server', 'url_to_download')
    def download_url(self, params):
        url = params['url']
        log('Downloading {url}'.format(url=url))
        if not url_pattern.match(url):
            logging.error('Error with {url}: not a URL'.format(url=url))
            return
        file_path = os.path.join(SOURCES_FOLDER, params['url_hash'])
        from_cache = file_exists(file_path) and not params['force_download']
        if from_cache:
            log('Fetching from cache {file_path}'.format(file_path=file_path))
        else:
            self.storage.set_status(params['job_hash'], STATUS_DOWNLOAD)
            try:
                self.download_file_by_chunk(url, file_path)
            except Exception as e:
                logging.error('Error with {url}: {e}'.format(url=url, e=e))
                return
        log('Dispatching reduce of {file_path}'.format(file_path=file_path))
        self.dispatch('file_to_reduce', params)
