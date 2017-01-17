import csv
import logging
import os
import re

from nameko.events import event_handler

from .constants import RESULTS_FOLDER, STATUS_REDUCE, STATUS_COMPLETE
from .logger import LoggingDependency
from .storages import RedisStorage

# See https://github.com/kvesteri/validators for reference.
url_pattern = re.compile(
    r'^[a-z]+://([^/:]+\.[a-z]{2,10}|([0-9]{{1,3}}\.)'
    r'{{3}}[0-9]{{1,3}})(:[0-9]+)?(\/.*)?$'
)

log = logging.info


class ReducerService(object):
    name = 'file_reducer'
    storage = RedisStorage()
    logger = LoggingDependency()

    @event_handler('url_downloader', 'file_to_reduce')
    def reduce_file(self, file_name_filters):
        file_name, filters = file_name_filters
        log('Reducing {file_name}'.format(file_name=file_name))
        url_hash = os.path.split(file_name)[-1]
        file_name_out = os.path.join(RESULTS_FOLDER, url_hash)
        if os.path.exists(file_name_out):
            log('Fetching from cache {file_name}'.format(file_name=file_name))
            self.storage.set_status(url_hash, STATUS_COMPLETE)
            return
        self.storage.set_status(url_hash, STATUS_REDUCE)
        with open(file_name, encoding='cp1252') as csvfile_in,\
                open(file_name_out, 'w') as csvfile_out:
            reader = csv.DictReader(csvfile_in, delimiter=str(';'))
            writer = csv.DictWriter(csvfile_out, fieldnames=reader.fieldnames)
            writer.writerow(dict(zip(writer.fieldnames, writer.fieldnames)))
            for row in reader:
                if all(row[column] == value for column, value in filters):
                    # Happens when fewer fieldnames than columns.
                    if None in row:
                        del row[None]
                    writer.writerow(row)
            self.storage.set_status(url_hash, STATUS_COMPLETE)
