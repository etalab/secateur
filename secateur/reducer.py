import csv
import logging
import os
import re

from nameko.events import event_handler

from .constants import RESULTS_FOLDER, STATUS_REDUCE, STATUS_COMPLETE
from .logger import LoggingDependency
from .storages import RedisStorage
from .tools import guess_encoding

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
        # Only guess the encoding over the first bytes of the file.
        encoding = guess_encoding(open(file_name, 'rb').read(2048))
        with open(file_name, encoding=encoding) as csvfile_in,\
                open(file_name_out, 'w') as csvfile_out:
            # Documention suggests 1024 but it's not enough for
            # CSV files with many columns like:
            # https://www.data.gouv.fr/storage/f/2014-03-31T09-49-28/
            # muni-2014-resultats-com-1000-et-plus-t2.txt
            # So we double it to be sure to have enough data to sniff.
            # https://docs.python.org/3/library/csv.html#csv.Sniffer
            dialect = csv.Sniffer().sniff(csvfile_in.read(2048))
            csvfile_in.seek(0)
            reader = csv.DictReader(csvfile_in, dialect=dialect)
            writer = csv.DictWriter(
                csvfile_out, fieldnames=reader.fieldnames, dialect=dialect)
            writer.writerow(dict(zip(writer.fieldnames, writer.fieldnames)))
            for row in reader:
                if all(row.get(column) == value for column, value in filters):
                    # Happens when there are fewer fieldnames than columns.
                    if None in row:
                        del row[None]
                    writer.writerow(row)
            self.storage.set_status(url_hash, STATUS_COMPLETE)
