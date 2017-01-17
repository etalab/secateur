import csv
import logging
import os
import re

from chardet.universaldetector import UniversalDetector
from nameko.events import event_handler

from .constants import (
    RESULTS_FOLDER, STATUS_REDUCE, STATUS_COMPLETE, SOURCES_FOLDER
)
from .logger import LoggingDependency
from .storages import RedisStorage
from .tools import file_exists

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
    def reduce_file(self, job_data):
        file_path = os.path.join(SOURCES_FOLDER, job_data['url_hash'])
        log('Reducing {file_path}'.format(file_path=file_path))
        job_hash = job_data['job_hash']
        file_path_out = os.path.join(RESULTS_FOLDER, job_hash)
        if file_exists(file_path_out):
            log('Fetching from cache {file_path}'.format(file_path=file_path))
            self.storage.set_status(job_hash, STATUS_COMPLETE)
            return
        self.storage.set_status(job_hash, STATUS_REDUCE)
        # Guess encoding using chardet.
        detector = UniversalDetector()
        for line in open(file_path, 'rb'):
            detector.feed(line)
            if detector.done:
                break
        detector.close()
        encoding = detector.result['encoding']
        with open(file_path, encoding=encoding) as csvfile_in,\
                open(file_path_out, 'w') as csvfile_out:
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
                if all(row.get(column) == value
                       for column, value in job_data['filters']):
                    # Happens when there are fewer fieldnames than columns.
                    if None in row:
                        del row[None]
                    writer.writerow(row)
            self.storage.set_status(job_hash, STATUS_COMPLETE)
