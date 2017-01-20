import codecs
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

    def guess_encoding(self, file_path):
        """Guess encoding of `file_path` using chardet."""
        detector = UniversalDetector()
        for line in open(file_path, 'rb'):
            # TODO: set a hard limit on the iteration to avoid at all
            # cost to iter over the entire file if huge?
            detector.feed(line)
            if detector.done:
                break
        detector.close()
        return detector.result['encoding']

    def sniff_dialect(self, csvfile_in, extract_length=4096):
        """Sniff dialect from `csvfile_in` with `extract_length` data."""
        try:
            dialect = csv.Sniffer().sniff(csvfile_in.read(extract_length))
        except csv.Error:
            dialect = csv.unix_dialect()
        csvfile_in.seek(0)  # Otherwise we miss the first bytes after.
        return dialect

    def write_bom(self, csvfile_out, encoding):
        """Make Excel happy with UTF-8 using a BOM."""
        if encoding.startswith('utf-8'):
            csvfile_out.write(codecs.BOM_UTF8.decode('utf-8'))

    def write_row(self, writer, row, filters):
        """The `row` is written given matching `filters`."""
        if all(row.get(column) == value for column, value in filters):
            if None in row:
                # Happens when there are fewer fieldnames than columns,
                # at some point we should inform the user we truncate.
                del row[None]
            writer.writerow(row)

    @event_handler('url_downloader', 'file_to_reduce')
    def reduce_file(self, job_data):
        file_path = os.path.join(SOURCES_FOLDER, job_data['url_hash'])
        job_hash = job_data['job_hash']
        file_path_out = os.path.join(RESULTS_FOLDER, job_hash)
        log('Reducing {file_path} to {file_path_out}'.format(
            file_path=file_path, file_path_out=file_path_out))
        from_cache = file_exists(file_path_out) and not job_data['force']
        if from_cache:
            log('Fetching from cache {file_path_out}'.format(
                file_path_out=file_path_out))
            self.storage.set_status(job_hash, STATUS_COMPLETE)
            return
        self.storage.set_status(job_hash, STATUS_REDUCE)
        encoding = self.guess_encoding(file_path)
        with open(file_path, encoding=encoding) as csvfile_in,\
                open(file_path_out, 'w') as csvfile_out:
            dialect = self.sniff_dialect(csvfile_in)
            self.write_bom(csvfile_out, encoding)
            reader = csv.DictReader(csvfile_in, dialect=dialect)
            writer = csv.DictWriter(
                csvfile_out, fieldnames=reader.fieldnames, dialect=dialect)
            writer.writeheader()
            for row in reader:
                self.write_row(writer, row, job_data['filters'])
            self.storage.set_status(job_hash, STATUS_COMPLETE)
