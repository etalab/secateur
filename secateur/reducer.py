import codecs
import csv
import logging
import os
import re

from chardet.universaldetector import UniversalDetector
from nameko.events import event_handler

from .constants import (
    RESULTS_FOLDER, STATUS_REDUCE, STATUS_COMPLETE, SOURCES_FOLDER,
    POPULAR_DELIMITER
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
        for i, line in enumerate(open(file_path, 'rb')):
            detector.feed(line)
            # Refrain chardet from reading the whole file!
            if detector.done or i > 1000:
                break
        detector.close()
        return detector.result['encoding']

    def sniff_dialect(self, csvfile_in, extract_length=4096):
        """Sniff dialect from `csvfile_in` with `extract_length` data."""
        extract = csvfile_in.read(extract_length)
        try:
            dialect = csv.Sniffer().sniff(extract)
        except csv.Error:
            dialect = csv.unix_dialect()
            # Poor-man guessing of the popular delimiter.
            # The magic number `5` has to be tested across real files.
            if len(extract.split(POPULAR_DELIMITER)) >= 5:
                dialect.delimiter = POPULAR_DELIMITER
        csvfile_in.seek(0)  # Otherwise we miss the first bytes after.
        return dialect

    def write_bom(self, csvfile_out, encoding):
        """Make Excel happy with UTF-8 using a BOM."""
        if encoding.startswith('utf-8'):
            csvfile_out.write(codecs.BOM_UTF8.decode('utf-8'))

    def write_row(self, writer, row, filters):
        """The `row` is written given matching `filters`."""
        try:
            # We start at 1 for counting columns on the API for a better
            # understanding of users but Python's lists start at 0,
            # hence the -1 applied.
            if all(row[int(column) - 1] == value for column, value in filters):
                writer.writerow(row)
        except KeyError:
            pass

    def write_dict_row(self, writer, row, filters):
        """The `row` from dict is written given matching `filters`."""
        if all(row.get(column) == value for column, value in filters):
            if None in row:
                # Happens when there are fewer fieldnames than columns,
                # at some point we should inform the user we truncate.
                del row[None]
            writer.writerow(row)

    @event_handler('url_downloader', 'file_to_reduce')
    def reduce_file(self, params):
        file_path = os.path.join(SOURCES_FOLDER, params['url_hash'])
        job_hash = params['job_hash']
        file_path_out = os.path.join(RESULTS_FOLDER, job_hash)
        log('Reducing {file_path} to {file_path_out}'.format(
            file_path=file_path, file_path_out=file_path_out))
        from_cache = file_exists(file_path_out) and not params['force_reduce']
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
            if params['no_headers']:
                reader = csv.reader(csvfile_in, dialect=dialect)
                writer = csv.writer(csvfile_out, dialect=dialect)
                for row in reader:
                    self.write_row(writer, row, params['filters'])
            else:
                reader = csv.DictReader(csvfile_in, dialect=dialect)
                writer = csv.DictWriter(
                    csvfile_out, fieldnames=reader.fieldnames, dialect=dialect)
                writer.writeheader()
                for row in reader:
                    self.write_dict_row(writer, row, params['filters'])
            self.storage.set_status(job_hash, STATUS_COMPLETE)
