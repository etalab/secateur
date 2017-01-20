import os

POPULAR_DELIMITER = ';'

STATUS_DOWNLOAD = 3
STATUS_REDUCE = 6
STATUS_COMPLETE = 10

RESULTS_FOLDER = os.path.join('downloads', 'results')
if not os.path.exists(RESULTS_FOLDER):  # Initialization only.
    os.makedirs(RESULTS_FOLDER)

SOURCES_FOLDER = os.path.join('downloads', 'sources')
if not os.path.exists(SOURCES_FOLDER):  # Initialization only.
    os.makedirs(SOURCES_FOLDER)
