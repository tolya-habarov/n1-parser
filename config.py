import os
import sys
import logging
from pathlib import Path


def config_logger(logger):
    console_handler = logging.StreamHandler(sys.stdout)
    file_path = Path(__file__).resolve().parent / os.environ.get('LOG_FILE_NAME')
    file_handler = logging.FileHandler(str(file_path))
    console_handler.setLevel(logging.DEBUG)
    file_handler.setLevel(logging.INFO)

    log_format = '%(asctime)s|%(name)s|%(levelname)s|%(message)s'
    console_handler.setFormatter(logging.Formatter(log_format))
    file_handler.setFormatter(logging.Formatter(log_format))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)