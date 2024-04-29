import sys
import colorama
import logging

from pathlib import Path
from functools import wraps
from datetime import datetime as dt
from colorama import Fore, Back, Style
from typing import Optional, Literal

colorama.init()


def verbose_level_switcher(verbose):
    return {
        0: logging.CRITICAL,
        1: logging.ERROR,
        2: logging.WARNING,
        3: logging.INFO,
        4: logging.DEBUG
    }[verbose]


def init_logger(level=logging.INFO, logger=logging.getLogger(),
                file_prefix: Optional[str] = None, outdir='log'):
    logger.setLevel(level)

    if file_prefix:
        Path(outdir).mkdir(parents=True, exist_ok=True)
        date = dt.now().strftime('%Y-%m-%d-%H-%M-%S-%f')
        filename = f'{file_prefix}-{date}.log'
        file_handler = logging.FileHandler(Path(outdir) / filename)
        log_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(threadName)s: %(message)s")
        file_handler.setFormatter(log_formatter)
        logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(LogFormatter())
    logger.addHandler(console_handler)


class LogFormatter(logging.Formatter):
    def __init__(self, style: Literal["%", "{", "$"] = '{'):
        logging.Formatter.__init__(self, style=style)

    def format(self, record):
        stdout_template = '{levelname}' + Fore.RESET + \
            '] {asctime}@{threadName}: ' + '{message}'
        stdout_head = '[%s'

        all_formats = {
            logging.DEBUG: logging.StrFormatStyle(stdout_head % Fore.LIGHTBLUE_EX + stdout_template),
            logging.INFO: logging.StrFormatStyle(stdout_head % Fore.GREEN + stdout_template),
            logging.WARNING: logging.StrFormatStyle(stdout_head % Fore.LIGHTYELLOW_EX + stdout_template),
            logging.ERROR: logging.StrFormatStyle(stdout_head % Fore.LIGHTRED_EX + stdout_template),
            logging.CRITICAL: logging.StrFormatStyle(
                stdout_head % Fore.RED + stdout_template)
        }

        self._style = all_formats.get(
            record.levelno, logging.StrFormatStyle(logging._STYLES['{'][1]))    # TODO
        self._fmt = self._style._fmt
        result = logging.Formatter.format(self, record)
        return result
