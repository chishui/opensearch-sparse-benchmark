import os
import logging
from datetime import datetime
from rich.logging import RichHandler

DEBUG = os.environ.get("DEBUG", "0") == "1"
LOG_DIR = os.environ.get("LOG_DIR", "logs")


def create_console_logger():
    """Create a logger with Rich console output (terminal only)."""
    log = logging.getLogger('sparse-ann-console')
    log.setLevel(logging.DEBUG if DEBUG else logging.INFO)

    if log.handlers:
        log.handlers.clear()

    rich_handler = RichHandler(rich_tracebacks=True, markup=True)
    rich_handler.setFormatter(logging.Formatter('%(message)s'))
    log.addHandler(rich_handler)

    return log


def create_file_logger(name=None):
    """Create a logger with file output only."""
    if name is None:
        name = datetime.now().strftime("%Y%m%d_%H%M%S")

    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)

    if log.handlers:
        log.handlers.clear()

    os.makedirs(LOG_DIR, exist_ok=True)
    file_path = os.path.join(LOG_DIR, f"{name}.log")
    file_handler = logging.FileHandler(file_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))
    log.addHandler(file_handler)

    return log


# Default console logger (terminal output with Rich formatting)
logger = create_console_logger()

# File logger (plain text to file)
file_logger = create_file_logger('sparse-benchmark')
