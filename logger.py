import logging
from pathlib import Path
from datetime import datetime

from config.settings import LOG_DIR, LOG_LEVEL


def get_logger(
    name: str = "ETL"
) -> logging.Logger:
    """
    Create and return application logger.
    """


    logger = logging.getLogger(name)


    # Prevent duplicate handlers
    if logger.handlers:

        return logger


    logger.setLevel(
        getattr(
            logging,
            LOG_LEVEL.upper()
        )
    )


    # Ensure log folder exists

    Path(LOG_DIR).mkdir(
        parents=True,
        exist_ok=True
    )


    log_file = (
        Path(LOG_DIR)
        /
        f"etl_{datetime.now():%Y%m%d}.log"
    )


    formatter = logging.Formatter(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(name)s | "
        "%(message)s"
    )


    # File handler

    file_handler = logging.FileHandler(
        log_file,
        encoding="utf-8"
    )

    file_handler.setFormatter(
        formatter
    )


    # Console handler

    console_handler = logging.StreamHandler()

    console_handler.setFormatter(
        formatter
    )


    logger.addHandler(
        file_handler
    )

    logger.addHandler(
        console_handler
    )


    return logger
