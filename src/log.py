import logging
import logging.config
from typing import Callable

from config import BASE_DIR

LOG_DIR = BASE_DIR / "log"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_DIR / "bittorrent_client.log", maxBytes=5000000, backupCount=1
        ),
        logging.StreamHandler(),
    ],
)


def logged(cls) -> Callable:
    "Class decorator for logging purposes"

    cls.logger = logging.getLogger(
        cls.__qualname__,
    )

    return cls


master = logging.getLogger(
    "master",
)
