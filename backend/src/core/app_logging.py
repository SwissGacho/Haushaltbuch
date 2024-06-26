""" Application specific logging
"""

import logging
from core.const import APPNAME

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
app_logger = logging.getLogger(APPNAME)
app_logger.setLevel(logging.DEBUG)

root_handler = logging.StreamHandler()
root_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
root_logger.addHandler(root_handler)

root_logger.debug("root logger initialized.")
app_logger.debug("app logger initialized.")


def getLogger(name: str, level=logging.NOTSET) -> None:
    return logging.getLogger(APPNAME if name == "__main__" else (APPNAME + "." + name))
