""" Application specific logging
"""

import logging

APPNAME = "moneypilot"
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
root_logger.addHandler(ch)


def getLogger(name: str, level=logging.NOTSET) -> None:
    return logging.getLogger(APPNAME if name == "__main__" else (APPNAME + "." + name))
