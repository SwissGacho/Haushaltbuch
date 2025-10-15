"""Application specific logging"""

import logging
from core.const import APPNAME

# Control logging of module entry and exit
_LOG_MODULE_ENTRY = False
_LOG_MODULE_EXIT = False


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


# pylint: disable=invalid-name,unused-argument
def getLogger(name: str, level=logging.NOTSET) -> logging.Logger:
    "Create module specific logger and log potentially module code entry (when module is imported)"
    logger = logging.getLogger(
        APPNAME if name == "__main__" else (APPNAME + "." + name)
    )

    print(f"{logger.handlers=}")
    if not logger.handlers:
        ch = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(name)-60s:%(lineno)4d - %(levelname)-5s - %(message)s"
        )
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.setLevel(level or logging.DEBUG)
    logger.propagate = False

    if _LOG_MODULE_ENTRY:
        logger.debug("Enter module")
    print(f"{logger.handlers=}")
    return logger


def log_exit(logger):
    "Log end of execution of the modole code"
    if _LOG_MODULE_EXIT:
        logger.debug("Exit module")
