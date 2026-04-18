"""Application specific logging"""

import json
import logging
import re
from logging import Logger
from typing import Any
from core.const import APPNAME

# Control logging of module entry and exit
_LOG_MODULE_ENTRY = False
_LOG_MODULE_EXIT = False


root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
app_logger = logging.getLogger(APPNAME)
app_logger.setLevel(logging.DEBUG)

root_logger.debug("root logger initialized.")
app_logger.debug("app logger initialized.")

# 🏴‍☠️ ANSI runes for color
RESET = "\033[0m"
BOLD = "\033[1m"

# Foreground colors
FG_RED = "\033[31m"
FG_YELLOW = "\033[33m"
FG_CYAN = "\033[36m"
FG_WHITE = "\033[97m"

# Backgrounds
BG_RED = "\033[41m"


class ColorFormatter(logging.Formatter):
    """A formatter so vivid it burns the Kraken’s eyes!"""

    COLORS = {
        logging.DEBUG: FG_CYAN,
        logging.INFO: FG_WHITE,
        logging.WARNING: FG_YELLOW,
        logging.ERROR: BG_RED + FG_WHITE + BOLD,
        logging.CRITICAL: BG_RED + FG_WHITE + BOLD,
    }

    def __init__(self):
        base_format = (
            "%(asctime)s  %(name)-55s:%(lineno)4d - %(levelname)-5s - %(message)s"
        )
        super().__init__(base_format)
        self.base_formatter = logging.Formatter(base_format)

    def format(self, record):
        color = self.COLORS.get(record.levelno, "")
        record.levelname = f"{color}{record.levelname:<5s}{RESET}"
        return self.base_formatter.format(record)


root_handler = logging.StreamHandler()
root_handler.setFormatter(ColorFormatter())
root_logger.addHandler(root_handler)

_REDACT_PATTERN = re.compile(r"(pass|secret|token|key)", re.IGNORECASE)


def redact(value: Any) -> Any:
    "Return a log-safe copy with sensitive values redacted."
    if isinstance(value, dict):
        return {
            key: (
                "***redacted***" if _REDACT_PATTERN.search(str(key)) else redact(item)
            )
            for key, item in value.items()
        }
    return value


def getLogger(  # pylint: disable=invalid-name
    name: str, level=logging.NOTSET  # pylint: disable=unused-argument
) -> Logger:
    "Create module specific logger and log potentially module code entry (when module is imported)"
    logger = logging.getLogger(
        APPNAME if name == "__main__" else (APPNAME + "." + name)
    )

    if _LOG_MODULE_ENTRY:
        logger.debug("Enter module")
    return logger


def log_exit(logger):
    "Log end of execution of the module code"
    if _LOG_MODULE_EXIT:
        logger.debug("Exit module")
