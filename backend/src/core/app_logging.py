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
            "%(asctime)s  %(name)-60s:%(lineno)4d " "- %(levelname)-5s - %(message)s"
        )
        super().__init__(base_format)
        self.base_formatter = logging.Formatter(base_format)

    def format(self, record):
        color = self.COLORS.get(record.levelno, "")
        record.levelname = f"{color}{record.levelname}{RESET}"
        return self.base_formatter.format(record)


root_handler = logging.StreamHandler()
root_handler.setFormatter(ColorFormatter())
root_logger.addHandler(root_handler)


def getLogger(name: str, level=logging.NOTSET) -> logging.Logger:
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
