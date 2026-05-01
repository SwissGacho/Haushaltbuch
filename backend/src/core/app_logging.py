"""Application specific logging"""

import datetime
from enum import StrEnum
import re
import json

import logging
import logging.config
from logging import Logger
from typing import Any
from core.const import APPNAME, CONFIG_DBCFG_FILE
from core.configuration.cmd_line import CommandLine
from core.util_base import get_config_item


# Control logging of module entry and exit
_LOG_MODULE_ENTRY = False
_LOG_MODULE_EXIT = _LOG_MODULE_ENTRY


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

    def formatTime(self, record, datefmt=None):
        ct = datetime.datetime.fromtimestamp(record.created)
        return ct.strftime("%H:%M:%S.%f")

    def format(self, record):
        color = self.COLORS.get(record.levelno, "")
        record.levelname = f"{color}{record.levelname:<5s}{RESET}"
        return super().format(record)


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


logging_dict = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "color": {
            "()": ColorFormatter,
            "format": "%(asctime)s  %(name)-65s:%(lineno)4d - %(levelname)-5s - %(message)s",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "color",
        }
    },
    "root": {
        "level": "WARNING",
        "handlers": ["console"],
    },
    "loggers": {
        APPNAME: {
            "level": "INFO",
            "propagate": True,
        }
    },
}


class LogConfig(StrEnum):
    CONFIG_LOGGING = "logging"
    CONFIG_LOG_DEFAULT = "default"
    CONFIG_LOG_LEVEL = "level"


def app_loggers() -> set[logging.Logger]:
    "Get all loggers in the app"
    return {
        logger
        for name, logger in logging.Logger.manager.loggerDict.items()
        if name == APPNAME or name.startswith(APPNAME + ".")
        if isinstance(logger, logging.Logger)
    }


def all_loggers(parent=logging.getLogger()) -> set[logging.Logger]:
    "Get all loggers"
    loggers = {parent} | {
        l
        for l in logging.Logger.manager.loggerDict.values()
        if isinstance(l, logging.Logger)
    }
    return loggers


def app_logger_names() -> list[str]:
    "Get names of all loggers in the app"
    return sorted({l.name for l in app_loggers()})


def _get_log_config_keyes(cfg):
    def _get_keys(d):
        if not d:
            return []
        return [
            k + "/" + subkey
            for k, v in d.items()
            if isinstance(v, dict)
            for subkey in _get_keys(v)
        ] + [k for k, v in d.items() if isinstance(v, str)]

    def _flatten(lst):
        return [
            item
            for element in lst
            for item in (_flatten(element) if isinstance(element, list) else [element])
        ]

    return [
        k
        for k in _flatten(_get_keys(get_config_item(cfg, LogConfig.CONFIG_LOGGING)))
        if k not in (LogConfig.CONFIG_LOG_DEFAULT)
    ]


def _get_log_config_level(cfg, key: str) -> str:
    "Get log level from config"
    val = get_config_item(cfg, key)
    if not val:
        return ""
    if not isinstance(val, str):
        # LOG.warning(f"Log level for '{key}' is not a string: {val}")
        return ""
    return val.upper()


def disabled(rec):
    "Filter function to disable logging from a logger"
    return False


def configure_logging(log_cfg: dict = None):

    global_default_level = _get_log_config_level(
        log_cfg, LogConfig.CONFIG_LOGGING + "/" + LogConfig.CONFIG_LOG_DEFAULT
    )

    if global_default_level:
        logging_dict["root"]["level"] = global_default_level
    for logger_item in _get_log_config_keyes(log_cfg):
        if logger_item.split("/")[-1] == LogConfig.CONFIG_LOG_LEVEL:
            logger = "/".join(logger_item.split("/")[:-1])
        else:
            logger = logger_item
        level = _get_log_config_level(
            log_cfg, LogConfig.CONFIG_LOGGING + "/" + logger_item
        )
        if level.upper() in ["DISABLED", "DISABLE", "OFF"]:
            logging_dict["loggers"][logger.replace("/", ".")] = {"filters": [disabled]}
        else:
            logging_dict["loggers"][logger.replace("/", ".")] = {
                "level": level,
                "propagate": True,
            }

    try:
        logging.config.dictConfig(logging_dict)
    except (ValueError, TypeError, AttributeError, ImportError) as e:
        print(f"ERROR configuring logging: {e}")

    log = logging.getLogger(APPNAME + "." + __name__)
    if log.isEnabledFor(logging.DEBUG):
        log.debug(
            f"Logging configured with config:\n{json.dumps((log_cfg or {}).get(LogConfig.CONFIG_LOGGING, {}), indent=4)}"
        )
        log.debug("Logging is now reconfigured:")
        for l in sorted(
            [
                f"   {l.name:<65}: {logging.getLevelName(l.level):>8} {', '.join(f.__name__ for f in l.filters)}"
                for l in [
                    l
                    for l in (
                        all_loggers()
                        | {
                            logging.getLogger(),
                        }
                    )
                    if l.level > logging.NOTSET or l.filters
                ]
            ]
        ):
            log.debug(l)
    log.debug(
        f'Main loggers: {", ".join([l.name for l in logging.getLogger().getChildren()])}'
    )


# parse commandline for logging config overrides
parsed_commandline = CommandLine.parse_commandline(dbcfg_file_key=CONFIG_DBCFG_FILE)
configure_logging(parsed_commandline)

log_exit(logging.getLogger(APPNAME + "." + __name__))
