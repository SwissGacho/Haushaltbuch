"""Application specific logging"""

import datetime
from enum import StrEnum
import re
import json
import pprint
import copy

import logging
import logging.config
from logging import Logger, DEBUG, INFO, WARNING, ERROR, CRITICAL
from typing import Any, MutableMapping
from core.const import APPNAME, CONFIG_FILECFG_FILE
from core.configuration.cmd_line import CommandLine
from core.util_base import get_config_item

# Control logging of module entry and exit
_LOG_MODULE_ENTRY = False
_LOG_MODULE_EXIT = _LOG_MODULE_ENTRY

# Verbose debug logging level
VERBOSE_DEBUG = 5
logging.addLevelName(VERBOSE_DEBUG, "VERBOSE_DEBUG")


# 🏴‍☠️ ANSI runes for color
RESET = "\033[0m"
BOLD = "\033[1m"

# Foreground colors
FG_RED = "\033[31m"
FG_GREEN = "\033[32m"
FG_YELLOW = "\033[33m"
FG_BLUE = "\033[34m"
FG_MAGENTA = "\033[35m"
FG_CYAN = "\033[36m"
FG_WHITE = "\033[97m"

# Backgrounds
BG_RED = "\033[41m"


_PREFIX_PATTERN = re.compile(r"^(\[[^\]]+\])(\s*)")


class ColorFormatter(logging.Formatter):
    """A formatter so vivid it burns the Kraken’s eyes!"""

    COLORS = {
        VERBOSE_DEBUG: FG_MAGENTA,
        DEBUG: FG_CYAN,
        INFO: FG_WHITE,
        WARNING: FG_YELLOW,
        ERROR: BG_RED + FG_WHITE + BOLD,
        CRITICAL: BG_RED + FG_WHITE + BOLD,
    }

    def formatTime(self, record, datefmt=None):
        ct = datetime.datetime.fromtimestamp(record.created)
        return ct.strftime("%H:%M:%S.%f")

    @staticmethod
    def _highlight_prefix(message: str) -> str:
        return _PREFIX_PATTERN.sub(rf"{FG_GREEN}\1{RESET}\2", message, count=1)

    def format(self, record):
        original_msg = record.msg
        original_args = record.args
        original_levelname = record.levelname
        color = self.COLORS.get(record.levelno, "")
        try:
            record.levelname = f"{color}{record.levelname:<5s}{RESET}"
            record.msg = self._highlight_prefix(record.getMessage())
            record.args = ()
            return super().format(record)
        finally:
            record.msg = original_msg
            record.args = original_args
            record.levelname = original_levelname


_REDACT_PATTERN = re.compile(r"(pass|secret|token|key)", re.IGNORECASE)


def redact(value: Any) -> Any:
    "Return a log-safe copy with sensitive values redacted."
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, dict):
        return {
            key: (
                "***redacted***" if _REDACT_PATTERN.search(str(key)) else redact(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, str) and value.startswith("{"):
        try:
            return json.dumps(redact(json.loads(value)))
        except json.JSONDecodeError:
            pass
    return value


def pprint_lines(value: Any) -> list[str]:
    "Return a list of lines for pretty-printing a value in logs."
    return pprint.pformat(redact(value), indent=4, width=120, compact=True).splitlines()


def redact_truncate(value: Any, max_length: int = 80) -> str:
    "Return a string representation of the value with its prefix truncated to max_length characters (plus a length suffix when truncated)."
    s = str(redact(value))
    if len(s) > max_length:
        return f"{s[:max_length]}... (total {len(s)} chars)"
    return s


entered_modules = [__name__]


def log_entry(logger, module_name: str):
    "Log entry into execution of the module code"
    if _LOG_MODULE_ENTRY:
        if module_name != "__main__":
            entered_modules.append(module_name)
        logger.debug(
            f"Enter module {module_name.split('.')[-1]:>35}, "
            f"entered modules: {', '.join([m.split('.')[-1] for m in entered_modules])}"
        )


def log_exit(logger):
    "Log end of execution of the module code"
    if _LOG_MODULE_EXIT:
        module_name = logger.name
        prefix = f"{APPNAME}."
        if module_name.startswith(prefix):
            module_name = module_name[len(prefix) :]
        elif module_name == APPNAME:
            module_name = "__main__"
        if module_name in entered_modules:
            entered_modules.remove(module_name)
        logger.debug(
            f"Exit module  {module_name.split('.')[-1]:>35}, "
            f"entered modules: {', '.join([m.split('.')[-1] for m in entered_modules])}"
        )


def getLogger(  # pylint: disable=invalid-name
    name: str, level=logging.NOTSET  # pylint: disable=unused-argument
) -> Logger:
    "Create module specific logger and log potentially module code entry (when module is imported)"
    logger = logging.getLogger(
        APPNAME if name == "__main__" else (APPNAME + "." + name)
    )

    if _LOG_MODULE_ENTRY:
        log_entry(logger, name)
    return logger


class ContextLogger(logging.LoggerAdapter):
    """Keep logger names stable while attaching runtime context to messages."""

    def __init__(self, logger: Logger, extra: dict[str, Any] | None = None) -> None:
        super().__init__(logger, extra or {})

    def bind(self, **extra: Any) -> "ContextLogger":
        "Return a new adapter with merged context."
        return ContextLogger(self.logger, dict(self.extra or {}) | extra)

    def process(self, msg: str, kwargs: MutableMapping[str, Any]):
        context = {
            key: value
            for key, value in dict(self.extra or {}).items()
            if value is not None and value != ""
        }
        if context:
            prefix = ";".join(f"{key}={value}" for key, value in context.items())
            msg = f"[{prefix}] {msg}"
        return msg, kwargs


def get_context_logger(logger: Logger | ContextLogger, **extra: Any) -> ContextLogger:
    "Return a logger adapter carrying runtime context without creating new logger names."
    if isinstance(logger, ContextLogger):
        return logger.bind(**extra)
    return ContextLogger(logger, extra)


default_logging_dict = {
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
        if k != LogConfig.CONFIG_LOG_DEFAULT
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


def configure_logging(log_cfg: dict | None = None):
    """Configure logging from config dict"""
    log = logging.getLogger(APPNAME + "." + __name__)
    global_default_level = _get_log_config_level(
        log_cfg, LogConfig.CONFIG_LOGGING + "/" + LogConfig.CONFIG_LOG_DEFAULT
    )
    logging_dict = copy.deepcopy(default_logging_dict)
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
        elif level.upper() not in [
            "VERBOSE_DEBUG",
            "DEBUG",
            "INFO",
            "WARNING",
            "ERROR",
            "CRITICAL",
            "NOTSET",
        ]:
            log.error(
                f"ERROR: Invalid log level '{level}' for logger '{logger}', ignoring"
            )
        else:
            logging_dict["loggers"][logger.replace("/", ".")] = {
                "level": level,
                "propagate": True,
            }

    try:
        logging.config.dictConfig(logging_dict)
    except (ValueError, TypeError, AttributeError, ImportError) as e:
        log.error(f"ERROR configuring logging: {e}")
    if log.isEnabledFor(VERBOSE_DEBUG):
        log.log(VERBOSE_DEBUG, f"Logging dictConfig:")
        for line in pprint.pformat(
            logging_dict, indent=4, width=120, compact=True
        ).splitlines():
            log.log(VERBOSE_DEBUG, f" - {line}")
    if log.isEnabledFor(DEBUG):
        if log.isEnabledFor(VERBOSE_DEBUG):
            log.log(VERBOSE_DEBUG, f"Logging configured with config:")
            for line in pprint.pformat(
                (log_cfg or {}).get(LogConfig.CONFIG_LOGGING, {}),
                indent=4,
                width=120,
                compact=True,
            ).splitlines():
                log.log(VERBOSE_DEBUG, f" - {line}")
        log.debug("Logging is now (re)configured:")
        for l in sorted(
            [
                f"   {l.name:<65}: {logging.getLevelName(l.level):>8} "
                + f"{', '.join(getattr(f, '__name__', type(f).__name__) for f in l.filters)}"
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
parsed_commandline = CommandLine.parse_commandline(filecfg_file_key=CONFIG_FILECFG_FILE)
configure_logging(parsed_commandline)

log_exit(logging.getLogger(APPNAME + "." + __name__))
