""" Administrative messages.
    (for login related messages see login.py)
"""

from enum import StrEnum
import logging
from messages.message import Message, MessageType, MessageAttribute
from core.app_logging import getLogger

LOG = getLogger(__name__)


class LogLevel(StrEnum):
    LOG_LEVEL_DEBUB = "debug"
    LOG_LEVEL_INFO = "info"
    LOG_LEVEL_WARNING = "warning"
    LOG_LEVEL_ERROR = "error"
    LOG_LEVEL_CRITICAL = "critical"


LOGGING_LEVEL = {
    LogLevel.LOG_LEVEL_DEBUB: logging.DEBUG,
    LogLevel.LOG_LEVEL_INFO: logging.INFO,
    LogLevel.LOG_LEVEL_WARNING: logging.WARNING,
    LogLevel.LOG_LEVEL_ERROR: logging.ERROR,
    LogLevel.LOG_LEVEL_CRITICAL: logging.CRITICAL,
}


class LogMessage(Message):
    "receive a log message and send to logger"

    @classmethod
    def message_type(cls):
        return MessageType.WS_TYPE_LOG

    async def handle_message(self, connection):
        "handle a log message"
        level = self.message.get(MessageAttribute.WS_ATTR_LOGLEVEL)
        text = self.message.get(MessageAttribute.WS_ATTR_MESSAGE)
        caller = self.message.get(MessageAttribute.WS_ATTR_CALLER)
        getLogger(caller or "FrontEnd").log(LOGGING_LEVEL.get(level), text)
