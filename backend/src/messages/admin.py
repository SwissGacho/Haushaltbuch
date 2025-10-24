"""Administrative messages.
(for login related messages see login.py)
"""

from enum import StrEnum
import logging

from core.app_logging import getLogger, log_exit, Logger

LOG: Logger = getLogger(__name__)
from messages.message import Message, MessageType, MessageAttribute
from server.ws_connection_base import WSConnectionBase


class LogLevel(StrEnum):
    "Logging levels from Frontend"

    LOG_LEVEL_DEBUG = "debug"
    LOG_LEVEL_INFO = "info"
    LOG_LEVEL_WARNING = "warning"
    LOG_LEVEL_ERROR = "error"
    LOG_LEVEL_CRITICAL = "critical"


LOGGING_LEVEL = {
    LogLevel.LOG_LEVEL_DEBUG: logging.DEBUG,
    LogLevel.LOG_LEVEL_INFO: logging.INFO,
    LogLevel.LOG_LEVEL_WARNING: logging.WARNING,
    LogLevel.LOG_LEVEL_ERROR: logging.ERROR,
    LogLevel.LOG_LEVEL_CRITICAL: logging.CRITICAL,
    0: logging.NOTSET,
}


class LogMessage(Message):
    "receive a log message and send to logger"

    @classmethod
    def message_type(cls):
        return MessageType.WS_TYPE_LOG

    async def handle_message(self, connection):
        "handle a log message"
        # LOG.debug(f"LogMessage.handle_message({connection=})")
        level: str = self.message.get(
            MessageAttribute.WS_ATTR_LOGLEVEL, LogLevel.LOG_LEVEL_WARNING
        )
        text = self.message.get(MessageAttribute.WS_ATTR_MESSAGE, "<no message>")
        caller = "FrontEnd." + str(
            self.message.get(MessageAttribute.WS_ATTR_CALLER, "")
        )
        # LOG.debug(f"Logging message from {caller=} at level {level}: {text}")
        getLogger(caller).log(LOGGING_LEVEL.get(level, 0), text)


class EchoMessage(Message):
    "Echo request from Frontend. Reply by sending the requested message back."

    @classmethod
    def message_type(cls):
        return MessageType.WS_TYPE_ECHO

    async def handle_message(self, connection: WSConnectionBase):
        "Return the payload to the requsted component"
        await connection.send_message_to_component(
            self.message.get(MessageAttribute.WS_ATTR_COMPONENT),
            self.message.get(MessageAttribute.WS_ATTR_PAYLOAD),
        )


log_exit(LOG)
