""" Administrative messages.
    (for login related messages see login.py)
"""

from enum import StrEnum
import logging
from messages.message import Message, MessageType, MessageAttribute
from core.app_logging import getLogger

LOG = getLogger(__name__)


class Echo(Message):
    "Echo request from Frontend. Reply by sending the requested message back."

    @classmethod
    def message_type(cls):
        return MessageType.WS_TYPE_ECHO

    async def handle_message(self, connection):
        "Return the payload to the requsted component"
        await connection.send_message_to_component(
            self.message.get(MessageAttribute.WS_ATTR_COMPONENT),
            self.message.get(MessageAttribute.WS_ATTR_PAYLOAD),
        )
