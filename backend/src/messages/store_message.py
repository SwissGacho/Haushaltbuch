"""This message is sent by the frontend when a business object should be persisted in the database."""

from logging import Logger

from core.app_logging import getLogger
from messages.message import Message, MessageType

LOG: Logger = getLogger(__name__)


class StoreMessage(Message):
    "Business object to be stored in the DB"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_STORE

    async def handle_message(self, connection):
        "Handle a StoreMessage"
        LOG.debug(f"StoreMessage.handle_message {self.message=}")
