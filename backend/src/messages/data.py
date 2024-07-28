""" Messages to request and send business objects
"""

from typing import Optional
from enum import StrEnum

from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from core.app import App
from core.configuration.config import Config
from core.app_logging import getLogger

LOG = getLogger(__name__)


class DataObjectTypes(StrEnum):
    DO_TYPE_SETUP_CONFIG = "setup_config"


class FetchMessage(Message):
    "Message requesting a business object"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_FETCH

    async def handle_message(self, connection):
        "handle a fetch message"


class ObjectMessage(Message):
    "Message containing a single requested business object"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_OBJECT

    def __init__(
        self,
        object_type: DataObjectTypes,
        index: Optional[int | str],
        payload: any,
        token: Optional[WSToken] = None,
        status: str = None,
    ) -> None:
        self.message = {}
        super().__init__(
            msg_type=self.__class__.message_type(), token=token, status=status
        )
        self.message |= {
            MessageAttribute.WS_ATTR_OBJECT: object_type,
            MessageAttribute.WS_ATTR_INDEX: index,
            MessageAttribute.WS_ATTR_PAYLOAD: payload,
        }


class StoreMessage(Message):
    "Business object to be stored in the DB"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_STORE

    async def handle_message(self, connection):
        "Handle a StoreMessage"
        LOG.debug(f"StoreMessage.handle_message {self.message=}")
