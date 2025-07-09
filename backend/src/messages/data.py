"""Messages to request and send business objects"""

from logging import Logger
from types import NoneType
from typing import TypeAlias, Union
from enum import StrEnum
import pathlib

from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from core.base_objects import BaseObject
from core.app_logging import getLogger

LOG: Logger = getLogger(__name__)


JSONAble: TypeAlias = Union[
    str,
    int,
    bool,
    NoneType,
    dict[str, "JSONAble"],
    list["JSONAble"],
    BaseObject,
    pathlib.Path,
]


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
        index: int | str | None,
        payload: JSONAble,
        token: WSToken | None = None,
        status: str | None = None,
    ) -> None:
        self.message = {}
        super().__init__(token=token, status=status)
        self.add(
            {
                MessageAttribute.WS_ATTR_OBJECT: object_type,
                MessageAttribute.WS_ATTR_INDEX: index,
                MessageAttribute.WS_ATTR_PAYLOAD: payload,
            }
        )
