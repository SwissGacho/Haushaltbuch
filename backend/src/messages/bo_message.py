"""Messages to request and send business objects"""

from logging import Logger
from types import NoneType
from typing import TypeAlias, Union
from enum import StrEnum
import pathlib

from business_objects.bo_list import BOSubscription
from business_objects.business_object_base import BOBase
from server.ws_connection_base import WSConnectionBase
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

    async def handle_message(self, connection: WSConnectionBase):
        "handle a fetch message"
        object_type_name = self.message.get(MessageAttribute.WS_ATTR_OBJECT)

        BOSubscription(
            bo_type=object_type_name,
            connection=connection,
            id=self.message.get(MessageAttribute.WS_ATTR_INDEX),
            notify_subscribers_on_init=True,
        )


def callback(subscribed_object: BOBase):
    msg = ObjectMessage()
    msg.add(
        {
            MessageAttribute.WS_ATTR_OBJECT: subscribed_object.bo_type_name(),
            MessageAttribute.WS_ATTR_INDEX: subscribed_object.id,
            MessageAttribute.WS_ATTR_PAYLOAD: subscribed_object.__class__.attributes_as_dict(),
        }
    )


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
