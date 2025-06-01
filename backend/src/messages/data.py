"""Messages to request and send business objects"""

from logging import root
from types import NoneType
from typing import TypeAlias, Optional, Union
from enum import StrEnum
import pathlib
from wsgiref import headers

from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from core.app import App
from core.configuration.config import Config
from core.base_objects import BaseObject
from core.app_logging import getLogger
from persistance.business_object_base import BOBase

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


JSONAble: TypeAlias = Union[
    str,
    int,
    bool,
    NoneType,
    dict[str, "JSONAble"],
    list["JSONAble"],
    BaseObject,
    pathlib.Path,
    list[str],
]


class ObjectMessage(Message):
    "Message containing a single requested business object"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_OBJECT

    def __init__(
        self,
        object_type: DataObjectTypes,
        index: Optional[int | str],
        payload: JSONAble,
        token: Optional[WSToken] | None = None,
        status: str | None = None,
    ) -> None:
        self.message = {}
        super().__init__(token=token, status=status)
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


class FetchNavigationHeaders(Message):
    "Message requesting the list headers applicable to an object. If no object is provided, return a list of headers applicable to the root tree of the user."

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_FETCH_NAVIGATION_HEADERS

    async def handle_message(self, connection):
        "Handle a FetchListMessage"
        business_objects: dict[str, type[BOBase]] = BOBase.all_business_objects
        root_objectNames: list[str] = []
        # For now we iterate through the types of business objects and find whether any of them have the same name as the "objectType" attribute
        for name in business_objects:
            root_objectNames.append(name)

        msg = NavigationHeaders(token=self.message.get(MessageAttribute.WS_ATTR_TOKEN))
        msg.message |= {MessageAttribute.WS_ATTR_PAYLOAD: {"headers": root_objectNames}}
        await connection.send_message(msg)


class NavigationHeaders(Message):

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_NAVIGATION_HEADERS


class FetchListMessage(Message):
    "Message from the frontend requesting a list of business objects. The list is returned as a list of IDs."

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_FETCH_LIST

    async def handle_message(self, connection):
        # For now we just handle the special case of the root tree being requested
        # First create a list of all business objects; these form the headers of the root tree. For every business object, find out whether it's a "root tree" object or not
        business_objects: dict[str, type["BOBase"]] = BOBase.all_business_objects
        object_type = self.message.get(MessageAttribute.WS_ATTR_OBJECT)
        name_list: list[str] = []
        for name in business_objects:
            # right now we just assume every BO is a root tree object
            if name == object_type:
                my_type = business_objects[name]
                assert issubclass(my_type, BOBase)
                ids = await my_type.get_matching_ids()
                for id in ids:
                    # in future we'll also get the name of the object
                    name_list.append(str(id))

        # log the ids
        LOG.debug(f"FetchListMessage.handle_message {name_list=}")
        msg = ObjectList(token=self.message.get(MessageAttribute.WS_ATTR_TOKEN))
        msg.message |= {MessageAttribute.WS_ATTR_PAYLOAD: {"objects": name_list}}
        LOG.debug(f"FetchListMessage.handle_message {msg=}")
        await connection.send_message(msg)


class ObjectList(Message):
    "Message containing a list of business objects"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_OBJECT_LIST
