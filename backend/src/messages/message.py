"""Websocket messages exchanged between backend and frontend"""

import pathlib
from enum import StrEnum
from json import dumps, loads
from typing import Any, Optional
from core.base_objects import BaseObject
from server.ws_token import WSToken
from core.app_logging import getLogger
import messages

LOG = getLogger(__name__)


class MessageType(StrEnum):
    WS_TYPE_NONE = "None"
    WS_TYPE_HELLO = "Hello"
    WS_TYPE_LOGIN = "Login"
    WS_TYPE_WELCOME = "Welcome"
    WS_TYPE_BYE = "Bye"
    WS_TYPE_LOG = "Log"
    WS_TYPE_ECHO = "Echo"
    WS_TYPE_FETCH = "Fetch"
    WS_TYPE_OBJECT = "Object"
    WS_TYPE_STORE = "Store"
    WS_TYPE_FETCH_SETUP = "FetchSetup"
    WS_TYPE_OBJECT_SETUP = "ObjectSetup"
    WS_TYPE_STORE_SETUP = "StoreSetup"
    WS_TYPE_FETCH_NAVIGATION_HEADERS = "FetchNavigationHeaders"
    WS_TYPE_FETCH_LIST = "FetchList"
    WS_TYPE_NAVIGATION_HEADERS = "NavigationHeaders"
    WS_TYPE_OBJECT_LIST = "ObjectList"


class MessageAttribute(StrEnum):
    "Key used in message paylod"

    WS_ATTR_TYPE = "type"
    WS_ATTR_TOKEN = "token"
    WS_ATTR_STATUS = "status"
    WS_ATTR_PAYLOAD = "payload"

    # Fetch
    WS_ATTR_OBJECT = "object"
    WS_ATTR_INDEX = "index"

    # Hello
    WS_ATTR_SEARCH_PATH = "search_path"

    # Login
    WS_ATTR_USER = "user"
    WS_ATTR_SES_TOKEN = "ses_token"
    WS_ATTR_PREV_TOKEN = "prev_token"

    # Bye
    WS_ATTR_REASON = "reason"

    # Log
    WS_ATTR_LOGLEVEL = "log_level"
    WS_ATTR_MESSAGE = "message"
    WS_ATTR_CALLER = "caller"

    # Echo
    WS_ATTR_COMPONENT = "component"

    # Navigation Headers
    WS_ATTR_NAVIGATION_HEADERS = "navigation_headers"


def json_encode(obj: Any) -> Any:
    "jsonize objects"
    return (
        str(obj)
        if isinstance(obj, BaseObject) or isinstance(obj, pathlib.Path)
        else obj
    )


def _serialize(msg_dict: dict) -> dict:
    "serialize a message dictionary replacing keys by str(key)"
    return {
        str(k): _serialize(v) if isinstance(v, dict) else v for k, v in msg_dict.items()
    }


def _all_subclasses(cls):
    "return subclasses recursively"
    return set(cls.__subclasses__()).union(
        [s for c in cls.__subclasses__() for s in _all_subclasses(c)]
    )


class Message(BaseObject):
    "Commons of messages"

    def __new__(cls, json_message: str = None, **kwa):
        # LOG.debug(f"Message.__new__({cls=} {json_message=} {kwa=})")
        if json_message and isinstance(json_message, str):
            message_type = loads(json_message).get(MessageAttribute.WS_ATTR_TYPE)
            if message_type:
                for sub in _all_subclasses(cls=cls):
                    if sub.message_type() == message_type:
                        return super().__new__(sub)
        return super().__new__(cls)

    def __init__(
        self,
        json_message: str | None = None,
        msg_type: MessageType | None = MessageType.WS_TYPE_NONE,
        token: WSToken | None = None,
        status: str | None = None,
        **kwargs,
    ) -> None:
        if json_message and isinstance(json_message, str):
            self.message = loads(json_message)
            if not self.message.get(MessageAttribute.WS_ATTR_TYPE):
                self.message[MessageAttribute.WS_ATTR_TYPE] = MessageType.WS_TYPE_NONE
        else:
            if (msg_type is None) or (msg_type == MessageType.WS_TYPE_NONE):
                msg_type = self.__class__.message_type()
            self.message = {
                MessageAttribute.WS_ATTR_TYPE: msg_type,
                MessageAttribute.WS_ATTR_TOKEN: token,
            }
            if status:
                self.message |= {MessageAttribute.WS_ATTR_STATUS: status}
        self.add(kwargs)

    @classmethod
    def message_type(cls) -> MessageType:
        "type of the message"
        return MessageType.WS_TYPE_NONE

    @property
    def token(self):
        "connection token of the message"
        return self.message.get(MessageAttribute.WS_ATTR_TOKEN)

    def get_str(self, attr: MessageAttribute) -> Optional[str]:
        val = self.message.get(attr, "")
        return val if isinstance(val, str) else None

    def get_int(self, attr: MessageAttribute) -> Optional[int]:
        val = self.message.get(attr, "")
        return val if isinstance(val, int) else None

    def get_dict(self, attr: MessageAttribute) -> Optional[dict]:
        val = self.message.get(attr, "")
        return val if isinstance(val, dict) else None

    def add(self, attrs: dict):
        "Add items to the message root that will be serialized and sent via websocket"
        self.message |= attrs

    def serialize(self):
        "Serialize to JSON"
        # LOG.debug(f"Message.serialize: message={self.message}")
        return dumps(_serialize(self.message), default=json_encode)

    async def handle_message(self, connection):
        "Handle unknown message type"
        LOG.error(f"received unknown message ({self.message})")


# LOG.debug("module imported")
