""" Websocket messages exchanged between backend and frontend
"""

import pathlib
from enum import StrEnum
from json import dumps, loads
from typing import Any
from core.base_object import BaseObject
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


class MessageAttribute(StrEnum):
    "Key used in message paylod"
    WS_ATTR_TYPE = "type"
    WS_ATTR_TOKEN = "token"
    WS_ATTR_STATUS = "status"

    # Fetch
    WS_ATTR_OBJECT = "object"
    WS_ATTR_INDDEX = "index"

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
    WS_ATTR_PAYLOAD = "payload"
    WS_ATTR_COMPONENT = "component"


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
        json_message: str = None,
        msg_type: MessageType = MessageType.WS_TYPE_NONE,
        token: WSToken = None,
        status: str = None,
    ) -> None:
        if json_message and isinstance(json_message, str):
            self.message = loads(json_message)
            if not self.message.get(MessageAttribute.WS_ATTR_TYPE):
                self.message[MessageAttribute.WS_ATTR_TYPE] = MessageType.WS_TYPE_NONE
        else:
            self.message = {
                MessageAttribute.WS_ATTR_TYPE: msg_type,
                MessageAttribute.WS_ATTR_TOKEN: token,
            }
            if status:
                self.message |= {MessageAttribute.WS_ATTR_STATUS: status}

    @classmethod
    def message_type(cls):
        "type of the message"
        return MessageType.WS_TYPE_NONE

    @property
    def token(self):
        "connection token of the message"
        return self.message.get(MessageAttribute.WS_ATTR_TOKEN)

    def add(self, attrs: dict):
        "Add items to the payload"
        self.message |= attrs

    def serialize(self):
        "Serialize to JSON"
        # LOG.debug(f"Message.serialize: message={self.message}")
        return dumps(_serialize(self.message), default=json_encode)

    async def handle_message(self, connection):
        "Handle unknown message type"
        LOG.error(f"received unknown message ({self.message})")


# LOG.debug("module imported")
