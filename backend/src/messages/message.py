""" Websocket messages exchanged between backend and frontend
"""

from enum import Enum
from json import dumps, loads
from typing import Any
from core.base_object import BaseObject
from server.ws_token import WSToken
from core.app_logging import getLogger

LOG = getLogger(__name__)


class MessageType(Enum):
    WS_TYPE_NONE = None
    WS_TYPE_HELLO = "Hello"
    WS_TYPE_LOGIN = "Login"
    WS_TYPE_WELCOME = "Welcome"


class MessageAttribute(Enum):
    WS_ATTR_TYPE = "type"
    WS_ATTR_TOKEN = "token"
    WS_ATTR_STATUS = "status"
    WS_ATTR_USER = "user"
    WS_ATTR_SES_TOKEN = "ses_token"
    WS_ATTR_PREV_TOKEN = "prev_token"


def json_encode(obj: Any) -> Any:
    "jsonize objects"
    return (
        str(obj)
        if isinstance(obj, BaseObject)
        else obj.value
        if isinstance(obj, Enum)
        else obj
    )


def serialize(msg_dict: dict) -> dict:
    "serialize a message dictionary replacing keys by str(key)"
    return {
        k.value
        if isinstance(k, Enum)
        else str(k): serialize(v)
        if isinstance(v, dict)
        else v
        for k, v in msg_dict.items()
    }


class Message(BaseObject):
    "Commons of messages"

    def __new__(cls, json_message: str = None, **kwa):
        # LOG.debug(f"Message.__new__({cls=} {json_message=} {kwa=})")
        if json_message and isinstance(json_message, str):
            message_type = loads(json_message).get(MessageAttribute.WS_ATTR_TYPE.value)
            for sub in cls.__subclasses__():
                if sub.message_type() == message_type:
                    return super().__new__(sub)
        return super().__new__(cls)

    def __init__(
        self,
        json_message: str = None,
        msg_type: MessageType = None,
        token: WSToken = None,
        status: str = None,
    ) -> None:
        if json_message and isinstance(json_message, str):
            self.message = loads(json_message)
        else:
            self.message = {
                MessageAttribute.WS_ATTR_TYPE: msg_type,
                MessageAttribute.WS_ATTR_TOKEN: token,
            }
            if status:
                self.message |= {MessageAttribute.WS_ATTR_STATUS: status}

    @classmethod
    def message_type(cls):
        return None

    @property
    def token(self):
        "connection token of the message"
        return self.message.get(MessageAttribute.WS_ATTR_TOKEN.value)

    def add(self, attrs: dict):
        self.message |= attrs

    def serialize(self):
        return dumps(serialize(self.message), default=json_encode)

    async def handle_message(self, connection):
        "Handle unknown message type"
        LOG.error(f"received unknown message ({self.message})")


# LOG.debug("module imported")
