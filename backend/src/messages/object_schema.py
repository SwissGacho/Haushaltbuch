"""Messages to request and send business objects"""

from datetime import date
import datetime
from logging import Logger
from enum import Flag

from business_objects.bo_descriptors import (
    BOBaseBase,
)
from business_objects.business_object_base import BOBase
from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from core.app_logging import getLogger
from typing import Any, Callable


LOG: Logger = getLogger(__name__)
ATTRIBUTETYPES: dict[type, Callable[[str], str]] = {
    int: lambda x: "int",
    str: lambda x: "str",
    date: lambda x: "date",
    datetime: lambda x: "datetime",
    dict: lambda x: "dict",
    Flag: lambda x: "flag",
    BOBaseBase: lambda x: ".".join(["relation", str(x.bo_type_name())]),
}


class ObjectSchema(Message):
    "Message containing the schema of a business object"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_OBJECT

    def generate_payload(self) -> dict[str, str]:
        properties = self._object_type.attributes_as_dict()
        print(f"{properties=}")
        for k, v in properties.items():
            print(f"{k=}")
            print(f"{v=}")
            if hasattr(v, "bo_type_name"):
                print(f"{v.bo_type_name()=}")
        payload = {
            k: ATTRIBUTETYPES[v](v)
            for k, v in properties.items()
            if v in ATTRIBUTETYPES and k != "last_updated"
        }
        return payload

    def __init__(
        self,
        object_type: type[BOBase],
        token: WSToken | None = None,
        status: str | None = None,
    ) -> None:
        self._object_type = object_type
        payload = self.generate_payload()
        self.message = {}
        super().__init__(token=token, status=status)
        self.add(
            {
                MessageAttribute.WS_ATTR_OBJECT: object_type.bo_type_name(),
                MessageAttribute.WS_ATTR_PAYLOAD: payload,
            }
        )
