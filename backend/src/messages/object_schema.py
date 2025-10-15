"""Messages to request and send business objects"""

from datetime import date, datetime
from logging import Logger
from enum import Flag
from typing import Any, Callable

from business_objects.bo_descriptors import (
    BOBaseBase,
)
from business_objects.business_object_base import BOBase
from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from core.app_logging import getLogger


LOG: Logger = getLogger(__name__)
ATTRIBUTETYPES: dict[type, Callable[[Any], str]] = {
    int: lambda _: "int",
    str: lambda _: "str",
    date: lambda _: "date",
    datetime: lambda _: "datetime",
    dict: lambda _: "dict",
    Flag: lambda _: "flag",
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
        payload = {}
        for k, v in properties.items():
            print(f"{k=}")
            print(f"{v=}")
            if hasattr(v, "bo_type_name"):
                print(f"{v.bo_type_name()=}")
            for attr_type, attr_type_name in ATTRIBUTETYPES.items():
                print(f"  {attr_type=}")
                if issubclass(v, attr_type):
                    payload[k] = attr_type_name(v)
                    break
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
