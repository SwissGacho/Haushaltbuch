"""Messages to request and send business objects"""

from logging import Logger

from business_objects.bo_descriptors import (
    BODate,
    BODatetime,
    BODict,
    BOFlag,
    BOInt,
    BORelation,
    BOStr,
)
from business_objects.business_object_base import BOBase
from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from core.base_objects import BaseObject
from core.app_logging import getLogger
from typing import Any, Callable


LOG: Logger = getLogger(__name__)
ATTRIBUTETYPES: dict[type, Callable[[Any], str]] = {
    BOInt: lambda x: "int",
    BOStr: lambda x: "str",
    BODate: lambda x: "date",
    BODatetime: lambda x: "datetime",
    BODict: lambda x: "dict",
    BOFlag: lambda x: "flag",
    BORelation: lambda x: ".".join(["relation", str(x._flag_values.get("relation"))]),
}


class ObjectSchema(Message):
    "Message containing the schema of a business object"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_OBJECT

    def __init__(
        self,
        object_type: type[BOBase],
        token: WSToken | None = None,
        status: str | None = None,
    ) -> None:

        properties = object_type.attributes_as_dict()
        payload = {
            k: ATTRIBUTETYPES.get(v, "unknown")
            for k, v in properties.items()
            if v in ATTRIBUTETYPES and k != "last_updated"
        }
        self.message = {}
        super().__init__(token=token, status=status)
        self.add(
            {
                MessageAttribute.WS_ATTR_OBJECT: object_type.bo_type_name(),
                MessageAttribute.WS_ATTR_PAYLOAD: payload,
            }
        )
