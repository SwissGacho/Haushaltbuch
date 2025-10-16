"""Messages to request and send business objects"""

from logging import Logger
from enum import EnumType, Flag

from business_objects.bo_descriptors import (
    AttributeType,
    BOBaseBase,
)
from business_objects.business_object_base import AttributeDescription
from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from core.app_logging import getLogger


LOG: Logger = getLogger(__name__)


class ObjectSchema(Message):
    "Message containing the schema of a business object"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_OBJECT

    def flag_representation(
        self, flag: str | type[BOBaseBase] | type[Flag] | None
    ) -> str:
        if flag is None:
            return ""
        if isinstance(flag, str):
            return flag
        if isinstance(flag, EnumType):
            LOG.debug(f"{flag=}, {type(flag)=}")
            return flag.__name__
        if issubclass(flag, BOBaseBase):
            return flag.bo_type_name()
        LOG.error(f"Unknown flag type: {flag=}, {type(flag)=}")
        return str(flag)

    def attribute_type_representation(
        self, attribute_type: AttributeType | None
    ) -> str:
        if attribute_type is None:
            return ""
        return attribute_type.value

    def attribute_representation(
        self, attribute: AttributeDescription
    ) -> dict[str, str | dict[str, str]]:
        return {
            "type": self.attribute_type_representation(attribute.attribute_type),
            "flags": {
                k: self.flag_representation(v) for k, v in attribute.flag_values.items()
            },
        }

    def generate_payload(self) -> dict[str, dict[str, str | dict[str, str]]]:
        properties = self._object_type.attribute_descriptions()
        payload = {
            desc.name: self.attribute_representation(desc)
            for desc in properties
            if not desc.is_technical
        }
        return payload

    def __init__(
        self,
        object_type: type[BOBaseBase],
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
