"""Messages to request and send business objects"""

from logging import Logger
from enum import EnumType

from business_objects.bo_descriptors import (
    AttributeType,
    BOBaseBase,
)
from business_objects.business_object_base import AttributeDescription
from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from core.app_logging import getLogger, log_exit


LOG: Logger = getLogger(__name__)


class ObjectSchema(Message):
    "Message containing the schema of a business object"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_OBJECT_SCHEMA

    def flag_values_representation(
        self, flag: str | type[BOBaseBase] | EnumType
    ) -> str | dict[str, str | list[str]]:
        """Specifies the possible values of a given flag. str for simple flags, dict for enums and relations"""
        if flag is None:
            return ""
        if isinstance(flag, str):
            return flag
        if isinstance(flag, EnumType):
            # LOG.debug(f"{flag=}, {type(flag)=}")
            return {"name": flag.__name__, "values": [str(v) for v in flag]}
        if issubclass(flag, BOBaseBase):
            return flag.bo_type_name()
        LOG.error(f"Unknown flag type: {flag=}, {type(flag)=}")
        return str(flag)

    def attribute_type_representation(self, attribute_type: AttributeType):
        """String representation of a business attribute type"""
        if attribute_type is None:
            return ""
        return str(attribute_type.value)

    def attribute_representation(
        self, attribute: AttributeDescription
    ) -> dict[str, str | dict[str, str | str | dict[str, str | list[str]]]]:
        """Dictionary representation of a business attribute specification"""
        return {
            "type": self.attribute_type_representation(attribute.attribute_type),
            "flags": {
                k: self.flag_values_representation(v)
                for k, v in attribute.flag_values.items()
                if v is not None
            },
        }

    def generate_payload(
        self,
    ) -> dict[str, dict[str, str | dict[str, str | str | dict[str, str | list[str]]]]]:
        """Generate the payload of the message, containing the object schema"""
        properties = self._object_type.attribute_descriptions()
        payload = {
            desc.name: self.attribute_representation(desc)
            for desc in properties
            if not desc.is_technical
        }
        # LOG.debug(f"{payload=}")
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
        LOG.debug(f"{self.message=}")


log_exit(LOG)
