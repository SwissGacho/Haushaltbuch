"""Messages to request and send business objects"""

from logging import Logger
from enum import EnumType

from business_objects.bo_descriptors import (
    AttributeAccessLevel,
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

    def __init__(
        self,
        object_type: type[BOBaseBase],
        token: WSToken | None = None,
        status: str | None = None,
    ) -> None:
        super().__init__(token=token, status=status)
        self._object_type = object_type
        payload = self.generate_payload()
        self.add(
            {
                MessageAttribute.WS_ATTR_OBJECT: object_type.bo_type_name(),
                MessageAttribute.WS_ATTR_PAYLOAD: payload,
            }
        )
        # LOG.debug(f"{self.message=}")

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_OBJECT_SCHEMA

    def constraint_values_representation(
        self, constraint_value: str | type[BOBaseBase] | EnumType
    ) -> dict[str, str | list[str]]:
        """Specifies the possible values of a given flag. str for simple flags, dict for enums and relations"""
        if constraint_value is None:
            return {}
        if isinstance(constraint_value, EnumType):
            # LOG.debug(f"{flag=}, {type(flag)=}")
            return {
                "name": constraint_value.__name__,
                "values": [str(v) for v in constraint_value],
            }
        if isinstance(constraint_value, type) and issubclass(
            constraint_value, BOBaseBase
        ):
            return {"relation": constraint_value.bo_type_name()}
        LOG.error(f"Unknown flag type: {constraint_value=}, {type(constraint_value)=}")
        return {"constraint_value": str(constraint_value)}

    def attribute_type_representation(self, attribute_type: AttributeType):
        """String representation of a business attribute type"""
        if attribute_type is None:
            return ""
        return str(attribute_type.value)

    def attribute_representation(
        self, attribute: AttributeDescription
    ) -> dict[str, str | dict[str, dict[str, str | list[str]]]]:
        """Dictionary representation of a business attribute specification"""
        return {
            "type": self.attribute_type_representation(attribute.attribute_type),
            "flags": {
                k: self.constraint_values_representation(v)
                for k, v in attribute.constraint_values.items()
                if v is not None
            },
        }

    def generate_payload(
        self,
    ) -> dict[str, dict[str, str | dict[str, dict[str, str | list[str]]]]]:
        """Generate the payload of the message, containing the object schema"""
        properties = self._object_type.attribute_descriptions()
        payload = {
            desc.name: self.attribute_representation(desc)
            for desc in properties
            if desc.access_level != AttributeAccessLevel.AAL_WRITE_ONLY
        }
        # LOG.debug(f"{payload=}")
        return payload


log_exit(LOG)
