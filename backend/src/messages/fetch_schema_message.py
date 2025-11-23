"Message from frontend requesting a description of a business object type"

from core.app_logging import getLogger, log_exit, Logger

LOG: Logger = getLogger(__name__)
from business_objects.business_object_base import BOBase
from messages.message import Message, MessageType, MessageAttribute
from messages.object_schema import ObjectSchema
from server.ws_connection_base import WSConnectionBase


class FetchSchemaMessage(Message):
    "Message requesting the attribute descriptions of a business object"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_FETCH_SCHEMA

    async def handle_message(self, connection: WSConnectionBase):
        "handle a fetch message"
        # LOG.debug("Handling fetch schema message")
        object_type_name = self.message.get(MessageAttribute.WS_ATTR_OBJECT)
        if object_type_name is None:
            raise ValueError(
                "FetchSchemaMessage missing "
                f"{MessageAttribute.WS_ATTR_OBJECT} attribute"
            )
        try:
            requested_type = BOBase.get_business_object_by_name(object_type_name)
        except ValueError as e:
            raise ValueError(f"Invalid schema requested: {object_type_name=}") from e
        msg = ObjectSchema(
            token=self.token,
            object_type=requested_type,
        )
        # LOG.debug(f"Sending object schema message: {msg}")
        await connection.send_message(msg)


log_exit(LOG)
