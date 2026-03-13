"""This message is sent by the frontend when
a business object should be persisted in the database."""

from core.app_logging import getLogger, log_exit, Logger

LOG: Logger = getLogger(__name__)

from business_objects.business_object_base import BOBase
from messages.message import Message, MessageAttribute, MessageType


class StoreMessage(Message):
    "Business object to be stored in the DB"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_STORE

    async def handle_message(self, connection):
        "Handle a StoreMessage"
        LOG.debug(f"StoreMessage.handle_message {self.message=}")
        object_type_name = self.message.get(MessageAttribute.WS_ATTR_OBJECT)
        bo_type = BOBase.get_business_object_by_name(str(object_type_name))
        bo_id = self.message.get(MessageAttribute.WS_ATTR_INDEX)

        affected_bo = bo_type(bo_id=bo_id)

        # If there's a payload, update the affected_bo with the new values
        payload = self.message.get(MessageAttribute.WS_ATTR_PAYLOAD)
        if payload is not None:
            for key, value in payload.items():
                if key in bo_type.attributes_as_dict().keys():
                    setattr(affected_bo, key, value)
        await affected_bo.store()


log_exit(LOG)
