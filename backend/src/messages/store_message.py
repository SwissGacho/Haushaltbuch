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
        if bo_id is not None:
            LOG.debug(f"Updating existing BO {bo_type=} {bo_id=}")
            updated_bo = bo_type(bo_id=bo_id)
            for key, value in self.message.get(
                MessageAttribute.WS_ATTR_PAYLOAD, {}
            ).items():
                if key in bo_type.attributes_as_dict().keys():
                    setattr(updated_bo, key, value)
            await updated_bo.store()
            return
        else:
            new_bo = await bo_type().store()
            LOG.debug(f"New BO created from StoreMessage {new_bo=}")


log_exit(LOG)
