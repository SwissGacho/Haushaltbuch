"""This message is sent by the frontend when a business object should be persisted in the database."""

from logging import Logger

from business_objects.business_object_base import BOBase
from core.app_logging import getLogger
from messages.message import Message, MessageAttribute, MessageType

LOG: Logger = getLogger(__name__)


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
        LOG.debug(f"Storing new BO of type {bo_type=}")

        new_bo = await bo_type().store()

        LOG.debug(f"New BO created from StoreMessage {new_bo=}")
