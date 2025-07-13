from logging import Logger
from business_objects.bo_list import BOList
from messages.message import Message, MessageAttribute, MessageType
from core.app_logging import getLogger

LOG: Logger = getLogger(__name__)


class FetchListMessage(Message):
    "Message from the frontend requesting a list of business objects. The list is returned as a list of IDs."

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_FETCH_LIST

    async def handle_message(self, connection):
        object_type_name = self.message.get(MessageAttribute.WS_ATTR_OBJECT)

        # Maybe the connection should have a method that creates a BOList?
        BOList(
            bo_type=object_type_name,
            connection=connection,
            notify_subscribers_on_init=True,
        )
