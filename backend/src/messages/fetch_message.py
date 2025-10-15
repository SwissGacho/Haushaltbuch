from business_objects.bo_list import BOSubscription
from messages.message import Message, MessageType, MessageAttribute
from server.ws_connection_base import WSConnectionBase


class FetchMessage(Message):
    "Message requesting a business object"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_FETCH

    async def handle_message(self, connection: WSConnectionBase):
        "handle a fetch message"
        object_type_name = self.message.get(MessageAttribute.WS_ATTR_OBJECT)

        BOSubscription(
            bo_type=object_type_name,
            connection=connection,
            id=self.message.get(MessageAttribute.WS_ATTR_INDEX),
            notify_subscribers_on_init=True,
        )
