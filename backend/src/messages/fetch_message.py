"Message from frontend requesting a business object"

from core.app_logging import getLogger, log_exit, Logger

LOG: Logger = getLogger(__name__)
from business_objects.bo_subscription import BOSubscription
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
        conditions = self.message.get(MessageAttribute.WS_ATTR_CONDITIONS)

        if object_type_name and isinstance(object_type_name, str):
            BOSubscription(
                bo_type=object_type_name,
                connection=connection,
                index=self.message.get(MessageAttribute.WS_ATTR_INDEX),
                conditions=conditions,
                notify_subscribers_on_init=True,
            )
        else:
            raise TypeError(
                "FetchMessage missing or invalid "
                f"{MessageAttribute.WS_ATTR_OBJECT} attribute"
            )


log_exit(LOG)
