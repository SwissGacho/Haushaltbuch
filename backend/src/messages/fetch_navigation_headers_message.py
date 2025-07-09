"""Message from frontend requesting a list of navigation headers to show under an object"""

from logging import Logger
from core.app_logging import getLogger
from persistance.business_object_base import BOBase
from messages.message import Message, MessageAttribute, MessageType

LOG: Logger = getLogger(__name__)


class FetchNavigationHeadersMessage(Message):
    "Message requesting the list headers applicable to an object. If no object is provided, return a list of headers applicable to the root tree of the user."

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_FETCH_NAVIGATION_HEADERS

    async def handle_message(self, connection):
        "Handle a FetchListMessage"
        business_objects: dict[str, type[BOBase]] = BOBase.all_business_objects
        root_object_names: list[str] = [name for name in business_objects]
        msg = NavigationHeadersMessage(
            token=self.message.get(MessageAttribute.WS_ATTR_TOKEN)
        )
        msg.add({MessageAttribute.WS_ATTR_PAYLOAD: {"headers": root_object_names}})
        await connection.send_message(msg)


class NavigationHeadersMessage(Message):

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_NAVIGATION_HEADERS
