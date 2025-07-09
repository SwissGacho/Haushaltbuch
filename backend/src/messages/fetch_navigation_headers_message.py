"""Message from frontend requesting a list of navigation headers to show under an object"""

from logging import Logger
from core.app_logging import getLogger
from persistance.bo_descriptors import BORelation
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

        # For now we just handle the special case of the root tree being requested
        # First create a list of all business objects; these form the headers of the root tree. For every business object, find out whether it's a "root tree" object or not

        parent_object_name = self.message.get(MessageAttribute.WS_ATTR_OBJECT)
        parent_object: type[BOBase] | None = None
        if parent_object is not None:
            parent_object = BOBase.get_business_object_by_name(parent_object_name)
            object_names = [
                attribute[0]
                for attribute in parent_object.attribute_descriptions()
                if attribute[1] == BORelation
            ]
        else:
            business_objects: dict[str, type[BOBase]] = BOBase.all_business_objects
            object_names: list[str] = [name for name in business_objects]
        msg = NavigationHeadersMessage(
            token=self.message.get(MessageAttribute.WS_ATTR_TOKEN)
        )
        msg.add({MessageAttribute.WS_ATTR_PAYLOAD: {"headers": object_names}})
        await connection.send_message(msg)


class NavigationHeadersMessage(Message):
    "Message containing a list of navigation headers applicable to an object"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_NAVIGATION_HEADERS
