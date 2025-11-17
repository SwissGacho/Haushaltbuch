"""Message from frontend requesting a list of navigation headers to show under an object"""

from core.app_logging import getLogger, log_exit, Logger

LOG: Logger = getLogger(__name__)
from business_objects.bo_descriptors import BORelation
from business_objects.business_object_base import BOBase
from messages.message import Message, MessageAttribute, MessageType
from server.ws_connection_base import WSConnectionBase

LOG: Logger = getLogger(__name__)


class FetchNavigationHeadersMessage(Message):
    """Message requesting the list headers applicable to an object.
    If no object is provided, return a list of headers applicable to the root tree of the user.
    """

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_FETCH_NAVIGATION_HEADERS

    async def handle_message(self, connection: WSConnectionBase):
        "Handle a FetchListMessage"

        parent_object_name = self.message.get(MessageAttribute.WS_ATTR_OBJECT)
        parent_object: type[BOBase] | None = None
        object_names: list[str] = []

        # If headers were request for a specific object, get the relations of that object
        if parent_object is not None:
            parent_object = BOBase.get_business_object_by_name(parent_object_name)
            object_names = [
                attribute.name
                for attribute in parent_object.attribute_descriptions()
                if attribute.data_type == BORelation
            ]

        # If no object was specified, return the headers of the root tree
        #  - for now these are all business objects
        else:
            object_names = list(
                BOBase.all_business_objects.keys()  # pylint:disable=no-member
            )
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


log_exit(LOG)
