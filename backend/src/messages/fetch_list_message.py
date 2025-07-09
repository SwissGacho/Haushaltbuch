from logging import Logger
from persistance.bo_list import BOList
from persistance.business_object_base import BOBase
from messages.message import Message, MessageAttribute, MessageType
from core.app_logging import getLogger

LOG: Logger = getLogger(__name__)


class FetchListMessage(Message):
    "Message from the frontend requesting a list of business objects. The list is returned as a list of IDs."

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_FETCH_LIST

    async def handle_message(self, connection):
        business_objects: dict[str, type["BOBase"]] = BOBase.all_business_objects
        object_type = self.message.get(MessageAttribute.WS_ATTR_OBJECT)
        for name in business_objects:

            # right now we just assume every BO is a root tree object
            if name == object_type:
                my_type = business_objects[name]
                assert issubclass(my_type, BOBase)
                boList = BOList(
                    bo_type=my_type,
                    connection=connection,
                )
                assert isinstance(
                    boList, BOList
                ), "BOList should be a subclass of BOList"
                await boList.notify_subscribers()
