"Message containing a list of business objects to be sent to the frontend"

from messages.message import Message, MessageType


class ObjectList(Message):
    "Message containing a list of business objects"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_OBJECT_LIST
