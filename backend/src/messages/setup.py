""" Messages used during application setup
"""

from core.app import App
from core.config import Config
from messages.message import MessageType, MessageAttribute
from messages.data import FetchMessage, ObjectMessage, StoreMessage, DataObjectTypes

from core.app_logging import getLogger

LOG = getLogger(__name__)


class FetchSetupMessage(FetchMessage):
    "Fetch the actual configuration during setup."

    @classmethod
    def message_type(cls):
        return MessageType.WS_TYPE_FETCH_SETUP

    async def handle_message(self, connection):
        "handle a request for the configuration from the setup dialogue"
        object_type = self.message.get(MessageAttribute.WS_ATTR_OBJECT)
        index = self.message.get(MessageAttribute.WS_ATTR_INDDEX)
        token = self.message.get(MessageAttribute.WS_ATTR_TOKEN)
        if object_type == DataObjectTypes.DO_TYPE_SETUP_CONFIG:
            msg = ObjectSetupMessage(
                object_type=object_type,
                index=index,
                payload=[
                    str(p) for p in App.configuration.get(Config.CONFIG_CFG_SEARCH_PATH)
                ],
                token=token,
            )
            await connection.send_message(msg)


class ObjectSetupMessage(ObjectMessage):
    "Send the actual configuration for the setup dialogue"

    @classmethod
    def message_type(cls):
        return MessageType.WS_TYPE_OBJECT_SETUP


class StoreSetupMessage(StoreMessage):
    "Receive result of the setup dialogue and store the configuration accordingly"

    @classmethod
    def message_type(cls):
        return MessageType.WS_TYPE_STORE_SETUP

    async def handle_message(self, connection):
        "Handle a StoreMessage"
        LOG.debug(f"{self=} {self.message=}")
