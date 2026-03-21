"""Messages used during application setup"""

from enum import StrEnum
from messages.fetch_message import FetchMessage
from messages.store_message import StoreMessage
from messages.message import MessageType, MessageAttribute
from messages.bo_message import ObjectMessage, DataObjectTypes
from core.app import App
from core.configuration.db_config import DBConfig
from core.configuration.setup_config import ConfigSetup

from core.app_logging import getLogger

LOG = getLogger(__name__)


class _SetupPayloadKeys(StrEnum):
    DBCFG_CFG_SEARCH_PATH = "search_path"
    DBCFG_SYSTEM = "system"
    DBCFG_DB_LOCATIONS = "db_paths"


class FetchSetupMessage(FetchMessage):
    "Fetch the actual configuration during setup."

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_FETCH_SETUP

    async def handle_message(self, connection):
        "handle a request for the configuration from the setup dialogue"
        object_type = self.message.get(MessageAttribute.WS_ATTR_OBJECT)
        index = self.get_str(MessageAttribute.WS_ATTR_INDEX)
        token = self.message.get(MessageAttribute.WS_ATTR_TOKEN)
        if object_type == DataObjectTypes.DO_TYPE_SETUP_CONFIG:
            cfg_searchpath = DBConfig.cfg_searchpath()
            db_locations = DBConfig.db_locations()
            msg = ObjectSetupMessage(
                object_type=DataObjectTypes.DO_TYPE_SETUP_CONFIG,
                index=index,
                payload={
                    _SetupPayloadKeys.DBCFG_SYSTEM: App.config_object().system,
                    _SetupPayloadKeys.DBCFG_CFG_SEARCH_PATH: [
                        str(p) for p in cfg_searchpath
                    ],
                    _SetupPayloadKeys.DBCFG_DB_LOCATIONS: [
                        str(p) for p in db_locations
                    ],
                },
                token=token,
            )
            await connection.send_message(msg)


class ObjectSetupMessage(ObjectMessage):
    "Send the actual configuration for the setup dialogue"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_OBJECT_SETUP


class StoreSetupMessage(StoreMessage):
    "Receive result of the setup dialogue and store the configuration accordingly"

    @classmethod
    def message_type(cls) -> MessageType:
        return MessageType.WS_TYPE_STORE_SETUP

    async def handle_message(self, connection):
        "Handle a StoreMessage"
        LOG.debug(f"StoreSetupMessage.handle_message({self.message=})")
        await ConfigSetup.setup_configuration(
            self.get_dict(MessageAttribute.WS_ATTR_PAYLOAD) or {}
        )
