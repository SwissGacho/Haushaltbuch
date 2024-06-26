""" Messages used during application setup
"""

from enum import StrEnum
from core.app import App
from core.config import Config
from messages.message import MessageType, MessageAttribute
from messages.data import FetchMessage, ObjectMessage, StoreMessage, DataObjectTypes

from core.app_logging import getLogger

LOG = getLogger(__name__)


class _SetupPayloadKeys(StrEnum):
    DBCFG_CFG_FILE = "dbcfg_file"
    DBCFG = "configuration/dbConfig"
    DBCFG_CFG_SEARCH_PATH = "search_path"
    DBCFG_SYSTEM = "system"
    DBCFG_DB_LOCATIONS = "db_paths"


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
                object_type=DataObjectTypes.DO_TYPE_SETUP_CONFIG,
                index=index,
                payload={
                    _SetupPayloadKeys.DBCFG_SYSTEM: App.configuration.get(
                        Config.CONFIG_SYSTEM
                    ),
                    _SetupPayloadKeys.DBCFG_CFG_SEARCH_PATH: [
                        str(p)
                        for p in App.configuration.get(Config.CONFIG_CFG_SEARCH_PATH)
                    ],
                    _SetupPayloadKeys.DBCFG_DB_LOCATIONS: [
                        str(p)
                        for p in App.configuration.get(Config.CONFIG_DB_LOCATIONS)
                    ],
                },
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

    def _get_payload_item(self, key: _SetupPayloadKeys):
        payload = self.message.get(MessageAttribute.WS_ATTR_PAYLOAD)
        for key_part in key.split("/"):
            payload = payload.get(key_part)
        return payload

    async def handle_message(self, connection):
        "Handle a StoreMessage"
        LOG.debug(f"{self.message=}")
        dbcfg_cfg_file = self._get_payload_item(_SetupPayloadKeys.DBCFG_CFG_FILE)
        dbcfg = self._get_payload_item(_SetupPayloadKeys.DBCFG)
        LOG.debug(f"{dbcfg_cfg_file=} {dbcfg=}")
        App.configuration[Config.CONFIG_DB] = dbcfg
        App.config_object.write_db_cfg_file(dbcfg_cfg_file)
