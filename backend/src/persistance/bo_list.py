"""
- Lists are transient objects
- They create a temporary view with the conditions of the list and the ids of the selected objects and the connection they send messages to
- Lists subscribe to all events relating to their BO class
- When an event is received, the view is refreshed. If elements were added or removed, the connection sends an appropriate messages
"""

from typing import Type
from core.app_logging import getLogger

# from server.ws_connection import WS_Connection
from messages.message import MessageAttribute

from messages.object_list import ObjectList
from persistance.business_object_base import BOBase
from persistance.transient_business_object import TransientBusinessObject
from server.ws_message_sender import WSMessageSender


LOG = getLogger(__name__)


class BOList(TransientBusinessObject, WSMessageSender):
    """Represents a list of business objects of a certain type. The list subscribes to events of the business object type and updates its own subscribers accordingly."""

    def __init__(self, bo_type: Type[BOBase], connection) -> None:
        print(f"BOList.__init__({bo_type=}, {connection=})")
        LOG.debug(f"===============================================")
        LOG.debug(f"BOList.__init__({bo_type=}, {connection=})")
        TransientBusinessObject.__init__(self)
        WSMessageSender.__init__(self, connection=connection)
        LOG.debug(f"BOList super called")
        self._bo_type = bo_type
        self._subscription_id = self._bo_type.subscribe_to_creation(self._handle_event_)

    async def _get_objects_(self):
        rslt = await self._bo_type.get_matching_ids()
        return [str(cur) for cur in rslt]

    async def _handle_event_(self, changed_bo: BOBase):
        LOG.debug(f"BOList._handle_event_({changed_bo})")
        await self.notify_subscribers()

    async def notify_subscribers(self):
        name_list = await self._get_objects_()
        LOG.debug(
            f"Updating subscribers of {self._bo_type.__name__} with {len(name_list)} objects"
        )
        LOG.debug(f"BOList.update_subscribers {name_list=}")

        # log the ids
        msg = ObjectList()
        msg.add({MessageAttribute.WS_ATTR_PAYLOAD: {"objects": name_list}})
        LOG.debug(f"BOList.update_subscribers {msg=}")
        await self.send_message(msg)

    def handle_connection_closed(self):
        LOG.debug(f"BOList.handle_connection_closed({self._connection})")
        LOG.debug(f"{self._subscription_id=}, {self._bo_type=}")
        self._bo_type.unsubscribe_from_creation(self._subscription_id)
