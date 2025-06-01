"""- Lists are transient objects
- They create a temporary view with the conditions of the list and the ids of the selected objects and the connection they send messages to
- Lists subscribe to all events relating to their BO class
- When an event is received, the view is refreshed. If elements were added or removed, the connection sends an appropriate messages
"""

from typing import Type
from core.app_logging import getLogger
from server.ws_connection import WS_Connection
from messages.message import MessageAttribute
from messages.data import ObjectList
from persistance.business_object_base import BOBase
from persistance.transient_business_object import TransientBusinessObject

LOG = getLogger(__name__)


class BOList(TransientBusinessObject):
    def __init__(self, bo_type: Type[BOBase], connection: WS_Connection, token) -> None:
        super().__init__()
        self._token = token
        self._bo_type = bo_type
        self._connection = connection
        self._bo_type.subscribe_to_creation(self._handle_event_)

    async def _get_objects_(self):
        rslt = await self._bo_type.get_matching_ids()
        return [str(cur) for cur in rslt]

    async def _handle_event_(self, changed_bo: BOBase):
        LOG.debug(f"BOList._handle_event_({changed_bo})")
        await self.update_subscribers()

    async def update_subscribers(self):
        # For now we just handle the special case of the root tree being requested
        # First create a list of all business objects; these form the headers of the root tree. For every business object, find out whether it's a "root tree" object or not
        name_list = self._get_objects_()

        # log the ids
        LOG.debug(f"{name_list=}")
        msg = ObjectList(token=self._token)
        msg.message |= {MessageAttribute.WS_ATTR_PAYLOAD: {"objects": name_list}}
        LOG.debug(f"FetchListMessage.handle_message {msg=}")
        await self._connection.send_message(msg)
