"""
- Lists are transient objects
- They create a temporary view with the conditions of the list and the ids of the selected
    objects and the connection they send messages to
- Lists subscribe to all events relating to their BO class
- When an event is received, the view is refreshed. If elements were added or removed,
    the connection sends appropriate messages
"""

from typing import TypeVar

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

# from server.ws_connection import WS_Connection
from messages.message import MessageAttribute

from messages.object_list import ObjectList
from business_objects.business_object_base import BOBase
from business_objects.bo_subscription import BOSubscription
from business_objects.persistant_business_object import PersistentBusinessObject

T = TypeVar("T", bound=BOBase)


class BOList(BOSubscription[T]):
    """Represents a list of business objects of a certain type. The list subscribes to events of
    the business object type and updates its own subscribers accordingly."""

    def __init__(self, conditions: dict | None = None, **kwargs) -> None:
        self._conditions = conditions
        super().__init__(**kwargs)

    def _initialize_subscriptions(self, **kwargs):
        self._subscription_id = self._bo_type.subscribe_to_all_changes(
            self._handle_event_
        )

    async def notify_subscription_subscribers(self):
        """Notify subscribers about the current state of the list."""
        if self._bo_type is None or not issubclass(
            self._bo_type, PersistentBusinessObject
        ):
            raise ValueError(
                f"BOList.notify_subscription_subscribers: _bo_type is not a PersistentBusinessObject ({self._bo_type})"
            )
        bo_type = self._bo_type.__name__
        name_components = self._bo_type.display_name_components()
        name_list = [
            {"id": cur.id, "display_name": cur.display_name}
            for cur in await self._bo_type.get_matching_objects(
                attributes=name_components, conditions=self._conditions
            )
        ]
        # LOG.debug(f"Updating subscribers of {bo_type} with {len(name_list)} objects")
        msg = ObjectList()
        msg.add(
            {
                MessageAttribute.WS_ATTR_PAYLOAD: {"objects": name_list},
                MessageAttribute.WS_ATTR_INDEX: self.id,
            }
        )
        # LOG.debug(f"BOList.update_subscribers {msg=}")

        # TODO: Why await? Or rather, why return the result of await, which is None?
        return await self.send_message(msg)

    async def _get_objects_(self) -> list[T]:
        if self._bo_type is None:
            # LOG.debug("BOList._get_objects_: _bo_type is None, no objects to return")
            return []
        rslt = [
            self._bo_type(bo_id=cur)
            for cur in await self._bo_type.get_matching_ids(conditions=self._conditions)
        ]
        return rslt

    def cleanup(self):
        # LOG.debug(f"BOList.cleanup({self._connection})")
        # LOG.debug(f"{self._subscription_id=}, {self._bo_type=}")
        if self._subscription_id is None:
            # LOG.debug("BOList.cleanup: Nothing to cleanup, _subscription_id is None")
            return
        if self._bo_type is None:
            # LOG.debug("BOList.cleanup: Cannot cleanup, _bo_type is None")
            return
        self._bo_type.unsubscribe_from_all_changes(self._subscription_id)
        self._connection.unregister_message_sender(self)


log_exit(LOG)
