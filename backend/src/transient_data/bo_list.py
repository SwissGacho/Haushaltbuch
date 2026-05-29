"""
- Lists are transient objects
- They create a temporary view with the conditions of the list and the ids of the selected
    objects and the connection they send messages to
- Lists subscribe to all events relating to their BO class
- When an event is received, the view is refreshed. If elements were added or removed,
    the connection sends appropriate messages
"""

import pprint
from typing import Any

from core.app_logging import getLogger, log_exit, VERBOSE_DEBUG

LOG = getLogger(__name__)

from business_objects.business_object_base import BOBase, BOCallback
from business_objects.transient_business_object import TransientBusinessObject


class BOList(TransientBusinessObject):
    """Represents a list of business object instances of a certain type. The list subscribes to events of
    the business object type and updates its own subscribers accordingly."""

    def __init__(self, index: str, conditions: dict | None = None, **kwargs) -> None:
        LOG.debug(f"BOList.__init__({index=}, {conditions=}, {kwargs=})")
        if not isinstance(index, str):
            raise TypeError(
                f"BOList.__init__: index must be a string, got {type(index)}"
            )
        self._bo_type = BOBase.get_business_object_by_name(index)
        self._conditions = conditions
        self._subscription_id: int | None = None
        super().__init__(**kwargs)

    async def _on_change(self, _: BOBase) -> None:
        self.notify_instance_subscribers()

    def subscribe_to_instance(self, callback: BOCallback) -> int:
        LOG.debug(
            f"{str(self)}.subscribe_to_instance: Subscribing to {self._bo_type.__name__} "
            f"{'with conditions ' + str(self._conditions) if self._conditions else 'without conditions'}"
        )
        instance_subscription_id = super().subscribe_to_instance(callback)

        if self._subscription_id is None:
            self._subscription_id = self._bo_type.subscribe_to_all_changes(
                self._on_change
            )

        LOG.log(
            VERBOSE_DEBUG,
            f"{str(self)}.subscribe_to_instance: Subscribed to {self._bo_type.__name__}: "
            f"instance subscription id = {instance_subscription_id}; "
            f"change subscription id = {self._subscription_id}",
        )
        return instance_subscription_id

    def unsubscribe_from_instance(self, callback_id: int):
        LOG.log(
            VERBOSE_DEBUG,
            f"{str(self)}.unsubscribe_from_instance: Unsubscribing from {self._bo_type.__name__} "
            f"with instance subscription id = {callback_id}",
        )
        super().unsubscribe_from_instance(callback_id)
        if not self._instance_subscribers and self._subscription_id is not None:
            self._bo_type.unsubscribe_from_all_changes(self._subscription_id)
            self._subscription_id = None

    async def business_values_as_dict(self) -> dict[str, Any]:
        LOG.debug(
            f"{str(self)}.business_values_as_dict: bo={self._bo_type}, conditions={self._conditions}"
        )
        name_components = self._bo_type.display_name_components() or ["name"]
        matching_objects = await self._bo_type.get_matching_objects(
            attributes=name_components, conditions=self._conditions
        )
        name_list = [
            {"id": cur.id, "display_name": cur.display_name} for cur in matching_objects
        ]
        if LOG.isEnabledFor(VERBOSE_DEBUG):
            LOG.log(
                VERBOSE_DEBUG,
                f"Updating subscribers of {self._bo_type} with {len(name_list)} objects:",
            )
            for obj in pprint.pformat(
                name_list,
                indent=4,
                width=120,
                compact=True,
            ).splitlines():
                LOG.log(VERBOSE_DEBUG, f" - {obj}")
        else:
            LOG.debug(
                f"Updating subscribers of {self._bo_type} with {len(name_list)} objects"
            )
        return {"objects": name_list}


log_exit(LOG)
