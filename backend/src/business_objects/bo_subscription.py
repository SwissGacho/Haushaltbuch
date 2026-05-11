"""Subscription business object for a single BO instance.

This module defines ``BOSubscription``, a transient business object that
subscribes to change events of one concrete business object instance and
forwards updates to connected clients via WebSocket messages.
"""

from typing import TypeVar

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from messages.bo_message import ObjectMessage
from server.ws_connection_base import WSConnectionBase
from server.ws_message_sender import WSMessageSender
from business_objects.business_object_base import BOBase
from business_objects.transient_business_object import TransientBusinessObject


import asyncio
from typing import Generic, Type, cast

T = TypeVar("T", bound=BOBase)


class BOSubscription(Generic[T], TransientBusinessObject, WSMessageSender):
    """Represents a subscription to a single business object.
    The subscription listens for changes to the business object and notifies
    subscribers via WebSocket messages.
    """

    def __init__(
        self,
        bo_type: Type[T] | str,
        connection: WSConnectionBase,
        notify_subscribers_on_init: bool = False,
        id: int | None = None,
    ) -> None:
        # Initialize _subscription_id and self._bo_type, as it is used in cleanup if __init__ fails
        self._subscription_id: int | None = None
        # LOG.debug(f"BOSubscription.__init__({bo_type=}, {id=}, {connection=})")

        TransientBusinessObject.__init__(self)
        WSMessageSender.__init__(self, connection=connection)

        if isinstance(bo_type, str):
            # LOG.debug(f"BOSubscription.__init__: Resolving bo_type from string {bo_type}")
            try:
                bo_type = cast(
                    Type[T], BOBase.get_business_object_by_name(str(bo_type))
                )
            except ValueError as e:
                raise ValueError(
                    f"BOSubscription.__init__: Invalid business object type {bo_type}: {str(e)}"
                ) from e
        if not (isinstance(bo_type, type) and issubclass(bo_type, BOBase)):
            raise TypeError(
                f"BOSubscription.__init__: Could not resolve bo_type {bo_type}, is {type(bo_type)}"
            )
        self._bo_type: Type[T] = bo_type
        self._instance_subscriptions: dict[int, T] = {}
        self._initialize_subscriptions(id=id)
        connection.unregister_other_senders(self)
        if notify_subscribers_on_init:
            asyncio.create_task(self.notify_subscription_subscribers())

    def _initialize_subscriptions(self, **kwargs):
        if "id" not in kwargs:
            raise ValueError("BOSubscription requires an 'id' argument")
        if not isinstance(kwargs["id"], int):
            raise ValueError("BOSubscription requires an 'id' argument of type int")

        bo_id = int(kwargs["id"])
        bo = self._bo_type(bo_id=bo_id)

        self._subscription_id = bo.subscribe_to_instance(self._handle_event_)
        self._obj = bo

    async def _get_objects_(self) -> list[T]:
        if self._bo_type is None:
            LOG.debug(
                "BOSubscription._get_objects_: _bo_type is None, no objects to return"
            )
            return []
        if not hasattr(self, "_obj"):
            LOG.debug(
                "BOSubscription._get_objects_: _obj not set, no objects to return"
            )
            return []
        return [self._obj]

    async def _handle_event_(self, _: BOBase):
        """Should be called when the underlying information of the list changes.
        This method will update the list of objects and notify subscribers."""
        # LOG.debug(f"BOSubscription._handle_event_({changed_bo}) - {self._bo_type=}")
        if self._bo_type is None:
            LOG.debug("BOSubscription._handle_event_: _bo_type is None, nothing to do")
            return

        await self.notify_subscription_subscribers()

    async def notify_subscription_subscribers(self):
        """Notify subscribers about the current state of the list."""
        name_list = [cur.id for cur in await self._get_objects_()]
        # LOG.debug(
        #     f"Updating subscribers of {(self._bo_type.__name__ if self._bo_type else 'Undefined')} "
        #     f"with {len(name_list)} objects"
        # )
        BOBase.subscriptions_report()

        await self.send_message(
            ObjectMessage(
                object_type=self._bo_type,
                index=self._obj.id,
                payload=await self._obj.business_values_as_dict(),
            )
        )

    def cleanup(self):
        # LOG.debug(f"BOSubscription.cleanup({self._connection})")
        # LOG.debug(f"{self._subscription_id=}, {self._bo_type=}")
        if self._subscription_id is None:
            LOG.debug(
                "BOSubscription.cleanup: Nothing to cleanup, _subscription_id is None"
            )
            return
        if self._bo_type is None:
            LOG.debug("BOSubscription.cleanup: Cannot cleanup, _bo_type is None")
            return
        if self._obj is None:
            LOG.debug("BOSubscription.cleanup: Cannot cleanup, _obj is None")
            return
        self._obj.unsubscribe_from_instance(self._subscription_id)
        self._connection.unregister_message_sender(self)


log_exit(LOG)
