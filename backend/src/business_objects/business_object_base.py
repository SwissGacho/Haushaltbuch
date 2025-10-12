"""Base module for Business Object logic

Business objects are classes that support persistance in the data base
"""

import asyncio
import itertools
from typing import Any, Coroutine, TypeAlias, Optional, Callable

from cycler import V

from core.util import _classproperty
from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

# pylint: disable=wrong-import-position

from business_objects.bo_descriptors import (
    BOColumnFlag,
    BOBaseBase,
    BOId,
    BODatetime,
)


AttributeDescription: TypeAlias = tuple[
    str, type, BOColumnFlag, dict[str, str | BOBaseBase]
]
BOCallback: TypeAlias = Callable[["BOBase"], Coroutine[Any, Any, None]]


class BOBase(BOBaseBase):
    "Business Object baseclass"

    id = BOId(BOColumnFlag.BOC_PK_INC)
    last_updated = BODatetime(BOColumnFlag.BOC_DEFAULT_CURR)
    _table = None
    _attributes: dict[str, list[AttributeDescription]] = {}
    _business_objects: dict[str, type["BOBase"]] = {}

    _loaded_instances: dict[int, "BOBase"] = {}

    _creation_subscribers: dict[int, BOCallback] = {}
    _change_subscribers: dict[int, BOCallback] = {}

    _last_subscriber_id = itertools.count(1)

    def __new__(cls, *args, identity: int | None = None, **attributes):
        if identity is not None:
            if identity in cls._loaded_instances:
                return cls._loaded_instances[identity]

        return super().__new__(cls)

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls._creation_subscribers = {}
        cls._change_subscribers = {}

    # pylint: disable=redefined-builtin
    def __init__(self, *args, id: int | None = None, **attributes) -> None:
        LOG.debug(f"BOBase({id=},{attributes})")
        self._instance_subscribers: dict[int, BOCallback] = {}
        self._data = {}
        self._db_data = {}
        self.id = id
        self.last_updated = None
        self._instance_subscriber_id = itertools.count(1)
        for attribute, value in attributes.items():
            self._data[attribute] = value

    def handle_callback_result(self, task: asyncio.Task):
        """Logs exceptions from background callback tasks."""
        try:
            task.result()  # Raise exception if one occurred during the task
        except Exception:
            LOG.exception(
                f"Exception raised in background creation callback task: {task.get_name()}"
            )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__} "
            f"({', '.join([a+': '+str(v) for a,v in self._data.items()])})"
        )

    @classmethod
    def register_instance(cls, instance: "BOBase"):
        """Register an instance of this class as being loaded from the database."""
        if instance.id is not None:
            cls._loaded_instances[instance.id] = instance  # type: ignore

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}({self.id})"
            if self.id
            else f"{self.__class__.__name__}(no id)"
        )

    @classmethod
    def add_attribute(
        cls,
        attribute_name: str,
        data_type: type,
        constraint_flag: BOColumnFlag,
        **flag_values,
    ):
        if not cls._attributes.get(cls.__name__):
            cls._attributes[cls.__name__] = []
        if any(a[0] == attribute_name for a in cls._attributes[cls.__name__]):
            LOG.warning(
                f"BOBase.add_attribute({cls.__name__}, {attribute_name}) already registered"
            )
            return
        cls._attributes[cls.__name__].append(
            (attribute_name, data_type, constraint_flag, flag_values)
        )

    @classmethod
    def register_persistant_class(cls):
        "Register the Business Object."
        BOBase._business_objects |= {cls._name(): cls}
        LOG.debug(f"registered class '{cls.__name__}' as {cls._name()}")

    @_classproperty
    def all_business_objects(self) -> dict[str, type["BOBase"]]:
        "Set of registered Business Objects"
        return self._business_objects

    @classmethod
    def get_business_object_by_name(cls, name: str) -> type["BOBase"]:
        "Get a business object class by its name"
        if name in cls._business_objects:
            return cls._business_objects[name]
        raise ValueError(f"No type of business object with name '{name}' found")

    @classmethod
    def _name(cls) -> str:
        return cls.__name__.lower()

    @_classproperty
    def table(self) -> str:
        "Name of the BO's DB table"
        return self._table or self._name() + "s"

    @classmethod
    def attributes_as_dict(cls) -> dict[str, type]:
        "dict of BO attribute types with attribute names as keys"
        cls_cols = {a[0]: a[1] for a in cls._attributes.get(cls.__name__, [])}
        assert cls.__base__ is not None, "BOBase.__base__ is None"
        if issubclass(cls.__base__, BOBase):
            return cls.__base__.attributes_as_dict() | cls_cols
        return cls_cols

    @classmethod
    def attribute_descriptions(cls) -> list[AttributeDescription]:
        "list of attribute descriptions"
        cls_cols = cls._attributes.get(cls.__name__, [])
        assert cls.__base__ is not None, "BOBase.__base__ is None"
        if issubclass(cls.__base__, BOBase):
            return cls.__base__.attribute_descriptions() + cls_cols
        return cls_cols

    @classmethod
    def primary_key(cls) -> str:
        "Name of the primary key attribute"
        for name, _, constraint, _ in cls.attribute_descriptions():
            if (
                constraint == BOColumnFlag.BOC_PK
                or constraint == BOColumnFlag.BOC_PK_INC
            ):
                return name
        raise ValueError(f"No primary key defined for {cls.__name__}")

    @classmethod
    def references(cls) -> list[str | BOBaseBase | None]:
        "list of business objects referenced by this class"
        return [
            a[3].get("relation")
            for a in cls.attribute_descriptions()
            if a[1] == BOBaseBase and a[2] == BOColumnFlag.BOC_FK
        ]

    @classmethod
    async def count_rows(cls, conditions: Optional[dict] = None) -> int:
        """Count the number of existing business objects in the DB table matching the conditions"""
        raise NotImplementedError("count_rows not implemented")

    @classmethod
    async def get_matching_ids(cls, conditions: dict | None = None) -> list[int]:
        """Get the ids of business objects matching the conditions"""
        raise NotImplementedError("get_matching_ids not implemented")

    @classmethod
    def subscribe_to_creation(cls, callback: BOCallback):
        """Register a callback to be called when a new instance is created.
        Return a unique id that can be used to unsubscribe."""
        if not callable(callback):
            raise ValueError("Callback must be callable")
        subscriber_id = next(cls._last_subscriber_id)
        cls._creation_subscribers[subscriber_id] = callback
        return subscriber_id

    @classmethod
    def unsubscribe_from_creation(cls, callback_id: int):
        """Unregister a callback from the creation list."""
        # LOG.debug(f"Unsubscribing callback {callback_id} from creation subscribers")
        # LOG.debug(f"Current subscribers: {cls._creation_subscribers}")
        if not callback_id in cls._creation_subscribers:
            LOG.warning(
                f"Callback id {callback_id} not found in creation subscribers: {cls._creation_subscribers}"
            )
            return
        del cls._creation_subscribers[callback_id]

    @classmethod
    def subscribe_to_all_changes(cls, callback: BOCallback) -> int:
        """Register a callback to be called when any instance of this class changes.
        Return a unique id that can be used to unsubscribe."""
        LOG.debug(f"Subscribing callback {callback} to all changes on {cls}")
        if not callable(callback):
            raise ValueError("Callback must be callable")
        # Should only subscribe to subclasses, not to BOBase itself
        if cls is BOBase:
            raise ValueError("Cannot subscribe to changes of BOBase itself")
        subscriber_id = next(cls._last_subscriber_id)
        cls._change_subscribers[subscriber_id] = callback
        return subscriber_id

    @classmethod
    def unsubscribe_from_all_changes(cls, callback_id: int):
        """Unregister a callback from the change list by id."""
        if not callback_id in cls._change_subscribers:
            LOG.warning(
                f"Callback id {callback_id} not found in change subscribers: {cls._change_subscribers}"
            )
            return
        del cls._change_subscribers[callback_id]

    def subscribe_to_instance(self, callback: BOCallback) -> int:
        """Register a callback to be called when this instance changes or is deleted."""
        if not callable(callback):
            raise ValueError("Callback must be callable")
        subscriber_id: int = next(self._instance_subscriber_id)
        self._instance_subscribers[subscriber_id] = callback
        return subscriber_id

    def unsubscribe_from_instance(self, callback_id: int):
        """Unregister a callback from the instance subscriber list by id."""
        if callback_id not in self._instance_subscribers:
            LOG.warning(
                f"BOBase.unsubscribe_from_instance_by_id: no subscribers with id {callback_id}"
            )
            return
        del self._instance_subscribers[callback_id]

    async def business_values_as_dict(self) -> dict[str, Any]:
        "dict of BO attribute values with attribute names as keys"

        value_dict = {k: v for k, v in self._data.items() if k not in ("bo_type")}
        return value_dict

    async def store(self):
        """Store pending changes to the business object.
        In addition, the instance subscribers are notified.
        """
        self.notify_instance_subscribers()
        self.__class__.notify_change_subscribers(self)

    async def _insert_self(self):
        assert self.id is None, "id must be None for insert operation"

    async def _update_self(self):
        assert self.id is not None, "id must not be None for update operation"

    def notify_instance_subscribers(self):
        """Notify all subscribers of this instance about a change."""
        LOG.debug(f"Notifying {len(self._instance_subscribers)} subscribers for {self}")
        if not self.id:
            return
        BOBase.notify_bo_subscribers(self._instance_subscribers, self)

    @classmethod
    def notify_change_subscribers(cls, changed_bo: "BOBase"):
        """Notify all subscribers of this class about a change in an instance."""
        LOG.debug(
            f"Notifying {len(cls._change_subscribers)} change subscribers for {changed_bo}"
        )
        cls.notify_bo_subscribers(cls._change_subscribers, changed_bo)

    @classmethod
    def notify_bo_subscribers(
        cls, subscriptions: dict[int, BOCallback], changed_bo: "BOBase"
    ):
        """Notify all subscribers about a change in a business object."""
        LOG.debug(
            f"Notifying {len(subscriptions)} subscribers for {changed_bo} with {changed_bo.id=}"
        )
        for callback in subscriptions.values():
            try:
                task = asyncio.create_task(
                    callback(changed_bo),
                    name=f"subscriber_callback_{callback.__name__}_{changed_bo.id}",
                )
                task.add_done_callback(changed_bo.handle_callback_result)
            except Exception:
                LOG.exception(
                    f"Error scheduling callback {callback.__name__} for {changed_bo!r}"
                )


log_exit(LOG)
