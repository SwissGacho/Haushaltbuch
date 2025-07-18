"""Base module for Business Object logic

Business objects are classes that support persistance in the data base
"""

import asyncio
from typing import Any, Coroutine, TypeAlias, Optional, Callable

from core.util import _classproperty
from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

# pylint: disable=wrong-import-position

from business_objects.bo_descriptors import (
    BOColumnFlag,
    BOBaseBase,
    BOId,
    BOInt,
    BODatetime,
)


AttributeDescription: TypeAlias = tuple[str, type, str, dict[str, str | BOBaseBase]]
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
    _last_subscriber_id = 0

    def __new__(cls, id: int | None = None, *args, **attributes):
        if id is not None:
            if id in cls._loaded_instances:
                return cls._loaded_instances[id]

        return super().__new__(cls)

    # pylint: disable=redefined-builtin
    def __init__(self, id: int | None = None, *args, **attributes) -> None:
        LOG.debug(f"BOBase({id=},{attributes})")
        self._data = {}
        self._db_data = {}
        self.id = id
        self.last_updated = None
        for attribute, value in attributes.items():
            self._data[attribute] = value

        for callback in self._creation_subscribers.values():
            try:
                task = asyncio.create_task(
                    callback(self),
                    name=f"creation_callback_{callback.__name__}_{self.id}",
                )
                task.add_done_callback(self._handle_callback_result)
            except Exception:
                LOG.exception(
                    f"Error scheduling creation callback {callback.__name__} for {self!r}"
                )

    def _handle_callback_result(self, task: asyncio.Task):
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
        if instance.id is not None:
            cls._loaded_instances[instance.id] = instance  # type: ignore

    def __str__(self) -> str:
        return str(self.id)

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
    def attributes_as_dict(cls) -> dict:
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
        for name, data_type, constraint, pars in cls.attribute_descriptions():
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
        """Register a callback to be called when a new instance is created. Return a unique id that can be used to unsubscribe."""
        if not callable(callback):
            raise ValueError("Callback must be callable")
        subscriber_id = cls._last_subscriber_id
        cls._last_subscriber_id += 1
        cls._creation_subscribers[subscriber_id] = callback
        return subscriber_id

    @classmethod
    def unsubscribe_from_creation(cls, callback_id: int):
        """Unregister a callback from the creation list."""
        LOG.debug(f"Unsubscribing callback {callback_id} from creation subscribers")
        LOG.debug(f"Current subscribers: {cls._creation_subscribers}")
        if callback_id in cls._creation_subscribers:
            del cls._creation_subscribers[callback_id]
        else:
            LOG.warning(
                f"Callback id {callback_id} not found in creation subscribers: {cls._creation_subscribers}"
            )

    async def _insert_self(self):
        assert self.id is None, "id must be None for insert operation"

    async def _update_self(self):
        assert self.id is not None, "id must not be None for update operation"


log_exit(LOG)
