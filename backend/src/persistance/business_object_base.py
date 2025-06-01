"""Base module for Business Object logic

Business objects are classes that support persistance in the data base
"""

from asyncio import Task, get_running_loop
from re import sub
from typing import Any, Coroutine, TypeAlias, Optional, Callable
import copy
from datetime import date, datetime, UTC

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

# pylint: disable=wrong-import-position

from persistance.bo_descriptors import BOColumnFlag, BOBaseBase, BODatetime, BOId


class _classproperty:
    def __init__(self, fget: Callable) -> None:
        self.fget = fget

    def __get__(self, owner_self, owner_cls=None):
        return self.fget(owner_cls)


AttributeDescription: TypeAlias = tuple[str, type, BOColumnFlag, dict[str, str]]
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

    def __new__(cls, id: int | None = None, **attributes):
        if id is not None:
            if id in cls._loaded_instances:
                return cls._loaded_instances[id]

        return super().__new__(cls)

    # pylint: disable=redefined-builtin
    def __init__(self, id: int | None = None, **attributes) -> None:
        # LOG.debug(f"BOBase({id=},{attributes})")
        self._data = {}
        self._db_data = {}
        self.id = id
        self.last_updated = None
        for attribute, value in attributes.items():
            self._data[attribute] = value

        loop = get_running_loop()
        for callback in self._creation_subscribers.values():
            try:
                task = loop.create_task(
                    callback(self),
                    name=f"creation_callback_{callback.__name__}_{self.id}",
                )
                task.add_done_callback(self._handle_callback_result)
            except Exception:
                LOG.exception(
                    f"Error scheduling creation callback {callback.__name__} for {self!r}"
                )

    def _handle_callback_result(self, task: Task):
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
        BOBase._business_objects |= {cls.__name__: cls}
        LOG.debug(f"registered class '{cls.__name__}'")

    @_classproperty
    def all_business_objects(self) -> dict[str, type["BOBase"]]:
        "Set of registered Business Objects"
        return self._business_objects

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
    async def sql_create_table(cls):
        "Create a DB table for this class"
        raise NotImplementedError("sql_create_table not implemented")

    def convert_from_db(self, value, typ):
        "convert a value of type 'typ' read from the DB"
        if value is None:
            return None
        if typ == date:
            return date.fromisoformat(value)
        if typ == datetime:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo in [None, UTC]:
                dt = dt.replace(tzinfo=UTC).astimezone(tz=None)
            return dt
        return copy.deepcopy(value)

    @classmethod
    async def count_rows(cls, conditions: Optional[dict] = None) -> int:
        """Count the number of existing business objects in the DB table matching the conditions"""
        raise NotImplementedError("count_rows not implemented")

    @classmethod
    async def get_matching_ids(cls, conditions: dict | None = None) -> list[int]:
        """Get the ids of business objects matching the conditions"""
        raise NotImplementedError("get_matching_ids not implemented")

    async def fetch(self, id=None, newest=None):
        """Fetch the content for a business object instance from the DB.
        If 'id' is given, fetch the identified object
        If 'id' omitted and 'newest'=True fetch the object with highest id
        If the oject is not found in the DB return the instance unchanged
        """
        return self

    async def store(self):
        """Store the business object in the database.
        If 'self.id is None' a new row is inserted
        Else the existing row is updated
        """
        raise NotImplementedError("store not implemented")

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
        if callback_id in cls._creation_subscribers:
            del cls._creation_subscribers[callback_id]
        else:
            LOG.warning(f"Callback ID {callback_id} not found in creation subscribers")


log_exit(LOG)
