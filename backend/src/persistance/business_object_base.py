"""Base module for Business Object logic

Business objects are classes that support persistance in the data base
"""

from typing import Self, TypeAlias, Optional, Callable
import copy
from datetime import date, datetime, UTC

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

# pylint: disable=wrong-import-position

from persistance.bo_descriptors import BOColumnFlag, BOBaseBase, BOInt, BODatetime
from database.sql import SQL
from database.sql_statement import CreateTable
from database.sql_expression import Eq, Filter, SQLExpression, Value


class _classproperty:
    def __init__(self, fget: Callable) -> None:
        self.fget = fget

    def __get__(self, owner_self, owner_cls=None):
        return self.fget(owner_cls)


AttributeDescription: TypeAlias = tuple[str, type, str, dict[str, str]]


class BOBase(BOBaseBase):
    "Business Object baseclass"

    id = BOInt(BOColumnFlag.BOC_PK_INC)
    last_updated = BODatetime(BOColumnFlag.BOC_DEFAULT_CURR)
    _table = None
    _attributes: dict[str, list[AttributeDescription]] = {}
    _business_objects: dict[str, type["BOBase"]] = {}

    # pylint: disable=redefined-builtin
    def __init__(self, id=None, **attributes) -> None:
        # LOG.debug(f"BOBase({id=},{attributes})")
        self._data = {}
        self._db_data = {}
        self.id = id
        self.last_updated = None
        for attribute, value in attributes.items():
            self._data[attribute] = value

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__} "
            f"({', '.join([a+': '+str(v) for a,v in self._data.items()])})"
        )

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
        if issubclass(cls.__base__, BOBase):
            return cls.__base__.attributes_as_dict() | cls_cols
        return cls_cols

    @classmethod
    def attribute_descriptions(cls) -> list[AttributeDescription]:
        "list of attribute descriptions"
        cls_cols = cls._attributes.get(cls.__name__, [])
        if issubclass(cls.__base__, BOBase):
            return cls.__base__.attribute_descriptions() + cls_cols
        return cls_cols

    @classmethod
    async def sql_create_table(cls):
        "Create a DB table for this class"
        attributes = cls.attribute_descriptions()
        async with SQL() as sql:
            create_table: CreateTable = sql.create_table(cls.table)
            LOG.debug(f"BOBase.sql_create_table():  {cls.table=}")
            for name, data_type, constraint, pars in attributes:
                LOG.debug(f" -  {name=}, {data_type=}, {constraint=}, {pars=})")
                create_table.column(name, data_type, constraint, **pars)
            await create_table.execute(close=0)

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
        async with SQL() as sql:
            select = sql.select(["count(*) as count"]).from_(cls.table)
            if conditions:
                select.where(Filter(conditions))
            result = await (await select.execute(close=1)).fetchone()
        # LOG.debug(f"BOBase.count_rows({conditions=}) {result=} -> return {result["count"]}")
        return result["count"]

    @classmethod
    async def get_matching_ids(cls, conditions: dict) -> list[int]:
        """Get the ids of business objects matching the conditions"""
        async with SQL() as sql:
            select = sql.select(["id"]).from_(cls.table)
            select.where(Filter(conditions))
            result = await (await select.execute(close=1)).fetchall()
        # LOG.debug(f"BOBase.get_matching_ids({conditions=}) -> {result=}")
        return [id["id"] for id in result]

    async def fetch(self, id=None, newest=None):
        """Fetch the content for a business object instance from the DB.
        If 'id' is given, fetch the identified object
        If 'id' omitted and 'newest'=True fetch the object with highest id
        If the oject is not found in the DB return the instance unchanged
        """
        # LOG.debug(f'BOBase.fetch({id=}, {newest=})')
        if id is None:
            id = self.id
        if id is None and newest is None:
            LOG.debug(f"fetching {self} without id or newest")
            return self
        # LOG.debug(f"fetching {self} with {id=}, {newest=}")

        async with SQL() as sql:
            select = sql.select([], True).from_(self.table)
            if id is not None:
                select.where(Eq("id", id))
            elif newest:
                select.where(SQLExpression(f"id = (SELECT MAX(id) FROM {self.table})"))
            self._db_data = await (await select.execute(close=1)).fetchone()

        if self._db_data:
            for attr, typ in [(a[0], a[1]) for a in self.attribute_descriptions()]:
                self._data[attr] = self.convert_from_db(self._db_data.get(attr), typ)
        return self

    async def store(self):
        """Store the business object in the database.
        If 'self.id is None' a new row is inserted
        Else the existing row is updated
        """
        if self.id is None:
            await self._insert_self()
        else:
            await self._update_self()

    async def _insert_self(self):
        assert self.id is None, "id must be None for insert operation"

        async with SQL() as sql:
            self.id = (
                await (
                    await (
                        sql.insert(self.table)
                        .rows(
                            [
                                (k, v)
                                for k, v in self._data.items()
                                if k != "id" and v is not None
                            ]
                        )
                        .returning("id")
                    ).execute(close=1, commit=True)
                ).fetchone()
            ).get("id")

    async def _update_self(self):
        assert self.id is not None, "id must not be None for update operation"
        async with SQL() as sql:
            value_class = Value
            update = sql.update(self.table).where(Eq("id", self.id))
            changes = False
            for k, v in self._data.items():
                if k != "id" and v != self.convert_from_db(
                    self._db_data.get(k), self.attributes_as_dict()[k]
                ):
                    changes = True
                    update.assignment(k, value_class(k, v))
            try:
                if changes:
                    await update.execute(close=0, commit=True)
            finally:
                await self.fetch()


log_exit(LOG)
