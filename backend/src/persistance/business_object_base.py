""" Base module for Business Object logic

    Business objects are classes that support persistance in the data base
"""

from typing import Self, TypeAlias
from datetime import date, datetime, UTC

from persistance.bo_descriptors import BOColumnFlag, BOBaseBase, BOInt, BODatetime
from database.sqlexecutable import SQL, CreateTable
from database.sqlexpression import Eq, Filter, SQLExpression, Value
from core.app_logging import getLogger

LOG = getLogger(__name__)


class _classproperty(property):
    def __get__(self, owner_self, owner_cls):
        return self.fget(owner_cls)


AttributeDescription: TypeAlias = tuple[str, str, str | None]


class BOBase(BOBaseBase):
    "Business Object baseclass"
    id = BOInt(BOColumnFlag.BOC_PK_INC)
    last_updated = BODatetime(BOColumnFlag.BOC_DEFAULT_CURR)
    _table = None
    _attributes: dict[str, list[AttributeDescription]] = {}
    _business_objects: dict[str, Self] = {}

    def __init__(self, id=None) -> None:
        self._data = {}
        self._db_data = {}
        self.id = id

    @classmethod
    def register_persistant_class(cls):
        "Register the Business Object."
        BOBase._business_objects |= {cls.__name__: cls}
        LOG.debug(f"registered class '{cls.__name__}'")

    @_classproperty
    def all_business_objects(self) -> dict[str, Self]:
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
        super_cols = (
            {}
            if cls == BOBase
            else cls.__base__.attributes_as_dict()  # pylint: disable=no-member
        )
        return super_cols | {a[0]: a[1] for a in cls._attributes.get(cls.__name__, [])}

    @classmethod
    def attribute_descriptions(cls) -> list[tuple[str]]:
        "list of attribute descriptions"
        super_cols = (
            []
            if cls == BOBase
            else cls.__base__.attribute_descriptions()  # pylint: disable=no-member
        )
        return super_cols + cls._attributes.get(cls.__name__, [])

    @classmethod
    async def sql_create_table(cls):
        "Create a DB table for this class"
        attributes = cls.attribute_descriptions()
        sql: CreateTable = SQL().create_table(cls.table)
        # LOG.debug(f"BOBase.sql_create_table():  {cls.table=}")
        for name, data_type, constraint, pars in attributes:
            # LOG.debug(f" -  {name=}, {data_type=}, {constraint=}, {pars=})")
            sql.column(name, data_type, constraint, **pars)
        await sql.execute(close=0)

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
        return value

    @classmethod
    async def count_rows(cls, conditions: dict = None) -> int:
        """Count the number of existing business objects in the DB table matching the conditions"""
        sql = SQL().select(["count(*) as count"]).from_(cls.table)
        if conditions:
            sql.where(sql.get_sql_class(Filter)(conditions))
        result = await (await sql.execute(close=1)).fetchone()
        # LOG.debug(f"BOBase.count_rows({conditions=}) {result=} -> return {result["count"]}")
        return result["count"]

    @classmethod
    async def get_matching_ids(cls, conditions: dict) -> list[int]:
        """Get the ids of business objects matching the conditions"""
        sql = SQL().select(["id"]).from_(cls.table)
        sql.where(sql.get_sql_class(Filter)(conditions))
        result = await (await sql.execute(close=1)).fetchall()
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
        # LOG.debug(f"fetching {self} with newest={newest}")

        sql = SQL().select([], True).from_(self.table)
        if self.id is not None:
            sql.where(sql.get_sql_class(Eq)("id", id))
        elif newest:
            sql.where(
                sql.get_sql_class(SQLExpression)(
                    f"id = (SELECT MAX(id) FROM {self.table})"
                )
            )
        self._db_data = await (await sql.execute(close=1)).fetchone()

        if self._db_data:
            for attr, typ in [(a[0], a[1]) for a in self.attribute_descriptions()]:
                if attr == "u1.last_updated":
                    LOG.debug(f"fetched u1.last_updated: {self._db_data.get(attr)}")
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

        self.id = (
            await (
                await (
                    SQL()
                    .insert(self.table)
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
        sql = SQL()
        value_class = sql.get_sql_class(Value)
        sql = sql.update(self.table).where(Eq("id", self.id))
        changes = False
        for k, v in self._data.items():
            if k != "id" and v != self.convert_from_db(
                self._db_data[k], self.attributes_as_dict()[k]
            ):
                changes = True
                sql.assignment(k, value_class(k, v))
        try:
            if changes:
                await sql.execute(close=0, commit=True)
        finally:
            await self.fetch()


LOG.debug(f"module {__name__} initialized")
