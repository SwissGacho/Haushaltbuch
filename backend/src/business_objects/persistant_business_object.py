"""Base class for persistent Business Objects

Persistent Business Objects are stored in the database and represent the
application's data model."""

import copy
import json
from typing import Optional
from core.app_logging import getLogger
from datetime import date, datetime, UTC

LOG = getLogger(__name__)

from core.util import _classproperty
from database.sql import SQL, SQLTransaction
from database.sql_expression import Eq, Filter, SQLExpression
from database.sql_statement import CreateTable, Value
from business_objects.business_object_base import BOBase


class PersistentBusinessObject(BOBase):
    """Base class for persistent Business Objects.
    Every subclass will be registered in a table in the database."""

    @_classproperty
    def all_business_objects(self) -> dict[str, type["PersistentBusinessObject"]]:
        "Set of registered Business Objects"
        return {
            name: cls
            for name, cls in BOBase.all_business_objects.items()
            if issubclass(cls, PersistentBusinessObject)
        }

    @classmethod
    def convert_from_db(cls, value, typ):
        "convert a value of type 'typ' read from the DB"
        # LOG.debug(f"BOBase.convert_from_db({value=}, {type(value)=}, {typ=})")
        if value is None:
            return None
        if typ == date and isinstance(value, str):
            return date.fromisoformat(value)
        if typ == datetime and isinstance(value, str):
            dt = datetime.fromisoformat(value)
            if dt.tzinfo in [None, UTC]:
                dt = dt.replace(tzinfo=UTC).astimezone(tz=None)
            return dt
        if typ in [dict, list] and isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError as exc:
                LOG.error(f"BOBase.convert_from_db: JSONDecodeError: {exc}")
        return copy.deepcopy(value)

    @classmethod
    async def sql_create_table(cls):
        "Create a DB table for this class"
        attributes = cls.attribute_descriptions()
        async with SQLTransaction() as txaction:
            # create_table: CreateTable = txaction.sql().create_table(cls.table)
            s = txaction.sql()
            create_table: CreateTable = s.create_table(cls.table)
            # LOG.debug(f"BOBase.sql_create_table():  {cls.table=}")
            for name, data_type, constraint, pars in attributes:
                # LOG.debug(f" -  {name=}, {data_type=}, {constraint=}, {pars=})")
                create_table.column(name, data_type, constraint, **pars)
            await create_table.execute()

    @classmethod
    async def count_rows(cls, conditions: Optional[dict] = None) -> int:
        """Count the number of existing business objects in the DB table matching the conditions"""
        async with SQL() as sql:
            select = sql.select(["count(*) as count"]).from_(cls.table)
            if conditions:
                select.where(Filter(conditions))
            result = await (await select.execute()).fetchone()
        # LOG.debug(f"BOBase.count_rows({conditions=}) {result=} -> return {result["count"]}")
        return result["count"]

    @classmethod
    async def get_matching_ids(cls, conditions: dict | None = None) -> list[int]:
        """Get the ids of business objects matching the conditions"""
        async with SQL() as sql:
            select = sql.select(["id"]).from_(cls.table)
            select.where(Filter(conditions))
            result = await (await select.execute()).fetchall()
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
            self._db_data = await (await select.execute()).fetchone()

        if self._db_data:
            # LOG.debug(f"BOBase.fetch: {self._db_data=}")
            for attr, typ in [(a[0], a[1]) for a in self.attribute_descriptions()]:
                self._data[attr] = PersistentBusinessObject.convert_from_db(
                    self._db_data.get(attr), typ
                )
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

        async with SQLTransaction() as txaction:
            self.id = (
                await (
                    await (
                        txaction.sql()
                        .insert(self.table)
                        .rows(
                            [
                                (k, v)
                                for k, v in self._data.items()
                                if k != "id" and v is not None
                            ]
                        )
                        .returning("id")
                    ).execute()
                ).fetchone()
            ).get("id")

    async def _update_self(self):
        assert self.id is not None, "id must not be None for update operation"
        async with SQLTransaction() as txaction:
            value_class = Value
            update = txaction.sql().update(self.table).where(Eq("id", self.id))
            changes = False
            for k, v in self._data.items():
                if k != "id" and v != PersistentBusinessObject.convert_from_db(
                    self._db_data.get(k), self.attributes_as_dict()[k]
                ):
                    changes = True
                    update.assignment(k, value_class(k, v))
            try:
                if changes:
                    await update.execute()
            finally:
                await self.fetch()
