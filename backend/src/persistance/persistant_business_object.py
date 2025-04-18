"""Base class for persistent Business Objects

Persistent Business Objects are stored in the database and represent the
application's data model."""

from typing import Optional
from core.app_logging import getLogger

LOG = getLogger(__name__)

from database.sql_executable import SQLExecutable
from database.sql_expression import Eq, Filter, SQLExpression
from database.sql_statement import CreateTable, SQL, Value
from persistance.business_object_base import BOBase


class PersistentBusinessObject(BOBase):
    """Base class for persistent Business Objects.
    Every subclass will be registered in a table in the database."""

    @classmethod
    async def sql_create_table(cls):
        "Create a DB table for this class"
        attributes = cls.attribute_descriptions()
        sql: CreateTable = SQL().create_table(cls.table)
        LOG.debug(f"BOBase.sql_create_table():  {cls.table=}")
        for name, data_type, constraint, pars in attributes:
            LOG.debug(f" -  {name=}, {data_type=}, {constraint=}, {pars=})")
            sql.column(name, data_type, constraint, **pars)
        await sql.execute(close=0)

    @classmethod
    async def count_rows(cls, conditions: Optional[dict] = None) -> int:
        """Count the number of existing business objects in the DB table matching the conditions"""
        sql = SQL().select(["count(*) as count"]).from_(cls.table)
        if conditions:
            sql.where(Filter(conditions))
        result = await (await sql.execute(close=1)).fetchone()
        # LOG.debug(f"BOBase.count_rows({conditions=}) {result=} -> return {result["count"]}")
        return result["count"]

    @classmethod
    async def get_matching_ids(cls, conditions: dict | None = None) -> list[int]:
        """Get the ids of business objects matching the conditions"""
        sql = SQL().select(["id"]).from_(cls.table)
        if conditions:
            sql.where(Filter(conditions))
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
        # LOG.debug(f"fetching {self} with {id=}, {newest=}")

        sql = SQL().select([], True).from_(self.table)
        if id is not None:
            sql.where(Eq("id", id))
        elif newest:
            sql.where(SQLExpression(f"id = (SELECT MAX(id) FROM {self.table})"))
        self._db_data = await (await sql.execute(close=1)).fetchone()

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
        sql: SQLExecutable = SQL().update(self.table).where(Eq("id", self.id))
        changes = False
        for k, v in self._data.items():
            if k != "id" and v != self.convert_from_db(
                self._db_data.get(k), self.attributes_as_dict()[k]
            ):
                changes = True
                sql.assignment(k, Value(k, v))
        try:
            if changes:
                await sql.execute(close=0, commit=True)
        finally:
            await self.fetch()
