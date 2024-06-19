""" Base module for Business Object logic

    Business objects are classes that support persistance in the data base
"""

from datetime import date, datetime, UTC

from persistance.bo_descriptors import BOInt, BODatetime
from core.app import App
from db.sqlexecutable import SQL, CreateTable
from db.sqlexpression import Eq, SQLExpression, Value
from core.app_logging import getLogger

LOG = getLogger(__name__)


class BOBase:
    id = BOInt(primary_key=True, auto_inc=True)
    last_updated = BODatetime(current_dt=True)
    _table = None
    _attributes = {}
    _business_objects = {}

    def __init__(self, id=None) -> None:
        self._data = {}
        self._db_data = {}
        self.id = id

    @classmethod
    def register_persistant_class(cls):
        BOBase._business_objects |= {cls.__name__: cls}
        LOG.debug(f"registered class {cls.__name__}: {BOBase._business_objects}")

    @classmethod
    @property
    def all_business_objects(cls):
        return cls._business_objects

    @classmethod
    @property
    def table(cls):
        return cls._table if cls._table else cls.__name__.lower() + "s"

    @classmethod
    def attributes_as_dict(cls):
        super_cols = {} if cls == BOBase else cls.__base__.attributes_as_dict()
        return super_cols | {a[0]: a[1] for a in cls._attributes.get(cls.__name__, [])}

    @classmethod
    def attribute_descriptions(cls):
        super_cols = [] if cls == BOBase else cls.__base__.attribute_descriptions()
        return super_cols + cls._attributes.get(cls.__name__, [])

    @classmethod
    async def sql_create_table(cls):

        attributes = cls.attribute_descriptions()
        sql: CreateTable = SQL().create_table(cls.table)
        for attr in attributes:
            sql.column(attr[0], attr[1], attr[2])
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

    async def fetch(self, id=None, newest=None):
        """Fetch the content for a business object instance from the DB.
        If 'id' is given, fetch the identified object
        If 'id' omitted and 'newest'=True fetch the object with highest id
        If the oject is not found in the DB return the instance unchanged
        """
        if id is None:
            id = self.id
        if id is None and newest is None:
            LOG.debug(f"fetching {self} without id or newest")
            return self
        LOG.debug(f"fetching {self} with newest={newest}")

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
        for k, v in self._data.items():
            if k != "id" and v != self.convert_from_db(
                self._db_data[k], self.attributes_as_dict()[k]
            ):
                sql.assignment(k, value_class(v))
        try:
            await sql.execute(close=0, commit=True)
        finally:
            await self.fetch()


LOG.debug(f"module {__name__} initialized")
