""" Base module for Business Object logic

    Business objects are classes that support persistance in the data base
"""

from datetime import date, datetime, UTC

from persistance.bo_descriptors import BO_int, BO_datetime
from core.app import App
from db.sql import SQL
from db.sql_statement import SQL, eq, SQL_expression
from core.app_logging import getLogger

LOG = getLogger(__name__)


class BO_Base:
    id = BO_int(primary_key=True, auto_inc=True)
    last_updated = BO_datetime(current_dt=True)
    _table = None
    _attributes = {}
    _business_objects = {}

    def __init__(self, id=None) -> None:
        self._data = {}
        self._db_data = {}
        self.id = id

    @classmethod
    def register_persistant_class(cls):
        BO_Base._business_objects |= {cls.__name__: cls}
        LOG.debug(f"registered class {cls.__name__}: {BO_Base._business_objects}")

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
        super_cols = {} if cls == BO_Base else cls.__base__.attributes_as_dict()
        return super_cols | {a[0]: a[1] for a in cls._attributes.get(cls.__name__, [])}

    @classmethod
    def attribute_descriptions(cls):
        super_cols = [] if cls == BO_Base else cls.__base__.attribute_descriptions()
        return super_cols + cls._attributes.get(cls.__name__, [])

    @classmethod
    async def sql_create_table(cls):

        sql = SQL().create_table(
            cls.table,
            [
                c for c in cls.attribute_descriptions()
            ]
        ).execute(close = False)

        """ cols = [
            App.db.sql(SQL.CREATE_TABLE_COLUMN, column=c)
            for c in cls.attribute_descriptions()
        ]
        sql = App.db.sql(SQL.CREATE_TABLE, table=cls.table, columns=cols)
        await App.db.execute(query=sql, close=0) """

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
        # LOG.debug(f"{self}.fetch({id=},{newest=})")
        if id is None:
            id = self.id
        if id is None and newest is None:
            LOG.debug(f"fetching {self} without id or newest")
            return self
        LOG.debug(f"fetching {self} with newest={newest}")
        #sql = App.db.sql(SQL.SELECT, table=self.table, id=id, newest=newest)
        
        sql = SQL(App.db).select([], True)
        if self.id is not None:
            sql.From(self.table).Where(eq('id',id))
        elif newest:
            sql.From(self.table).Where(SQL_expression(f"id = (SELECT MAX(id) FROM {self.table})"))
        db_data = await(await sql.execute(close = 1).rslt).fetchone()
        self._db_data = db_data
        #self._db_data = await (await App.db.execute(sql, close=1)).fetchone()
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
        # LOG.debug(f"{self}.store()")
        if self.id is None:
            try:
                values = {
                    k: v for k, v in self._data.items() if v is not None and k != "id"
                }
                if values:
                    query = App.db.sql(
                        SQL.INSERT_ARGS,
                        table=self.table,
                        columns=values,
                        returning=("id"),
                    )
                    LOG.debug(f"{query=}  {values=}")
                    cur = await App.db.execute(
                        query=query,
                        params=values,
                        close=1,
                        commit=True,
                    )
                    returned = await cur.fetchone()
                    self.id = returned.get("id")
            except Exception as err:
                LOG.error(f"Error during INSERT of {self} into DB: {err}")
            else:
                await self.fetch()
        else:
            try:
                self.last_updated = datetime.now(UTC)
                values = {
                    k: v
                    for k, v in self._data.items()
                    if k != "id"
                    and self._data[k]
                    != self.convert_from_db(
                        self._db_data[k], self.attributes_as_dict()[k]
                    )
                }
                query = App.db.sql(SQL.UPDATE_ARGS, table=self.table, columns=values)
                # LOG.debug(f"{query=}  {self._data=}")
                await App.db.execute(
                    query=query,
                    params=self._data,
                    close=0,
                    commit=True,
                )
            finally:
                await self.fetch()


LOG.debug(f"module {__name__} initialized")
