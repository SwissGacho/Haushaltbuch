"""This module defines a SQLExecutable class that is used to create and execute SQL statements."""

from typing import Optional, Self

from core.app import App
from core.base_objects import DBBaseClass
from database.sql_factory import SQLFactory
from database.sql_key_manager import SQLKeyManager, SQL_Dict
from core.app_logging import getLogger

LOG = getLogger(__name__)


class SQLExecutable(object):
    """Base class for SQL operations. Should not be instantiated directly."""

    def __init__(self, parent: Self | None = None):
        super().__init__()
        self._parent: Self | None = parent

    def __new__(cls, *args, **kwargs):
        # LOG.debug(f"SQLExecutable.__new__({cls=}, {args=}, {kwargs=})")
        future_parent = kwargs.get("parent", None)
        if future_parent is None or not (isinstance(future_parent, SQLExecutable)):
            raise TypeError(
                f"Expected 'SQLExecutable' as parent, got {type(future_parent).__name__}"
            )
        actual_class = future_parent._get_db().sql_factory.get_sql_class(cls)

        if not issubclass(actual_class, SQLExecutable):
            raise TypeError(f"Factory returned an invalid class: {actual_class}")
        actual_object = super().__new__(actual_class)  # type: ignore
        return actual_object

    async def execute(self):
        """Execute the current SQL statement on the database."""
        # LOG.debug(f"SQLExecutable.execute()")
        return await self._parent.execute()

    async def close(self):
        """Close the database connection."""
        return await self._parent.close()

    @classmethod
    def _get_db(cls) -> DBBaseClass:
        """Get the current database connection."""
        return App.db

    @property
    def sql_factory(self) -> SQLFactory:
        """Get the SQLFactory of the current database. Usually call get_sql_class instead."""
        return self._parent.sql_factory


class SQLManagedExecutable(SQLExecutable, SQLKeyManager):
    """Base class for SQL operations that are managed by a parent object.
    Should not be instantiated directly."""

    def get_query(self) -> str:
        """Get the SQL statement as a string."""
        raise NotImplementedError("Subclasses must implement this method.")

    def get_sql(self) -> SQL_Dict:
        """Get the SQL statement as a dictionary."""
        return {"query": self.get_query(), "params": self.params}
