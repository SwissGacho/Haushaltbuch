"""This module defines a SQLExecutable class that is used to create and execute SQL statements."""

from abc import ABC, abstractmethod

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from core.app import App
from core.base_objects import DBBaseClass
from database.sql_factory import SQLFactory
from database.sql_key_manager import SQLKeyManager, SQL_Dict


class SQLExecutable(object):
    """Base class for SQL operations. Should not be instantiated directly."""

    def __init__(self, parent: "SQLExecutable | None" = None):
        super().__init__()
        self._parent: "SQLExecutable | None" = parent

    def __new__(cls, *args, **kwargs):
        # LOG.debug(f"SQLExecutable.__new__({cls=}, {args=}, {kwargs=})")
        future_parent = kwargs.get("parent", None)
        if future_parent is None or not isinstance(future_parent, SQLExecutable):
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
        if self._parent is None:
            raise ReferenceError("No parent defined for SQL execution.")
        return await self._parent.execute()

    async def close(self):
        """Close the database connection."""
        if self._parent is None:
            raise ReferenceError("No parent defined to close.")
        return await self._parent.close()

    @classmethod
    def _get_db(cls) -> DBBaseClass:
        """Get the current database connection."""
        return App.db

    @property
    def sql_factory(self) -> SQLFactory:
        """Get the SQLFactory of the current database. Usually call get_sql_class instead."""
        if self._parent is None:
            raise ReferenceError("No parent defined to get SQL factory.")
        return self._parent.sql_factory


class SQLManagedExecutable(SQLExecutable, SQLKeyManager, ABC):
    """Base class for SQL operations that are managed by a parent object.
    Should not be instantiated directly."""

    @abstractmethod
    def get_query(self) -> str:
        """Get the SQL statement as a string."""
        raise NotImplementedError("Subclasses must implement this method.")

    def get_sql(self) -> SQL_Dict:
        """Get the SQL statement as a dictionary."""
        return {"query": self.get_query(), "params": self.params}


log_exit(LOG)
