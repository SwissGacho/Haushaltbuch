"""SQL statement builder for the database.
This module provides functionality to create, execute, and manage SQL statements."""

from typing import Self, Optional

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from core.app import App
from core.exceptions import InvalidSQLStatementException, OperationalError, CommitError
from core.base_objects import ConnectionBaseClass
from database.sql_executable import SQLExecutable
from database.sql_factory import SQLFactory
from database.sql_key_manager import SQL_Dict
from database.sql_statement import (
    NamedValueListList,
    CreateTable,
    CreateView,
    Insert,
    SQLScript,
    SQLStatement,
    SQLTemplate,
    Select,
    Update,
)
from business_objects.bo_descriptors import BOColumnFlag


class _SQLBase(SQLExecutable):
    """Base class for SQL operations that need a DB connection.
    A connection can be provided on instantiation.
    .connect will create a new connection if not provided.
    .close will close the connection if it was created by this class.
    Context manager support is provided creating a connection when necessary.

    Should not be instantiated directly."""

    def __new__(cls, *args, **kwargs):
        factory = cls._get_db().sql_factory  # pylint: disable=no-member
        actual_class = factory.get_sql_class(cls)
        return object.__new__(actual_class)

    def __init__(self, connection: ConnectionBaseClass | None = None) -> None:
        super().__init__(None)
        self._connection = connection
        self._my_connection = connection

    async def __aenter__(self) -> Self:
        """Enter the runtime context related to this object."""
        # LOG.debug(f"Entering {self.__class__.__name__} context")
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Exit the runtime context related to this object."""
        # LOG.debug(f"Exiting {self.__class__.__name__} context")
        await self.close(rollback=exc_type is not None)
        return False

    async def connect(self) -> ConnectionBaseClass:
        """Create a new connection to the database."""
        if self._my_connection is None:
            # pylint: disable=no-member
            self._my_connection = await SQLExecutable._get_db().connect()
            # LOG.debug("New connection created")
        return self._my_connection

    @property
    def connection(self) -> ConnectionBaseClass | None:
        """Get the currently used connection to the database."""
        return self._my_connection

    async def close(self, rollback: bool = False):
        "Close the connection to the database."
        _ = rollback  # not used here
        if self._my_connection is not None and self._connection is None:
            await self._my_connection.close()
            self._my_connection = None
            # LOG.debug("Connection closed")

    async def commit(self):
        "commit current transaction"
        # LOG.debug("commit connection")
        if self._my_connection is not None:
            await self._my_connection.commit()

    async def rollback(self):
        "rollback current transaction"
        # LOG.debug("rollback connection")
        if self._my_connection is not None:
            await self._my_connection.rollback()


class SQL(_SQLBase):
    """Usage:
    sql = SQL_statement().create_table(
        "users",
        [
            ("name", SQL_data_type.TEXT),
            ("age",  SQL_data_type.INTEGER)
        ],
    ).execute()"""

    def __init__(
        self, connection: ConnectionBaseClass | None = None, auto_commit: bool = True
    ) -> None:
        # LOG.debug(f"SQL.__init__({connection=})")
        super().__init__(connection=connection)
        self._sql_statement: SQLStatement | None = None
        self._auto_commit: bool = auto_commit

    def __repr__(self):
        return f"SQL({repr(self._sql_statement)})"

    def __str__(self):
        return f"SQL({str(self._sql_statement)})"

    async def close(self, rollback: bool = False):
        if self._auto_commit:
            if rollback:
                # LOG.debug("Rolling back connection due to auto-commit")
                await self.rollback()
            else:
                # LOG.debug("auto-committing connection")
                await self.commit()
        await super().close()

    def get_sql(self) -> SQL_Dict:
        """Get a string representation of the current SQL statement."""
        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return self._sql_statement.get_sql()

    @property
    def sql_factory(self) -> SQLFactory:
        """Get the SQLFactory of the current database. Usually call get_sql_class instead."""
        return SQL._get_db().sql_factory  # pylint: disable=no-member

    def create_table(
        self,
        table: str,
        columns: Optional[list[tuple[str, type, BOColumnFlag | None, dict]]] = None,
        temporary: bool = False,
    ) -> "CreateTable":
        """Sets the SQL statement to create a table and returns a create_table object"""
        # LOG.debug(f"SQL.create_table({table=}, {columns=}, {temporary=})")
        create_table = CreateTable(
            table=table, columns=columns or [], temporary=temporary, parent=self
        )
        self._sql_statement = create_table
        return create_table

    def create_view(
        self,
        view_name: str,
        view_columns: list[str] | None = None,
        column_list: list[str] | None = None,
        distinct: bool = False,
        temporary: bool = False,
    ) -> "CreateView":
        """Sets the SQL statement to a create view statement and returns a create_view object
        view_columns: list of columns of the view
        column_list: list of columns to be selected by the view (see .select())"""
        create_view = CreateView(
            view=view_name,
            view_columns=view_columns,
            column_list=column_list,
            distinct=distinct,
            temporary=temporary,
            parent=self,
        )
        self._sql_statement = create_view
        return create_view

    def select(
        self, column_list: Optional[list[str]] = None, distinct: bool = False
    ) -> "Select":
        """Sets the SQL statement to a select statement and returns a select object"""
        select = Select(column_list, distinct, parent=self)
        self._sql_statement = select
        return select

    def insert(
        self, table: str, columns: Optional[NamedValueListList] = None
    ) -> "Insert":
        """Sets the SQL statement to a insert statement and returns an insert object"""
        insert = Insert(table, columns, parent=self)
        self._sql_statement = insert
        return insert

    def update(self, table: str) -> "Update":
        """Sets the SQL statement to a update statement and returns an update object"""
        update = Update(table, parent=self)
        self._sql_statement = update
        return update

    def script(self, script_or_template: str | SQLTemplate, **kwargs) -> "SQLScript":
        """Set the SQL statement to execute the specific script or
        use a template to build an SQLScript
        """
        if "parent" in kwargs:
            raise ValueError(
                f"'parent={kwargs['parent']}' must not be a template argument"
            )
        self._sql_statement = SQLScript(script_or_template, parent=self, **kwargs)
        return self._sql_statement

    async def execute(self):
        """Execute the current SQL statement on the database.
        Must create the statement before calling this method"""

        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        # LOG.debug(
        #     f"SQL.execute(): {self.get_sql()=}; {self._my_connection=}"
        # )
        sql = self.get_sql()
        # pylint: disable=no-member
        return await App.db.execute(
            query=sql["query"], params=sql["params"], connection=await self.connect()
        )


class SQLTransaction(_SQLBase):
    """SQL transaction context manager."""

    async def __aenter__(self) -> Self:
        """Enter the SQL transaction context."""
        # LOG.debug("Entering SQL transaction context")
        await self.connect()
        if self._my_connection is None:
            raise ConnectionError("No connection available for transaction.")
        await self._my_connection.begin()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        # LOG.debug("Exiting SQL transaction context")
        if exc_type is None:
            try:
                await self.commit()
                # LOG.debug("Transaction committed successfully.")
            except OperationalError as exc:
                await self.rollback()
                await self.close()
                raise CommitError(exc) from exc
        else:
            await self.rollback()
        await self.close()
        return False

    def sql(self) -> SQL:
        "Create a new SQL statement object"
        return SQL(connection=self.connection, auto_commit=False)


class SQLConnection(_SQLBase):
    """SQL connection context manager."""

    def __init__(self) -> None:
        super().__init__(None)

    def sql(self) -> SQL:
        "Create a new SQL statement object"
        return SQL(connection=self.connection)

    def transaction(self) -> SQLTransaction:
        "Create a new SQL transaction object"
        return SQLTransaction(connection=self.connection)


log_exit(LOG)
