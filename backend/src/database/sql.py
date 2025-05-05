"""SQL statement builder for the database.
This module provides functionality to create, execute, and manage SQL statements."""

from multiprocessing import connection
from core.exceptions import InvalidSQLStatementException

from core.base_objects import ConnectionBaseClass
from database.sql_executable import SQLExecutable
from database.sql_factory import SQLFactory
from database.sql_key_manager import SQL_Dict
from database.sql_statement import (
    NamedValueListList,
    CreateTable,
    Insert,
    SQLScript,
    SQLStatement,
    SQLTemplate,
    Select,
    Update,
)
from persistance.bo_descriptors import BOColumnFlag
from core.app_logging import getLogger

LOG = getLogger(__name__)


class SQL(SQLExecutable):
    """Usage:
    sql = SQL_statement().create_table(
        "users",
        [
            ("name", SQL_data_type.TEXT),
            ("age",  SQL_data_type.INTEGER)
        ],
    ).execute()"""

    def __new__(cls, *args, **kwargs):
        factory = cls._get_db().sql_factory
        actual_class = factory.get_sql_class(cls)
        return object().__new__(actual_class)

    def __init__(
        self,
        connection: ConnectionBaseClass | None = None,
    ) -> None:
        super().__init__(None)
        self._connection = connection
        self._sql_statement: SQLStatement | None = None

    def __repr__(self):
        return f"SQL({self._sql_statement})"

    def __str__(self):
        return f"SQL({self._sql_statement})"

    def get_sql(self) -> SQL_Dict:
        """Get a string representation of the current SQL statement."""
        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return self._sql_statement.get_sql()

    @property
    def sql_factory(self) -> SQLFactory:
        """Get the SQLFactory of the current database. Usually call get_sql_class instead."""
        return SQL._get_db().sql_factory

    def create_table(
        self,
        table: str,
        columns: list[tuple[str, type, BOColumnFlag | None, dict]] = [],
        temporary: bool = False,
    ) -> "CreateTable":
        """Sets the SQL statement to create a table and returns a create_table object"""
        # LOG.debug(f"SQL.create_table({table=}, {columns=}, {temporary=})")
        create_table = CreateTable(table, columns, temporary=temporary, parent=self)
        self._sql_statement = create_table
        return create_table

    def select(self, column_list: list[str] = None, distinct: bool = False) -> "Select":
        """Sets the SQL statement to a select statement and returns a select object"""
        select = Select(column_list, distinct, parent=self)
        self._sql_statement = select
        return select

    def insert(self, table: str, columns: NamedValueListList = None) -> "Insert":
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

    async def execute(self, close=False, commit=False):
        """Execute the current SQL statement on the database.
        Must create the statement before calling this method"""

        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        # LOG.debug(f"SQL.execute({close=}, {commit=}): {self.get_sql()=}")
        sql = self.get_sql()
        return await SQL._get_db().execute(
            sql["query"], sql["params"], close, commit, connection=self._connection
        )

    async def close(self):
        await SQL._get_db().close()
