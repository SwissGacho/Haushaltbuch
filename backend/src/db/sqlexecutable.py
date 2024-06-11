"""This module defines a SQLExecutable class that is used to create and execute SQL statements."""

from enum import Enum, auto
from typing import List

from core.app import App
from .sqlexpression import (
    SQLExpression,
    SQLColumnDefinition,
    Value,
    Row,
    Values,
    Assignment,
    Where,
    GroupBy,
    Having,
)
from .sqlexpression import From
from .sqlfactory import SQLFactory
from .sqlkeymanager import SQLKeyManager


class InvalidSQLStatementException(Exception):
    """
    Exception raised when an invalid SQL statement is encountered.
    """


class SQLDataType(Enum):
    """Basic SQL data types compliant with SQLite. DB implementations should override this enum."""

    TEXT = auto()
    INTEGER = auto()
    REAL = auto()
    BLOB = auto()


class SQLExecutable(object):
    """Base class for SQL operations. Should not be instantiated directly."""

    def __init__(self, parent: "SQLExecutable" = None):
        self._parent = parent
        self._parameters = {}
        self._children: List[SQLExecutable] = []

    async def execute(
        self,
        params: dict[str, str] = None,
        close=False,
        commit=False,
    ):
        """Execute the current SQL statement on the database."""
        return await self._parent.execute(params=params, close=close, commit=commit)

    async def close(self):
        """Close the database connection."""
        return await self._parent.close()

    def get_sql_class(self, sql_cls: type) -> type:
        """Get the speficied SQL class definition as defined by the db's SQLFactory."""
        return self.sql_factory.get_sql_class(sql_cls)

    @property
    def sql_factory(self) -> SQLFactory:
        """Get the SQLFactory of the current database. Usually call get_sql_class instead."""
        return self._parent.sql_factory


class SQL(SQLExecutable):
    """Usage:
    sql = SQL_statement().create_table(
        "users",
        [
            ("name", SQL_data_type.TEXT),
            ("age",  SQL_data_type.INTEGER)
        ],
    ).execute()"""

    def __init__(self):
        super().__init__(None)
        self._rslt = None
        self._sql_statement: "SQLStatement" = None

    @classmethod
    def _get_db(cls):
        """Get the current database connection."""
        return App.db

    def get_sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return self._sql_statement.get_sql()

    def get_params(self) -> dict[str, str]:
        """Get the parameters for the current SQL statement."""
        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return self._sql_statement.get_params()

    @property
    def sql_factory(self) -> SQLFactory:
        """Get the SQLFactory of the current database. Usually call get_sql_class instead."""
        return self._get_db().sqlFactory

    def create_table(
        self, table: str, columns: list[(str, SQLDataType)] = None
    ) -> "CreateTable":
        """Sets the SQL statement to create a table and returns a create_table object"""
        create_table = self.get_sql_class(CreateTable)(table, columns, self)
        # create_table = Create_Table(table, columns, self)
        self._sql_statement = create_table
        return create_table

    def select(self, column_list: list[str] = None, distinct: bool = False) -> "Select":
        """Sets the SQL statement to a select statement and returns a select object"""
        select = self.get_sql_class(Select)(column_list, distinct, self)
        self._sql_statement = select
        return select

    def insert(self, table: str, columns: list[str] = None) -> "Insert":
        """Sets the SQL statement to a insert statement and returns an insert object"""
        insert = self.get_sql_class(Insert)(table, columns, parent=self)
        self._sql_statement = insert
        return insert

    def update(self, table: str) -> "Update":
        """Sets the SQL statement to a update statement and returns an update object"""
        update = self.get_sql_class(Update)(table, parent=self)
        self._sql_statement = update
        return update

    def script(self, script: str) -> "SQLScript":
        """Set the SQL statement to execute the specific script supplied"""
        self._sql_statement = self.get_sql_class(SQLScript)(script, self)
        return self._sql_statement

    async def execute(self, params=None, close=False, commit=False):
        """Execute the current SQL statement on the database.
        Must create the statement before calling this method"""
        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return await self._get_db().execute(self.get_sql(), params, close, commit)

    async def close(self):
        await self._get_db().close()


class SQLStatement(SQLExecutable, SQLKeyManager):
    """Base class for SQL statements. Should not be instantiated directly."""

    def __init__(self, parent: SQLExecutable = None):
        super().__init__(parent)
        self._params = {}

    def get_params(self) -> dict[str, str]:
        """Get the parameters for the current SQL statement."""
        return self._params

    def get_sql(self) -> str:
        """Get the SQL statement."""
        raise NotImplementedError(
            "SQL_statement is an abstract class and should not be instantiated."
        )

    def sql(self) -> tuple[str, dict[str, str]]:
        """Get a string representation of the current SQL statement.
        Must be implemented by subclasses."""
        return self.get_sql(), self.get_params()


class SQLScript(SQLStatement):
    """A SQL statement that executes a script verbatim"""

    def __init__(self, script: str, parent: SQLExecutable = None):
        super().__init__(parent)
        self._script = script

    def get_sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        return self._script


class CreateTable(SQLStatement):
    """A SQLStatement representing a CREATE TABLE statement.
    Default implementation complies with SQLite syntax."""

    def __init__(
        self,
        table: str = "",
        columns: list[(str, SQLDataType, str)] = None,
        parent: SQLExecutable = None,
    ):
        super().__init__(parent)
        cols = [] if columns is None else columns
        self._table = table
        sql_column_definition = self.sql_factory.get_sql_class(SQLColumnDefinition)
        self._columns: list[SQLColumnDefinition] = [
            sql_column_definition(name, data_type, constraint, self)
            for name, data_type, constraint in cols
        ]

    def column(self, name: str, data_type: SQLDataType, constraint: str = None):
        """Add a column to the table to be created.
        The column will be added to the end of the column list."""
        self._columns.append(
            self.sql_factory.get_sql_class(SQLColumnDefinition)(
                name, data_type, constraint, self
            )
        )
        return self

    def get_params(self):
        value_dict = {}
        for column in self._columns:
            value_dict.update(column.get_params())
        return value_dict

    def get_sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        if self._table is None or len(self._table) == 0:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement must have a table name."
            )
        return f"CREATE TABLE {self._table} ({[column.get_sql() for column in self._columns]})"


class TableValuedQuery(SQLStatement):
    """SQLStatement representing a statement that has a table as its result set.
    Should not be instantiated directly."""

    def __init__(self, parent: SQLExecutable):
        super().__init__(parent)

    def get_sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        raise NotImplementedError(
            "Table_Valued_Query is an abstract class and should not be instantiated."
        )


class Select(TableValuedQuery):
    """Represents a SQL Select Statement. Default implementation complies with SQLite syntax."""

    def __init__(
        self,
        column_list: list[str] = None,
        distinct: bool = False,
        parent: SQLExecutable = None,
    ):
        super().__init__(parent)
        self._column_list = [] if column_list is None else column_list
        self._distinct = distinct
        self._from_statement: TableValuedQuery = None
        self._where: Where = None
        self._group_by: GroupBy = None
        self._having: Having = None

    def get_params(self):
        value_dict = {}
        if self._from_statement is not None:
            value_dict.update(self._from_statement.get_params())
        if self._where is not None:
            value_dict.update(self._where.get_params())
        if self._group_by is not None:
            value_dict.update(self._group_by.get_params())
        if self._having is not None:
            value_dict.update(self._having.get_params())
        return value_dict

    def get_sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        if self._from_statement is None:
            raise InvalidSQLStatementException(
                "SELECT statement must have a FROM clause."
            )
        sql = f"SELECT {'DISTINCT ' if self._distinct else ''}{', '.join(self._column_list)}"
        if self._column_list is None or len(self._column_list) == 0:
            sql += "*"
        sql += self._from_statement.sql()
        if self._where is not None:
            sql += self._where.sql()
        if self._group_by is not None:
            sql += self._group_by.sql()
        if self._having is not None:
            sql += self._having.sql()
        return sql

    def distinct(self):
        """Sets the distinct flag for the select statement.
        If not called select will not be distinct."""
        self._distinct = True
        return self

    def all(self):
        """Removes the distinct flag for the select statement."""
        self._distinct = False
        return self

    def columns(self, column_list: list[str]):
        """Sets the columns for the select statement.
        Default is ['*']. Any existing list is discarded."""
        self._column_list = column_list
        return self

    def from_(self, table: str | TableValuedQuery):
        """Sets the from clause for the select statement.
        The statement will not execute without a from clause."""
        from_table = self.sql_factory.get_sql_class(From)(table, self)
        self._from_statement = from_table
        return self

    def where(self, condition: SQLExpression):
        """Sets the where clause for the select statement. Optional."""
        where = self.sql_factory.get_sql_class(Where)(condition, self)
        self._where = where
        return self

    def having(self, condition: SQLExpression):
        """Sets the having clause for the select statement. Optional."""
        having = self.sql_factory.get_sql_class(Having)(condition, self)
        self._having = having
        return self


class Insert(SQLStatement):
    """A SQLStatement representing an INSERT statement.
    Default implementation complies with SQLite syntax."""

    def __init__(
        self,
        table: str,
        columns: list[str] = None,
        rows: Values = None,
        parent: SQLExecutable = None,
    ):
        super().__init__(parent)
        self._columns = [] if columns is None else columns
        self._table = table
        self._columns = columns
        self._rows = rows
        self._return_str: str = ""

    def get_params(self):
        return self._rows.get_params()

    def get_sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        sql = f"INSERT INTO {self._table} ({', '.join(self._columns)}) {self._rows.get_sql()}"
        return sql + self._return_str

    def single_row(self, row: list[(str, str | Value)]):
        """
        Set the statement to insert a single row of data defined by the supplied parameters.

        Args:
            row (list[(str, str | Value)]):
                A list of tuples, representing the fields in the row and their values.
        """
        row_obj: Row = self.get_sql_class(Row)()
        for column_name, value in row:
            self.column(column_name)
            row_obj.value(value)
        self._rows: Values = self.get_sql_class(Values)([row_obj], self)
        return self

    def column(self, column: str):
        """Add a column to the end of the list of columns to be inserted."""
        self._columns.append(column)
        return self

    def values(self, values: Values):
        """Set the values to be inserted."""
        self._rows = values
        return self

    def returning(self, column: str):
        """Set the column to be returned after the insert statement is executed."""
        self._return_str = f" RETURNING {column}"
        return self


class Update(SQLStatement):
    """A SQLStatement representing an UPDATE statement.
    Default implementation complies with SQLite syntax."""

    def __init__(self, table: str, parent: SQLExecutable = None):
        super().__init__(parent)
        self._table = table
        self._where: Where = None
        self.assignments: List[Assignment] = []

    def assignment(self, columns: list[str] | str, value: Value):
        """Add an assignment to the list of assignments to be made in the update statement.

        Args:
            columns (list[str] | str):
                A list of column names to be assigned the value.
            value (Value):
                The value to be assigned to the column(s)."""
        self.assignments.append(
            self.sql_factory.get_sql_class(Assignment)(columns, value, self)
        )
        return self

    def where(self, condition: SQLExpression):
        """Set the where clause for the update statement."""
        where: Where = self.sql_factory.get_sql_class(Where)(condition, self)
        self._where = where
        return self

    def returning(self, column: str):
        """Set the column to be returned after the update statement is executed."""
        self.sql += f" RETURNING {column}"
        return self

    def get_params(self):
        value_dict = {}
        for assignment in self.assignments:
            value_dict.update(assignment.get_params())
        if self._where is not None:
            value_dict.update(self._where.get_params())
        return value_dict

    def get_sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        sql = f"""UPDATE {self._table}
            SET {', '.join([assignment.get_sql() for assignment in self.assignments])}"""

        return sql