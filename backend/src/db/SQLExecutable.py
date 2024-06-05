from enum import Enum, auto
from core.app import App
from typing import List
from .SQLExpression import (
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
from .SQLExpression import From
from .SQLFactory import SQLFactory
from .db_base import DB


class InvalidSQLStatementException(Exception):
    """
    Exception raised when an invalid SQL statement is encountered.
    """


class SQLDataType(Enum):
    TEXT = auto()
    INTEGER = auto()
    REAL = auto()
    BLOB = auto()


class SQLExecutable(object):
    """Base class for SQL operations. Should not be instantiated directly."""

    def __init__(self, parent: "SQLExecutable" = None):
        self._parent = parent

    async def execute(
        self,
        params=None,
        close=False,
        commit=False,
    ):
        return await self._parent.execute(params=params, close=close, commit=commit)

    async def close(self):
        await self._parent.close()

    def get_sql_class(self, sql_cls: type) -> type:
        return self.sqlFactory.get_sql_class(sql_cls)

    @property
    def sqlFactory(self) -> SQLFactory:
        return self._parent.sqlFactory


class SQL(SQLExecutable):
    """Usage:
    sql = SQL_statement(db).create_table(
        "users",
        [
            ("name", SQL_data_type.TEXT),
            ("age",  SQL_data_type.INTEGER)
        ],
    ).execute()"""

    def __init__(self):
        super().__init__(None)
        self._rslt = None
        self._sql_statement = None
        self._sql_statment: "SQLStatement" = None

    @classmethod
    def _get_db(cls):
        return App.db

    def sql(self) -> str:
        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return self._sql_statement.sql()

    @property
    def sqlFactory(self) -> SQLFactory:
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
        update = self.get_sql_class(Update)(table, parent=self)
        self._sql_statement = update
        return update

    def script(self, script: str) -> "SQLScript":
        self._sql_statement = self.get_sql_class(SQLScript)(script, self)
        return self._sql_statement

    async def execute(self, params=None, close=False, commit=False):
        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return await self._get_db().execute(self.sql(), params, close, commit)

    async def close(self):
        await self._get_db().close()


class SQLStatement(SQLExecutable):
    def __init__(self, parent: SQLExecutable = None):
        super().__init__(parent)

    def sql(self) -> str:
        raise NotImplementedError(
            "SQL_statement is an abstract class and should not be instantiated."
        )


class SQLScript(SQLStatement):
    def __init__(self, script: str, parent: SQLExecutable = None):
        super().__init__(parent)
        self._script = script

    def sql(self) -> str:
        return self._script


class CreateTable(SQLStatement):
    def __init__(
        self,
        table: str = "",
        columns: list[(str, SQLDataType, str)] = None,
        parent: SQLExecutable = None,
    ):
        super().__init__(parent)
        cols = [] if columns is None else columns
        self._table = table
        sql_column_definition = self.sqlFactory.get_sql_class(SQLColumnDefinition)
        self._columns = [
            sql_column_definition(name, data_type, constraint)
            for name, data_type, constraint in cols
        ]

    def column(self, name: str, data_type: SQLDataType, constraint: str = None):
        self._columns.append(
            self.sqlFactory.get_sql_class(SQLColumnDefinition)(
                name, data_type, constraint
            )
        )
        return self

    def sql(self) -> str:
        if self._table is None or len(self._table) == 0:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement must have a table name."
            )
        return (
            f"CREATE TABLE {self._table} ({[column.sql() for column in self._columns]})"
        )


class TableValuedQuery(SQLStatement):

    def __init__(self, parent: SQLExecutable):
        super().__init__(parent)

    def sql(self) -> str:
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
        column_list = [] if column_list is None else column_list
        self._column_list = column_list
        self._distinct = distinct
        self._from_statement: TableValuedQuery = None
        self._where: Where = None
        self._group_by: GroupBy = None
        self._having: Having = None

    def sql(self) -> str:
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

    def distinct(self, distinct: bool = True):
        """Sets the distinct flag for the select statement. Default is False."""
        self._distinct = distinct
        return self

    def columns(self, column_list: list[str]):
        """Sets the columns for the select statement. Default is ['*']. Any existing lists is discarded."""
        self._column_list = column_list
        return self

    def from_(self, table: str | TableValuedQuery):
        """Sets the from clause for the select statement. The statement will not execute without a from clause."""
        from_table = self.sqlFactory.get_sql_class(From)(table)
        self._from_statement = from_table
        return self

    def Where(self, condition: SQLExpression):
        """Sets the where clause for the select statement. Optional."""
        where = self.sqlFactory.get_sql_class(Where)(condition)
        self._where = where
        return self

    def Having(self, condition: SQLExpression):
        """Sets the having clause for the select statement. Optional."""
        having = self.sqlFactory.get_sql_class(Having)(condition)
        self._having = having
        return self


class Insert(SQLStatement):
    def __init__(
        self,
        table: str,
        columns: list[str] = None,
        rows=None,
        parent: SQLExecutable = None,
    ):
        super().__init__(parent)
        self._columns = [] if columns is None else columns
        self._table = table
        self._columns = columns
        self._rows = rows
        self._return_str: str = ""

    def sql(self) -> str:
        sql = (
            f"INSERT INTO {self._table} ({', '.join(self._columns)}) {self._rows.sql()}"
        )
        return sql + self._return_str

    def single_row(self, row: list[(str, str | Value)]):
        row = self.get_sql_class(Row)()
        for column_name, value in row:
            self.column(column_name)
            row.value(value)
        self._rows = self.get_sql_class(Values)([row])
        return self

    def column(self, column: str):
        self._columns.append(column)
        return self

    def values(self, values):
        self._rows = values
        return self

    def returning(self, column: str):
        self._return_str = f" RETURNING {column}"
        return self


class Update(SQLStatement):
    def __init__(self, table: str, parent: SQLExecutable = None):
        super().__init__(parent)
        self._table = table
        self._where: Where = None
        self.assignments: List[Assignment] = []

    def assignment(self, columns: list[str] | str, value: Value):
        self.assignments.append(
            self.sqlFactory.get_sql_class(Assignment)(columns, value)
        )
        return self

    def where(self, condition: SQLExpression):
        where: Where = self.sqlFactory.get_sql_class(Where)(condition)
        self._where = where
        return self

    def returning(self, column: str):
        self.sql += f" RETURNING {column}"
        return self

    def sql(self) -> str:
        sql = f"UPDATE {self._table} SET {', '.join([assignment.sql() for assignment in self.assignments])}"

        return sql
