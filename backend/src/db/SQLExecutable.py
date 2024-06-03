from enum import Enum, auto
from typing import List
from .SQLExpression import (
    SQLExpression,
    SQL_column_definition,
    Value,
    Row,
    Values,
    Assignment,
    Where,
    Group_By,
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
    parent: "SQLExecutable" = None

    async def execute(
        self,
        params=None,
        close=False,
        commit=False,
    ):
        return await self.parent.execute(params=params, close=close, commit=commit)

    async def close(self):
        await self.parent.close()

    def get_sql_class(self, sql_cls: type) -> type:
        return self.sqlFactory.get_sql_class(sql_cls)

    @property
    def sqlFactory(self) -> SQLFactory:
        return self.parent.sqlFactory


class SQL(SQLExecutable):
    """Usage:
    sql = SQL_statement(db).create_table(
        "users",
        [
            ("name", SQL_data_type.TEXT),
            ("age",  SQL_data_type.INTEGER)
        ],
    ).execute()"""

    def sql(self) -> str:
        if self.sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return self.sql_statement.sql()

    @property
    def sqlFactory(self) -> SQLFactory:
        return self.db.sqlFactory

    def __init__(self, db: DB):
        self.db = db
        self.rslt = None
        self.sql_statement = None
        self.sql_statment: "SQL_statement" = None

    def create_table(
        self, table: str, columns: list[(str, SQLDataType)] = None
    ) -> "Create_Table":
        """Sets the SQL statement to create a table and returns a create_table object"""
        create_table = self.get_sql_class(Create_Table)(table, columns, self)
        # create_table = Create_Table(table, columns, self)
        self.sql_statement = create_table
        return create_table

    def select(self, column_list: list[str] = None, distinct: bool = False) -> "Select":
        """Sets the SQL statement to a select statement and returns a select object"""
        select = self.get_sql_class(Select)(column_list, distinct, self)
        self.sql_statement = select
        return select

    def insert(self, table: str, columns: list[str] = None) -> "Insert":
        """Sets the SQL statement to a insert statement and returns an insert object"""
        insert = self.get_sql_class(Insert)(table, columns, parent=self)
        self.sql_statement = insert
        return insert

    def update(self, table: str) -> "Update":
        update = self.get_sql_class(Update)(table, parent=self)
        self.sql_statement = update
        return update

    def script(self, script: str) -> "SQL_script":
        self.sql_statement = self.get_sql_class(SQL_script)(script, self)
        return self.sql_statement

    async def execute(self, params=None, close=False, commit=False):
        if self.sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return await self.db.execute(self.sql(), params, close, commit)

    async def close(self):
        await self.db.close()


class SQL_statement(SQLExecutable):

    def __init__(self, parent: SQLExecutable = None):
        self.parent = parent

    def sql(self) -> str:
        raise NotImplementedError(
            "SQL_statement is an abstract class and should not be instantiated."
        )


class SQL_script(SQL_statement):
    def __init__(self, script: str, parent: SQLExecutable = None):
        self.script = script
        super().__init__(parent)

    def sql(self) -> str:
        return self.script


class Create_Table(SQL_statement):
    def __init__(
        self,
        table: str = "",
        columns: list[(str, SQLDataType, str)] = None,
        parent: SQLExecutable = None,
    ):
        self.columns = [] if columns is None else columns
        self.table = table
        super().__init__(parent)
        sql_column_definition = self.sqlFactory.get_sql_class(SQL_column_definition)
        self.columns = [
            sql_column_definition(name, data_type, constraint)
            for name, data_type, constraint in self.columns
        ]

    def column(self, name: str, data_type: SQLDataType, constraint: str = None):
        self.columns.append(
            self.sqlFactory.get_sql_class(SQL_column_definition)(
                name, data_type, constraint
            )
        )
        return self

    def sql(self) -> str:
        if self.table is None or len(self.table) == 0:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement must have a table name."
            )
        return (
            f"CREATE TABLE {self.table} ({[column.sql() for column in self.columns]})"
        )


class TableValuedQuery(SQL_statement):

    def __init__(self, parent: SQLExecutable):
        super().__init__(parent)

    def sql(self) -> str:
        raise NotImplementedError(
            "Table_Valued_Query is an abstract class and should not be instantiated."
        )


class Select(TableValuedQuery):
    def __init__(
        self,
        column_list: list[str] = None,
        distinct: bool = False,
        parent: SQLExecutable = None,
    ):
        column_list = [] if column_list is None else column_list
        self.column_list = column_list
        self.distinct = distinct
        self.from_statement: TableValuedQuery = None
        self.where: Where = None
        self.group_by: Group_By = None
        self.having: Having = None
        super().__init__(parent)

    def sql(self) -> str:
        if self.from_statement is None:
            raise InvalidSQLStatementException(
                "SELECT statement must have a FROM clause."
            )
        sql = f"SELECT {'DISTINCT ' if self.distinct else ''}{', '.join(self.column_list)}"
        if self.column_list is None or len(self.column_list) == 0:
            sql += "*"
        sql += self.from_statement.sql()
        if self.where is not None:
            sql += self.where.sql()
        if self.group_by is not None:
            sql += self.group_by.sql()
        if self.having is not None:
            sql += self.having.sql()
        return sql

    def Distinct(self, distinct: bool = True):
        self.distinct = distinct
        return self

    def Columns(self, column_list: list[str]):
        self.column_list = column_list
        return self

    def From(self, table: str | TableValuedQuery):
        from_table = self.sqlFactory.get_sql_class(From)(table)
        self.from_statement = from_table
        return self

    def Where(self, condition: SQLExpression):
        where = self.sqlFactory.get_sql_class(Where)(condition)
        self.where = where
        return self

    def Having(self, condition: SQLExpression):
        having = self.sqlFactory.get_sql_class(Having)(condition)
        self.having = having
        return self


class Insert(SQL_statement):
    def __init__(
        self,
        table: str,
        columns: list[str] = None,
        rows=None,
        parent: SQLExecutable = None,
    ):
        self.columns = [] if columns is None else columns
        self.table = table
        self.columns = columns
        self.rows = rows
        super().__init__(parent)
        self._return_str: str = ""

    def sql(self) -> str:
        sql = f"INSERT INTO {self.table} ({', '.join(self.columns)}) {self.rows.sql()}"
        return sql + self._return_str

    def single_row(self, row: list[(str, str | Value)]):
        row = self.get_sql_class(Row)()
        for column_name, value in row:
            self.column(column_name)
            row.value(value)
        self.rows = self.get_sql_class(Values)([row])
        return self

    def column(self, column: str):
        self.columns.append(column)
        return self

    def values(self, values):
        self.rows = values
        return self

    def returning(self, column: str):
        self._return_str = f" RETURNING {column}"
        return self


class Update(SQL_statement):
    def __init__(self, table: str, parent: SQLExecutable = None):
        self.table = table
        self.where: Where = None
        self.assignments: List[Assignment] = []
        super().__init__(parent)

    def assignment(self, columns: list[str] | str, value: Value):
        self.assignments.append(
            self.sqlFactory.get_sql_class(Assignment)(columns, value)
        )
        return self

    def Where(self, condition: SQLExpression):
        where: Where = self.sqlFactory.get_sql_class(Where)(condition)
        self.where = where
        return self

    def returning(self, column: str):
        self.sql += f" RETURNING {column}"
        return self

    def sql(self) -> str:
        sql = f"UPDATE {self.table} SET {', '.join([assignment.sql() for assignment in self.assignments])}"

        return sql
