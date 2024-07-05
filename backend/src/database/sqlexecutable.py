"""This module defines a SQLExecutable class that is used to create and execute SQL statements."""

from enum import Enum, auto
from typing import TypeAlias

from core.app import App
from database.sqlexpression import (
    SQLExpression,
    SQLColumnDefinition,
    Value,
    Row,
    Values,
    From,
    Assignment,
    Where,
    GroupBy,
    Having,
)
from database.sqlfactory import SQLFactory
from core.app_logging import getLogger

LOG = getLogger(__name__)


class InvalidSQLStatementException(Exception):
    """
    Exception raised when an invalid SQL statement is encountered.
    """


class SQLDataType(Enum):
    "Basic SQL data types compliant with SQLite. DB implementations should override this enum."
    TEXT = auto()
    INTEGER = auto()
    REAL = auto()
    BLOB = auto()


class SQLTemplate(Enum):
    "Keys for dialect specific SQL templates used in SQLScript"
    TABLEINFO = auto()
    TABLELIST = auto()
    TABLESQL = auto()


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
        """Get the current database connection."""
        return App.db

    def sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return self._sql_statement.sql()

    @property
    def sql_factory(self) -> SQLFactory:
        """Get the SQLFactory of the current database. Usually call get_sql_class instead."""
        return self._get_db().sql_factory

    def create_table(
        self, table: str, columns: list[(str, SQLDataType)] = None
    ) -> "CreateTable":
        """Sets the SQL statement to create a table and returns a create_table object"""
        # LOG.debug(f"SQL.create_table({table=}, {columns=})")
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

    def script(self, script_or_template: str | SQLTemplate, **kwargs) -> "SQLScript":
        """Set the SQL statement to execute the specific script or
        use a template to build an SQLScript
        """
        if "parent" in kwargs:
            raise ValueError(
                f"'parent={kwargs['parent']}' must not be a template argument"
            )
        self._sql_statement = self.get_sql_class(SQLScript)(
            script_or_template, parent=self, **kwargs
        )
        return self._sql_statement

    async def execute(self, params=None, close=False, commit=False):
        """Execute the current SQL statement on the database.
        Must create the statement before calling this method"""
        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return await self._get_db().execute(self.sql(), params, close, commit)

    async def close(self):
        await self._get_db().close()


class SQLStatement(SQLExecutable):
    """Base class for SQL statements. Should not be instantiated directly."""

    def sql(self) -> str:
        """Get a string representation of the current SQL statement.
        Must be implemented by subclasses."""
        raise NotImplementedError(
            "SQL_statement is an abstract class and should not be instantiated."
        )


class SQLScript(SQLStatement):
    """A SQL statement that executes a script verbatim"""

    sql_templates = {}

    def __init__(
        self,
        script_or_template: str | SQLTemplate,
        parent: SQLExecutable = None,
        **kwargs,
    ):
        super().__init__(parent)
        self._script = (
            script_or_template
            if isinstance(script_or_template, str)
            else self.__class__.sql_templates.get(script_or_template).format(**kwargs)
        )

    def sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        return self._script


class CreateTable(SQLStatement):
    """A SQLStatement representing a CREATE TABLE statement.
    Default implementation complies with SQLite syntax."""

    def __init__(
        self,
        table: str = "",
        columns: list[(str, SQLDataType, str, dict)] = None,
        parent: SQLExecutable = None,
    ):
        super().__init__(parent)
        cols = columns or []
        self._table = table
        sql_column_definition_cls = self.sql_factory.get_sql_class(SQLColumnDefinition)
        self._columns = [
            sql_column_definition_cls(name, data_type, constraint, **pars)
            for name, data_type, constraint, pars in cols
        ]

    def column(self, name: str, data_type: SQLDataType, constraint: str = None, **pars):
        """Add a column to the table to be created.
        The column will be added to the end of the column list."""
        # LOG.debug(f"CreateTable.column({name=}, {data_type=}, {constraint=}, {pars=})")
        self._columns.append(
            self.sql_factory.get_sql_class(SQLColumnDefinition)(
                name, data_type, constraint, **pars
            )
        )
        return self

    def sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        if self._table is None or len(self._table) == 0:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement must have a table name."
            )
        return f"CREATE TABLE IF NOT EXISTS {self._table} ({', '.join([column.sql() for column in self._columns])})"


class TableValuedQuery(SQLStatement):
    """SQLStatement representing a statement that has a table as its result set.
    Should not be instantiated directly."""

    def __init__(self, parent: SQLExecutable):
        super().__init__(parent)

    def sql(self) -> str:
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

    def sql(self) -> str:
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
        from_table = self.sql_factory.get_sql_class(From)(table)
        self._from_statement = from_table
        return self

    def where(self, condition: SQLExpression):
        """Sets the where clause for the select statement. Optional."""
        where = self.sql_factory.get_sql_class(Where)(condition)
        self._where = where
        return self

    def having(self, condition: SQLExpression):
        """Sets the having clause for the select statement. Optional."""
        having = self.sql_factory.get_sql_class(Having)(condition)
        self._having = having
        return self


NamedValue: TypeAlias = tuple[str, any] | Value
NamedValueList: TypeAlias = list[NamedValue]


class Insert(SQLStatement):
    """A SQLStatement representing an INSERT statement.
    Multiple rows may be inserted. It is assumed that all rows have the same columns.
    Rows are represented as a list of Values.
    Values are Value objects or a tuple of column name and value
    Default implementation complies with SQLite syntax.
    """

    def __init__(
        self,
        table: str,
        rows: list[NamedValueList] | NamedValueList = None,
        parent: SQLExecutable = None,
    ):
        super().__init__(parent)
        self._table = table
        self._values = self.get_sql_class(Values)([])
        self._return_str: str = ""
        if rows is not None:
            self.rows(rows=rows)

    def sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        sql = f"INSERT INTO {self._table} {self._values.names()} {self._values.sql()}"
        # LOG.debug(f"Insert.sql() -> {sql + self._return_str}")
        return sql + self._return_str

    def _single_row(self, cols: NamedValueList):
        """Add a single row of values to be inserted"""
        # LOG.debug(f"Insert._single_row({cols=})")
        row = self.get_sql_class(Row)(
            [
                (
                    col
                    if isinstance(col, Value)
                    else self.get_sql_class(Value)(name=col[0], value=col[1])
                )
                for col in cols
            ]
        )
        self._values.row(row)
        # LOG.debug(f"{self.sql()=}")
        return self

    def rows(self, rows: list[NamedValueList] | NamedValueList):
        """Add rows of values to be inserted"""
        # LOG.debug(f"Insert.rows({rows=})")
        for row in rows if not rows or isinstance(rows[0], list) else [rows]:
            self._single_row(row)
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
        self.assignments: list[Assignment] = []

    def assignment(self, columns: list[str] | str, value: Value):
        """Add an assignment to the list of assignments to be made in the update statement.

        Args:
            columns (list[str] | str):
                A list of column names to be assigned the value.
            value (Value):
                The value to be assigned to the column(s)."""
        self.assignments.append(
            self.sql_factory.get_sql_class(Assignment)(columns, value)
        )
        return self

    def where(self, condition: SQLExpression):
        """Set the where clause for the update statement."""
        where: Where = self.sql_factory.get_sql_class(Where)(condition)
        self._where = where
        return self

    def returning(self, column: str):
        """Set the column to be returned after the update statement is executed."""
        self.sql += f" RETURNING {column}"
        return self

    def sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        sql = f"""UPDATE {self._table}
            SET {', '.join([assignment.sql() for assignment in self.assignments])}"""

        return sql
