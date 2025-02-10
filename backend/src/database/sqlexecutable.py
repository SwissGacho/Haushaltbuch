"""This module defines a SQLExecutable class that is used to create and execute SQL statements."""

from enum import Enum, auto
import re
from typing import TypeAlias, Optional, Type

from core.app import App
from core.app_logging import getLogger
from core.base_objects import DBBaseClass
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
from database.sqlkeymanager import SQLKeyManager

from persistance.bo_descriptors import BOColumnFlag

from core.exceptions import InvalidSQLStatementException

LOG = getLogger(__name__)


class SQLTemplate(Enum):
    "Keys for dialect specific SQL templates used in SQLScript"
    TABLEINFO = auto()
    TABLELIST = auto()
    TABLESQL = auto()


class SQLExecutable(object):
    """Base class for SQL operations. Should not be instantiated directly."""

    def __init__(self, parent: Optional["SQLExecutable"] = None):
        self._parent = parent
        self._parameters = {}

    def __new__(cls, *args, **kwargs):
        # Should we really get "_parent" rather than "parent"?
        LOG.debug(f"SQLExecutable.__new__({cls=}, {args=}, {kwargs=})")
        future_parent = kwargs.get("parent", None)
        LOG.debug(f"{future_parent=}")
        if future_parent is not None and not (isinstance(future_parent, SQLExecutable)):
            LOG.debug(f"{type(future_parent)=}")
            raise TypeError(f"Expected 'SQLExecutable' as parent, got {type(future_parent).__name__}")
        
        actual_class = future_parent._get_db().sql_factory.get_sql_class(cls) 

        LOG.debug(f"{actual_class=}")
        if not issubclass(actual_class, SQLExecutable):
            raise TypeError(f"Factory returned an invalid class: {actual_class}")
        LOG.debug(f"{super().__new__(actual_class)=}")
        return super().__new__(actual_class) # type: ignore

    async def execute(
        self,
        params: dict[str, type] = None,
        close: bool | int = False,
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

    @classmethod
    def _get_db(cls) -> DBBaseClass:
        raise NotImplementedError("Must be implemented by subclass.")

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

    def __new__(cls, *args, **kwargs):
        LOG.debug("SQL.__new__")
        factory = cls._get_db().sql_factory
        actual_class = factory.get_sql_class(cls)
        return object().__new__(actual_class, *args, **kwargs)

    def __init__(self):
        super().__init__(None)
        self._rslt = None
        self._sql_statement: "SQLStatement" = None

    def __repr__(self):
        return f"SQL({self._sql_statement})"

    def __str__(self):
        return f"SQL({self._sql_statement})"

    @classmethod
    def _get_db(cls) -> DBBaseClass:
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
        return SQL._get_db().sql_factory

    def create_table(
        self, table: str, columns: list[(str, type)] = None
    ) -> "CreateTable":
        """Sets the SQL statement to create a table and returns a create_table object"""
        # LOG.debug(f"SQL.create_table({table=}, {columns=})")
        create_table = CreateTable(table, columns, parent=self)
        # create_table = Create_Table(table, columns, self)
        self._sql_statement = create_table
        return create_table

    def select(self, column_list: list[str] = None, distinct: bool = False) -> "Select":
        """Sets the SQL statement to a select statement and returns a select object"""
        select = Select(column_list, distinct, parent=self)
        self._sql_statement = select
        return select

    def insert(self, table: str, columns: list[str] = None) -> "Insert":
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
        self._sql_statement = SQLScript(
            script_or_template, parent=self, **kwargs
        )
        return self._sql_statement

    async def execute(self, params=None, close=False, commit=False):
        """Execute the current SQL statement on the database.
        Must create the statement before calling this method"""
        if self._sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return await SQL._get_db().execute(self.get_sql(), params, close, commit)

    async def close(self):
        await SQL._get_db().close()


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

    def get_sql(self) -> tuple[str, dict[str, str]]:
        """Get a string representation of the current SQL statement.
        Must be implemented by subclasses."""
        return self.get_sql(), self.get_params()


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
            else self.__class__.sql_templates[script_or_template]
        )
        print(f"{self._script=}")
        self._script = self._register_and_replace_named_parameters(self._script, kwargs)

    def get_sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        return self._script

    def _register_and_replace_named_parameters(
        self, query: str, params: dict[str, str]
    ):
        for key, value in params.items():
            if not key in query:
                continue
            final_key = self._create_param(key, value)
            query = re.sub(fr":{key}\b", f":{final_key}", query)
        return query

    def _create_param(self, proposed_key: str, value):
        final_key = self.register_key(proposed_key)
        self._params[final_key] = value
        return final_key


class CreateTable(SQLStatement):
    """A SQLStatement representing a CREATE TABLE statement.
    Default implementation complies with SQLite syntax."""

    def __init__(
        self,
        table: str = "",
        columns: list[(str, type, BOColumnFlag, dict)] = None,
        parent: SQLExecutable = None,
    ):
        super().__init__(parent)
        self._table = table
        self._columns: list[SQLColumnDefinition] = []
        for column_description in columns or []:
            name, data_type, constraint, pars = column_description
            self.column(name, data_type, constraint, **pars)

    def column(self, name: str, data_type: type, constraint: str = None, **pars):
        """Add a column to the table to be created.
        The column will be added to the end of the column list."""
        self._columns.append(
            self.sql_factory.get_sql_class(SQLColumnDefinition)(
                name, data_type, constraint, **pars
            )
        )
        return self

    def get_params(self):
        return {}

    def get_sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        if self._table is None or len(self._table) == 0:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement must have a table name."
            )
        if len(self._columns) == 0:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement must have at least one column."
            )
        return f"CREATE TABLE IF NOT EXISTS {self._table} ({', '.join([column.get_sql() for column in self._columns])})"


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
        sql += self._from_statement.get_sql()
        if self._where is not None:
            sql += self._where.get_sql()
        if self._group_by is not None:
            sql += self._group_by.get_sql()
        if self._having is not None:
            sql += self._having.get_sql()
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

    def get_params(self):
        return self._values.get_params()

    def get_sql(self) -> str:
        """Get a string representation of the current SQL statement."""
        sql = (
            f"INSERT INTO {self._table} {self._values.names()} {self._values.get_sql()}"
        )
        # LOG.debug(f"Insert.sql() -> {sql + self._return_str}")
        return sql + self._return_str

    def _single_row(self, cols: list[tuple[str, any] | Value]):
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
        # LOG.debug(f"{row.get_sql()=}")
        self._values.row(row)
        # LOG.debug(f"{self.get_sql()=}")
        return self

    def rows(self, rows: list[NamedValueList] | NamedValueList):
        """Add rows of values to be inserted"""
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
        self.get_sql += f" RETURNING {column}"
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
        sql = f"UPDATE {self._table} "
        sql += f"SET {', '.join([assignment.get_sql() for assignment in self.assignments])}"
        sql += self._where.get_sql()
        return sql
