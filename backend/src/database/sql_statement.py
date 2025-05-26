"""This module SQLStatement classes from SQLExecutable to create and execute SQL statements."""

from enum import Enum, auto
from typing import Any, Optional, TypeAlias, Self

from database.sql_executable import SQLExecutable, SQLManagedExecutable
from database.sql_clause import (
    Assignment,
    From,
    JoinOperator,
    GroupBy,
    Having,
    SQLColumnDefinition,
    Values,
    Where,
)
from database.sql_expression import (
    Row,
    SQLExpression,
    Value,
)
from database.sql_key_manager import SQL_Dict

from persistance.bo_descriptors import BOColumnFlag

from core.exceptions import InvalidSQLStatementException

from core.app_logging import getLogger

LOG = getLogger(__name__)


class SQLTemplate(Enum):
    "Keys for dialect specific SQL templates used in SQLScript"

    TABLEINFO = auto()
    TABLELIST = auto()
    TABLESQL = auto()


NamedValue: TypeAlias = tuple[str, Any] | Value
NamedValueList: TypeAlias = list[NamedValue]
NamedValueListList: TypeAlias = list[NamedValueList] | NamedValueList


class SQLStatement(SQLManagedExecutable):
    """Base class for SQL statements. Should not be instantiated directly."""

    def __new__(cls, *args, **kwargs):
        if cls == SQLStatement:
            raise NotImplementedError(
                "SQLStatement is an abstract class and should not be instantiated"
            )
        return super().__new__(cls, *args, **kwargs)


class SQLScript(SQLStatement):
    """A SQL statement that executes a script verbatim"""

    sql_templates = {}

    def __init__(
        self,
        script_or_template: str | SQLTemplate,
        parent: SQLExecutable | None = None,
        **kwargs,
    ):
        super().__init__(parent)
        self._script = " ".join(
            (
                script_or_template
                if isinstance(script_or_template, str)
                else self.__class__.sql_templates[script_or_template]
            ).split()
        )
        self._script = self.merge_params(self._script, kwargs)

    def get_sql(self) -> SQL_Dict:
        """Get a string representation of the current SQL statement."""
        return {"query": self._script, "params": self.params}


class TableValuedQuery(SQLStatement):
    """SQLStatement representing a statement that has a table as its result set.
    Should not be instantiated directly."""

    def __init__(self, parent: SQLExecutable | None = None):
        super().__init__(parent)

    def get_sql(self) -> SQL_Dict:
        """Get a string representation and parameters of the current SQL statement."""
        return {"query": self.get_query(), "params": self.params}

    def get_query(self) -> str:
        """Get a string representation of the current SQL statement."""
        raise NotImplementedError(
            "Table_Valued_Query is an abstract class and should not be instantiated."
        )


class Select(TableValuedQuery):
    """Represents a SQL Select Statement. Default implementation complies with SQLite syntax."""

    def __init__(
        self,
        column_list: list[str] | None = None,
        distinct: bool = False,
        parent: SQLExecutable | None = None,
    ):
        super().__init__(parent)
        self._init_attrs(column_list or [], distinct)

    def _init_attrs(
        self,
        column_list: list[str] | None = None,
        distinct: bool = False,
    ):
        self._column_list = column_list or []
        self._distinct = distinct
        self._from_statement: From | None = None
        self._where: Where | None = None
        self._group_by: GroupBy | None = None
        self._having: Having | None = None

    def get_query(self) -> str:
        """Get a string representation of the current SQL statement."""
        if self._from_statement is None:
            raise InvalidSQLStatementException(
                "SELECT statement must have a FROM clause."
            )
        query: str = (
            f"SELECT {'DISTINCT ' if self._distinct else ''}{(', '.join(self._column_list)) or '*'}"
        )
        query += self.merge_params(**self._from_statement.get_sql())
        if self._where is not None:
            query += self.merge_params(**self._where.get_sql())
        if self._group_by is not None:
            query += self._group_by.get_query()
        if self._having is not None:
            query += self.merge_params(**self._having.get_sql())
        return query

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
        self._from_statement = From(table, parent=self)
        return self

    def join(
        self,
        table: str,
        join_constraint: SQLExpression | None = None,
        join_operator: JoinOperator = JoinOperator.FULL,
    ):
        """Sets the join clause for the select statement."""
        if self._from_statement is None:
            raise InvalidSQLStatementException(
                "SELECT statement must have a FROM clause before joining."
            )
        self._from_statement.join(table, join_constraint, join_operator)
        return self

    def where(self, condition: SQLExpression):
        """Sets the where clause for the select statement. Optional."""
        self._where = Where(condition, parent=self)
        return self

    def having(self, condition: SQLExpression):
        """Sets the having clause for the select statement. Optional."""
        self._having = Having(condition, parent=self)
        return self


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
        rows: NamedValueListList = None,
        parent: SQLExecutable = None,
    ):
        super().__init__(parent)
        self._table = table
        self._values = Values([], parent=self)
        self._return_str: str = ""
        if rows is not None:
            self.rows(rows=rows)

    def get_sql(self) -> SQL_Dict:
        """Get a string representation of the current SQL statement."""
        if not self._values:
            raise InvalidSQLStatementException(
                "INSERT statement must have at least one row of values."
            )
        query = " ".join(
            [
                "INSERT INTO",
                self._table,
                self._values.get_names(),
                self.merge_params(**self._values.get_sql()),
            ]
        )
        # LOG.debug(f"Insert.sql() -> query: {query + self._return_str}; params: {self.params}")
        return {"query": query + self._return_str, "params": self.params}

    def _single_row(self, cols: NamedValueList):
        """Add a single row of values to be inserted"""
        # LOG.debug(f"Insert._single_row({cols=})")
        row = Row(
            [
                (col if isinstance(col, Value) else Value(name=col[0], value=col[1]))
                for col in cols
            ]
        )
        self._values.row(row)
        return self

    def rows(self, rows: NamedValueListList):
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
        self._return_str: str = ""

    def assignment(self, columns: list[str] | str, value: Value):
        """Add an assignment to the list of assignments to be made in the update statement.

        Args:
            columns (list[str] | str):
                A list of column names to be assigned the value.
            value (Value):
                The value to be assigned to the column(s)."""
        self.assignments.append(Assignment(columns, value, parent=self))
        return self

    def where(self, condition: SQLExpression):
        """Set the where clause for the update statement."""
        where: Where = Where(condition, parent=self)
        self._where = where
        return self

    def returning(self, column: str):
        """Set the column to be returned after the update statement is executed."""
        self._return_str = f" RETURNING {column}"
        return self

    def get_sql(self) -> SQL_Dict:
        """Get a string representation of the current SQL statement."""
        if not self.assignments:
            raise InvalidSQLStatementException(
                "UPDATE statement must have at least one assignment."
            )
        query = " ".join(
            [
                "UPDATE",
                self._table,
                f"SET {', '.join([self.merge_params(**assignment.get_sql()) for assignment in self.assignments])}",
                self.merge_params(**self._where.get_sql()) if self._where else "",
                self._return_str,
            ]
        )
        return {"query": query, "params": self.params}


class CreateTable(SQLStatement):
    """A SQLStatement representing a CREATE TABLE statement.
    Default implementation complies with SQLite syntax."""

    def __init__(
        self,
        table: str = "",
        columns: list[tuple[str, type, BOColumnFlag | None, dict]] = None,
        temporary: bool = False,
        parent: SQLExecutable | None = None,
    ):
        # LOG.debug(f"CreateTable({table=}, {columns=}, {temporary=})")
        super().__init__(parent)
        self._table = table
        self._columns: list[SQLColumnDefinition] = []
        for column_description in columns or []:
            name, data_type, constraint, pars = column_description
            self.column(name, data_type, constraint, **pars)
        self._temporary = temporary

    def column(
        self, name: str, data_type: type, constraint: BOColumnFlag | None = None, **pars
    ) -> Self:
        """Add a column to the table to be created.
        The column will be added to the end of the column list."""
        if isinstance(self, Select):
            raise InvalidSQLStatementException(
                "Cannot add columns to a CREATE TABLE statement with AS SELECT."
            )
        self._columns.append(
            SQLColumnDefinition(name, data_type, constraint, parent=self, **pars)
        )
        return self

    def as_select(self, *args, **kwargs) -> "CreateTableAsSelect":
        """Set the select statement to be used as the source for the table."""
        if self._columns:
            raise InvalidSQLStatementException(
                "Cannot add AS SELECT to a CREATE TABLE statement with columns."
            )
        self.__class__ = CreateTableAsSelect  # type: ignore
        if isinstance(self, CreateTableAsSelect):
            self._init_attrs(*args, **kwargs)
            return self
        raise TypeError("Failed to change class of CreateTable to CreateTableAsSelect")

    def get_sql(self) -> SQL_Dict:
        """Get a string representation of the current SQL statement."""
        if self._table is None or len(self._table) == 0:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement must have a table name."
            )
        if isinstance(self, Select) and self._columns:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement cannot have both columns and 'AS SELECT' clause."
            )
        if len(self._columns) == 0:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement must have at least one column or 'AS SELECT' clause."
            )
        return {
            "query": " ".join(
                [
                    "CREATE",
                    "TEMPORARY" if self._temporary else "",
                    "TABLE",
                    self._table,
                    f"({', '.join([column.get_query() for column in self._columns])})",
                ]
            ),
            "params": {},
        }


class CreateTableAsSelect(CreateTable, Select):
    """A SQLStatement representing a CREATE TABLE AS SELECT statement.
    Default implementation complies with SQLite syntax."""

    def __init__(
        self,
        table: str = "",
        columns: list[tuple[str, type, BOColumnFlag | None, dict]] | None = None,
        temporary: bool = False,
        parent: SQLExecutable | None = None,
    ):
        super().__init__(table, columns, temporary, parent)

    def get_sql(self) -> SQL_Dict:
        return {
            "query": " ".join(
                [
                    "CREATE",
                    "TEMPORARY" if self._temporary else "",
                    "TABLE",
                    self._table,
                    "AS",
                    self.get_query(),
                ]
            ),
            "params": self.params,
        }


class CreateView(Select):
    """A SQLStatement representing a CREATE VIEW statement.
    Default implementation complies with SQLite syntax."""

    def __init__(
        self,
        view: str = "",
        view_columns: list[str] | None = None,
        *args,
        temporary: bool = False,
        parent: SQLExecutable | None = None,
        **kwargs,
    ):
        # LOG.debug(f"CreateTable({table=}, {columns=}, {temporary=})")
        self._view = view
        self._columns: list[str] = view_columns or []
        self._temporary = temporary
        super().__init__(parent=parent, *args, **kwargs)

    def get_sql(self) -> SQL_Dict:
        """Get a string representation of the current SQL statement."""
        if self._view is None or len(self._view) == 0:
            raise InvalidSQLStatementException(
                "CREATE VIEW statement must have a view name."
            )
        return {
            "query": " ".join(
                [
                    "CREATE",
                    "TEMPORARY VIEW" if self._temporary else "VIEW",
                    "IF NOT EXISTS",
                    self._view,
                    (
                        ("( " + ", ".join(self._columns) + " ) AS")
                        if self._columns
                        else "AS"
                    ),
                    super().get_query(),
                ]
            ),
            "params": self.params,
        }
