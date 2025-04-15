"""This module SQLStatement classes from SQLExecutable to create and execute SQL statements."""

from enum import Enum, auto
from typing import Any, TypeAlias

from database.sql_executable import SQLExecutable, SQLManagedExecutable
from database.sql_clause import (
    Assignment,
    From,
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
from database.sql_factory import SQLFactory
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
        LOG.debug(f"{self.get_sql()=},  {close=}, {commit=}")
        sql = self.get_sql()
        return await SQL._get_db().execute(sql["query"], sql["params"], close, commit)

    async def close(self):
        await SQL._get_db().close()


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
        parent: SQLExecutable = None,
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


class CreateTable(SQLStatement):
    """A SQLStatement representing a CREATE TABLE statement.
    Default implementation complies with SQLite syntax."""

    def __init__(
        self,
        table: str = "",
        columns: list[tuple[str, type, BOColumnFlag, dict]] = None,
        parent: SQLExecutable = None,
    ):
        # LOG.debug(f"CreateTable({table=}, {columns=})")
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
            SQLColumnDefinition(name, data_type, constraint, parent=self, **pars)
        )
        return self

    def get_sql(self) -> SQL_Dict:
        """Get a string representation of the current SQL statement."""
        if self._table is None or len(self._table) == 0:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement must have a table name."
            )
        if len(self._columns) == 0:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement must have at least one column."
            )
        return {
            "query": f"CREATE TABLE IF NOT EXISTS {self._table} ({', '.join([column.get_query() for column in self._columns])})",
            "params": {},
        }


class TableValuedQuery(SQLStatement):
    """SQLStatement representing a statement that has a table as its result set.
    Should not be instantiated directly."""

    def __init__(self, parent: SQLExecutable):
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
        column_list: list[str] = None,
        distinct: bool = False,
        parent: SQLExecutable = None,
    ):
        super().__init__(parent)
        self._column_list = [] if column_list is None else column_list
        self._distinct = distinct
        self._from_statement: SQLManagedExecutable = None
        self._where: Where = None
        self._group_by: GroupBy = None
        self._having: Having = None

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
