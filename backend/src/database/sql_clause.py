"""Clauses used in SQL statements."""

from calendar import c
from enum import Enum
from typing import Self

from database.sql_executable import SQLExecutable, SQLManagedExecutable
from database.sql_expression import ColumnName, Row, SQLExpression, Value
from business_objects.bo_descriptors import BOColumnFlag

from core.app_logging import getLogger

LOG = getLogger(__name__)


class JoinOperator(Enum):
    """Enum for SQL join operators."""

    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class SQLColumnDefinition(SQLManagedExecutable):
    """Represents the definition of a column in an SQL table."""

    type_map = {}
    constraint_map: dict[BOColumnFlag, str] = {}

    def __init__(
        self,
        name: str,
        data_type: type,
        constraints: BOColumnFlag | None = None,
        parent: SQLExecutable | None = None,
        **args,
    ):
        super().__init__(parent)
        self._name = name
        if data_type not in self.__class__.type_map:
            raise ValueError(
                f"Unsupported data type for a {self.__class__.__name__}: {data_type}"
            )
        self._data_type = self.__class__.type_map[data_type]
        self._constraint = constraints
        self._arguments = args
        constraint_map = self.__class__.constraint_map
        for flag in constraints or BOColumnFlag.BOC_NONE:
            if flag not in constraint_map:
                raise ValueError(
                    f"Unsupported column constraint for a {self.__class__.__name__}: {flag}"
                )

    def get_query(self):
        return f"""{self._name} {self._data_type} {' '.join([
                self.__class__.constraint_map[flag].format(
                    **{
                        k: v.table if hasattr(v, 'table') else str(v).lower()
                        for k, v in self._arguments.items()
                    }
                )
                for flag in self._constraint or BOColumnFlag.BOC_NONE
            ])}"""


class From(SQLManagedExecutable):
    """Class for the FROM clause of an SQL statement."""

    def __init__(self, table: str, parent: SQLExecutable = None):
        super().__init__(parent)
        self._table = table
        self._joins: list[tuple[JoinOperator, str, SQLExpression]] = []

    def get_query(self) -> str:
        sql = f" FROM {self._table} "
        if len(self._joins) > 0:
            sql += " ".join([self._get_join_clause(join) for join in self._joins])
        return sql

    def _get_join_clause(self, join: tuple[JoinOperator, str, SQLExpression]) -> str:
        join_operator = join[0].value
        join_table = join[1]
        join_condition = (
            f"ON {join[2].get_query(km=self)}" if join[2] is not None else ""
        )
        return f"{join_operator} {join_table} {join_condition}"

    def join(
        self,
        table: str = None,
        join_constraint: "SQLExpression" = None,
        join_operator: JoinOperator = JoinOperator.FULL,
    ) -> Self:
        """Add a join to another table to the FROM clause."""
        self._joins.append((join_operator, table, join_constraint))
        return self


class Where(SQLManagedExecutable):
    """Represents a WHERE clause in an SQL statement."""

    def __init__(self, condition: SQLExpression, parent: SQLExecutable = None):
        super().__init__(parent)
        self._condition: SQLExpression = condition

    def get_query(self):
        if not self._condition:
            return ""
        return f" WHERE {self._condition.get_query(km=self)}"


class GroupBy(SQLManagedExecutable):
    """Represents a GROUP BY clause in an SQL statement."""

    def __init__(self, column_list: list[str] | str, parent: SQLExecutable = None):
        if not column_list:
            raise ValueError("Column list must be provided for GROUP BY clause.")
        super().__init__(parent)
        self._column_list = (
            column_list if isinstance(column_list, list) else [column_list]
        )

    def get_query(self):
        return f"GROUP BY {', '.join(self._column_list)}"


class Having(SQLManagedExecutable):
    """Represents a HAVING clause in an SQL statement."""

    def __init__(self, condition: SQLExpression, parent: SQLExecutable = None):
        if not condition:
            raise ValueError("Condition must be provided for HAVING clause.")
        super().__init__(parent)
        self._condition = condition

    def get_query(self):
        return f" HAVING {self._condition.get_query(km=self)}"


class Values(SQLManagedExecutable):
    """Represents the VALUES clause in an SQL statement such as an INSERT.
    It may contain multiple rows of Value objects. The values in the first row also define the
    names of the columns. Each row must have the same number of values.
    """

    def __init__(self, rows: list[Row] = [], parent: SQLExecutable = None):
        # LOG.debug(f"Values({rows=})")
        super().__init__(parent)
        self._rows = rows

    def row(self, row: Row) -> Self:
        """Add a row to the end of the list."""
        self._rows.append(row)
        return self

    def get_names(self) -> str:
        "List of value names"
        if not self._rows:
            return ""
        return self._rows[0].get_names()

    def get_query(self):
        """Return the SQL expression as a string."""
        return f"VALUES {', '.join([row.get_query(km=self) for row in self._rows])}"

    def __len__(self) -> int:
        """Return the number of rows."""
        return len(self._rows)


class Assignment(SQLManagedExecutable):
    """Represents an assignment in the SET clause of an SQL UPDATE statement."""

    def __init__(
        self,
        columns: list[ColumnName | str] | ColumnName | str,
        value: Value,
        parent: SQLExecutable = None,
    ):
        super().__init__(parent=parent)
        self._columns = columns if isinstance(columns, list) else [columns]
        self._value = value
        if not self._columns:
            raise ValueError("A column must be provided for assignment.")
        if not self._value:
            raise ValueError("A value must be assigned to the column.")

    def get_query(self):
        sql = (
            "("
            + ",".join(
                [
                    c.get_query(km=self) if isinstance(c, ColumnName) else c
                    for c in self._columns
                ]
            )
            + ") = "
            + self._value.get_query(km=self)
        )
        return sql
