"""Classes for building SQL expressions that can be used in SQLStatements."""

from enum import Enum
from typing import List

from core.app_logging import getLogger

LOG = getLogger(__name__)


class JoinOperator(Enum):
    """Enum for SQL join operators."""

    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class SQLExpression:
    """Base class for an SQL expression.
    Can be instantiated directly to create an expression verbatim from a string."""

    def __init__(self, expression: str):
        self._expression = "Null" if expression is None else expression

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return self._expression


class From(SQLExpression):
    """Class for the FROM clause of an SQL statement."""

    def __init__(self, table):
        super().__init__(None)
        self.table = table
        self.joins: List[(JoinOperator, str, SQLExpression)] = []

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        sql = f" FROM {self.table}"
        if len(self.joins) > 0:
            sql += " ".join(
                [f"{join[0]} {join[1]} ON {join[2].sql()}" for join in self.joins]
            )
        return sql

    def join(
        self,
        table=None,
        join_constraint: "SQLExpression" = None,
        join_operator: JoinOperator = JoinOperator.FULL,
    ):
        """Add a join to another table to the FROM clause."""
        self.joins.append((join_operator, table, join_constraint))


class SQLMultiExpressin(SQLExpression):
    """Abstract class to combine any number of SQL expressions with an operator.
    Should not be instantiated directly."""

    def __init__(self, arguments: List[SQLExpression]):
        super().__init__(None)
        self.arguments = arguments

    operator: str = None

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        if self.__class__.operator is None:
            raise NotImplementedError(
                "SQL_multi_expression is an abstract class and should not be instantiated."
            )
        return self.__class__.operator.join(
            [expression.sql() for expression in self._expression]
        )


class And(SQLMultiExpressin):
    """Represents a SQL AND expression."""

    operator = " AND "


class Or(SQLMultiExpressin):
    """Represents a SQL OR expression."""

    operator = " OR "


class SQLBinaryExpression(SQLExpression):
    """Abstract class to combine exactly two SQL expressions with an operator.
    Should not be instantiated directly."""

    def __init__(self, left: SQLExpression | str, right: SQLExpression | str):
        super().__init__(None)
        self.left = left if isinstance(left, SQLExpression) else SQLExpression(left)
        self.right = right if isinstance(right, SQLExpression) else SQLExpression(right)

    operator = None

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        if self.__class__.operator is None:
            raise NotImplementedError(
                "SQL_binary_expression is an abstract class and should not be instantiated."
            )
        return f" ({self.left.sql()} {self.__class__.operator} {self.right.sql()}) "


class Eq(SQLBinaryExpression):
    """Represents a SQL = expression."""

    operator = " = "


class SQLTernaryExpression(SQLExpression):
    """Abstract class to combine exactly three SQL expressions with two operators.
    Should not be instantiated directly."""

    def __init__(
        self,
        first: SQLExpression | str,
        second: SQLExpression | str,
        third: SQLExpression | str,
    ):
        super().__init__(None)
        self.first = first if isinstance(first, SQLExpression) else SQLExpression(first)
        self.second = (
            second if isinstance(second, SQLExpression) else SQLExpression(second)
        )
        self.third = third if isinstance(third, SQLExpression) else SQLExpression(third)

    operator_one = None
    operator_two = None

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        if self.__class__.operator_one is None or self.__class__.operator_two is None:
            raise NotImplementedError(
                "SQL_binary_expression is an abstract class and should not be instantiated."
            )
        return f" ({self.first.sql()} {self.__class__.operator_one} {self.second.sql()} {self.__class__.operator_two} {self.third.sql()}) "


class SQLBetween(SQLTernaryExpression):
    """Represents a SQL BETWEEN expression."""

    operator_one = " BETWEEN "
    operator_two = " AND "


class Value(SQLExpression):
    """Represents a value in an SQL statement."""

    def _init__(self, value: str):
        super().__init__(None)
        self._value = value

    def sql(self) -> str:
        return self._value


class Row(SQLExpression):
    """Represents a list of values defining a row in an SQL statement such as an INSERT."""

    def __init__(self, values: list[Value]):
        super().__init__(None)
        self.values = values

    def value(self, value: Value):
        """Add a value to the end of the row."""
        self.values.append(value)
        return self

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f"({', '.join([value.sql() for value in self.values])})"


class Values(SQLExpression):
    """Represents a list of rows in an SQL statement such as an INSERT."""

    def __init__(self, rows: list[Row]):
        super().__init__(None)
        self.rows = rows

    def row(self, value: Row):
        """Add a row to the end of the list."""
        self.rows.append(value)
        return self

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f"VALUES {', '.join([row.sql() for row in self.rows])}"


class Assignment(SQLExpression):
    """Represents an assignment in an SQL statement such as an UPDATE."""

    def __init__(
        self,
        columns: list[str] | str,
        value: Value,
    ):
        super().__init__(None)
        self.columns = [columns] if isinstance(columns, str) else columns
        self.value = value
        self.where: Where = None

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        sql = Eq(",".join(self.columns), self.value.sql())
        if self.where is not None:
            sql += self.where.sql()
        return sql


class Where(SQLExpression):
    """Represents a WHERE clause in an SQL statement."""

    def __init__(self, condition: SQLExpression):
        super().__init__(None)
        self.condition = condition

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f" WHERE {self.condition.sql()}"


class GroupBy(SQLExpression):
    """Represents a GROUP BY clause in an SQL statement."""

    def __init__(self, column_list: list[str]):
        super().__init__(None)
        self.column_list = column_list

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f"GROUP BY {', '.join(self.column_list)}"


class Having(SQLExpression):
    """Represents a HAVING clause in an SQL statement."""

    def __init__(self, condition: SQLExpression):
        super().__init__(None)
        self.condition = condition

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f" HAVING {self.condition.sql()}"


class SQLColumnDefinition(SQLExpression):
    """Represents the definition of a column in an SQL table."""

    type_map = {}
    constraint_map = {}

    def __init__(self, name: str, data_type: type, constraint: str = None):
        super().__init__(None)
        self.name = name
        if data_type in self.type_map:
            self.data_type = self.__class__.type_map[data_type]
        else:
            raise ValueError(
                f"Unsupported data type for a {self.__class__.__name__}: {data_type}"
            )
        if not constraint:
            self.constraint = ""
        elif constraint in self.constraint_map:
            self.constraint = self.__class__.constraint_map[constraint]
        else:
            raise ValueError(
                f"Unsupported column constraint for a {self.__class__.__name__}: {constraint}"
            )

    def sql(self) -> str:
        """Return the SQL expression as a string."""
        return f"{self.name} {self.data_type} {self.constraint}"
