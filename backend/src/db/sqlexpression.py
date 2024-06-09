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
        self._params: dict[str, str] = {}

    def get_params(self):
        """Return a dictionary holding the parameters for the expression."""
        return self._params

    def get_sql(self):
        """Return the parametrized SQL string of the expression."""
        return self._expression

    def sql(self):
        """Return the SQL expression as a string combined with its parameters."""
        return (self.get_sql(), self.get_params())


class From(SQLExpression):
    """Class for the FROM clause of an SQL statement."""

    def __init__(self, table):
        super().__init__(None)
        self._table = table
        self._joins: List[tuple[JoinOperator, str, SQLExpression]] = []

    def get_params(self) -> dict:
        return {k: v for join in self._joins for k, v in join[2].get_params().items()}

    def get_sql(self) -> str:
        sql = f" FROM {self._table} "
        if len(self._joins) > 0:
            sql += " ".join(
                [f"{join[0]} {join[1]} ON {join[2].sql()}" for join in self._joins]
            )
        return sql

    def join(
        self,
        table=None,
        join_constraint: "SQLExpression" = None,
        join_operator: JoinOperator = JoinOperator.FULL,
    ):
        """Add a join to another table to the FROM clause."""
        self._joins.append((join_operator, table, join_constraint))


class SQLMultiExpression(SQLExpression):
    """Abstract class to combine any number of SQL expressions with an operator.
    Should not be instantiated directly."""

    def __init__(self, arguments: List[SQLExpression]):
        super().__init__(None)
        self._arguments = arguments

    operator: str = None

    def get_params(self):
        if self.__class__.operator is None:
            raise NotImplementedError(
                "SQL_multi_expression is an abstract class and should not be instantiated."
            )
        return {
            k: v
            for expression in self._arguments
            for k, v in expression.get_params().items()
        }

    def get_sql(self):
        if self.__class__.operator is None:
            raise NotImplementedError(
                "SQL_multi_expression is an abstract class and should not be instantiated."
            )
        return self.__class__.operator.join(
            [expression.get_sql() for expression in self._arguments]
        )


class And(SQLMultiExpression):
    """Represents a SQL AND expression."""

    operator = " AND "


class Or(SQLMultiExpression):
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

    def get_params(self):
        return {**self.left.get_params(), **self.right.get_params()}

    def get_sql(self):
        return f" ({self.left.get_sql()} {self.__class__.operator} {self.right.get_sql()}) "

    def sql(self):
        """Return the SQL expression as a string."""
        if self.__class__.operator is None:
            raise NotImplementedError(
                "SQL_binary_expression is an abstract class and should not be instantiated."
            )
        return super().sql()


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

    def get_params(self):
        return {
            **self.first.get_params(),
            **self.second.get_params(),
            **self.third.get_params(),
        }

    def get_sql(self):
        return (
            f" ({self.first.sql()} {self.__class__.operator_one} "
            f"{self.second.sql()} {self.__class__.operator_two} {self.third.sql()}) "
        )

    def sql(self):
        """Return the SQL expression as a string."""
        if self.__class__.operator_one is None or self.__class__.operator_two is None:
            raise NotImplementedError(
                "SQL_binary_expression is an abstract class and should not be instantiated."
            )
        return super().sql()


class SQLBetween(SQLTernaryExpression):
    """Represents a SQL BETWEEN expression."""

    operator_one = " BETWEEN "
    operator_two = " AND "


class Value(SQLExpression):
    """Represents a value in an SQL statement."""

    def __init__(self, value: str, key: str = ""):
        super().__init__(None)
        if key in self.__class__._keys or key == "":
            key += self.__class__.generate_key()
        self.__class__._keys |= {key}
        self._value = value
        self._key = key if key is not None else self.__class__.generate_key()

    _last_key = 0
    _keys: set[str] = set()

    @classmethod
    def generate_key(cls):
        """Generate a new unique id for a value"""
        cls._last_key += 1
        return str(cls._last_key)

    def get_params(self):
        """Get a dictionary of the value to be used in an SQL cursor"""
        return {self._key: self._value}

    def sql(self):
        """Return the SQL expression as parametrized string."""
        return (":" + self._key, self.get_params())


class Row(SQLExpression):
    """Represents a list of values defining a row in an SQL statement such as an INSERT."""

    def __init__(self, values: list[Value]):
        super().__init__(None)
        self._values = values

    def value(self, value: Value):
        """Add a value to the end of the row."""
        self._values.append(value)
        return self

    def get_params(self):
        return {k: v for value in self._values for k, v in value.get_params().items()}

    def sql(self):
        """Return the SQL expression as a string."""
        return (
            f" ({', '.join([value.sql()[0] for value in self._values])}) ",
            self.get_params(),
        )


class Values(SQLExpression):
    """Represents a list of rows in an SQL statement such as an INSERT."""

    def __init__(self, rows: list[Row]):
        super().__init__(None)
        self._rows = rows

    def row(self, value: Row):
        """Add a row to the end of the list."""
        self._rows.append(value)
        return self

    def get_params(self):
        value_dict: dict[str, str] = {}
        for row in self._rows:
            value_dict.update(row.get_params())
        return value_dict

    def sql(self):
        """Return the SQL expression as a string."""
        return (
            f"VALUES {', '.join([row.sql()[0] for row in self._rows])}",
            self.get_params(),
        )


class Assignment(SQLExpression):
    """Represents an assignment in an SQL statement such as an UPDATE."""

    def __init__(
        self,
        columns: list[str] | str,
        value: Value,
    ):
        super().__init__(None)
        self._columns = [columns] if isinstance(columns, str) else columns
        self._value = value
        self._where: Where = None

    def get_params(self):
        value_dict = self._value.get_params()
        if self._where is not None:
            value_dict.update(self._where.get_params())
        return value_dict

    def get_sql(self):
        sql = Eq(",".join(self._columns), self._value.sql()[0]).get_sql()
        if self._where is not None:
            sql += self._where.get_sql()
        return sql


class Where(SQLExpression):
    """Represents a WHERE clause in an SQL statement."""

    def __init__(self, condition: SQLExpression):
        super().__init__(None)
        self._condition = condition

    def get_params(self):
        return self._condition.get_params()

    def get_sql(self):
        return f" WHERE {self._condition.get_sql()}"


class GroupBy(SQLExpression):
    """Represents a GROUP BY clause in an SQL statement."""

    def __init__(self, column_list: list[str]):
        super().__init__(None)
        self.column_list = column_list

    def get_sql(self):
        return f"GROUP BY {', '.join(self.column_list)}"


class Having(SQLExpression):
    """Represents a HAVING clause in an SQL statement."""

    def __init__(self, condition: SQLExpression):
        super().__init__(None)
        self._condition = condition

    def get_params(self):
        return self._condition.get_params()

    def get_sql(self):
        return f" HAVING {self._condition.get_sql()}"


class SQLColumnDefinition(SQLExpression):
    """Represents the definition of a column in an SQL table."""

    type_map = {}

    def __init__(self, name: str, data_type: type, constraint: str = None):
        super().__init__(None)
        self.name = name
        if data_type in self.type_map:
            self.data_type = self.__class__.type_map[data_type]
        else:
            raise ValueError(
                f"Unsupported data type for a {self.__class__.__name__}: {data_type}"
            )
        self._constraint = constraint

    def get_sql(self):
        return f"{self.name} {self.data_type} {self._constraint}"
