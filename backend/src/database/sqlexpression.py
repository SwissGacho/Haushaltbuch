"""Classes for building SQL expressions that can be used in SQLStatements."""

from enum import Enum, StrEnum, Flag
from typing import TypeAlias
import json

from core.app_logging import getLogger
from database.sqlkeymanager import SQLKeyManager

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

    def __init__(self, expression: str = None, key_manager: SQLKeyManager = None):
        self._key_manager = key_manager
        self._expression = "Null" if expression is None else expression
        self._params: dict[str, str] = {}

    def create_param(self, key: str, value):
        if self._key_manager is None:
            raise ValueError("No key manager provided to create a parameter.")
        key = self._key_manager.register_key(key)
        self._params[key] = value
        return key

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

    def __init__(self, table, key_manager: SQLKeyManager = None):
        super().__init__(key_manager=key_manager)
        self._table = table
        self._joins: list[tuple[JoinOperator, str, SQLExpression]] = []

    def get_params(self) -> dict:
        return {
            k: v 
            for join in self._joins 
            if join[2] is not None
            for k, v in join[2].get_params().items()
        }
    
    def get_sql(self) -> str:
        sql = f" FROM {self._table} "
        if len(self._joins) > 0:
            sql += " ".join([f"{join[0].value} {join[1]} {"ON "+join[2].get_sql() if join[2] is not None else ""}" for join in self._joins])
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

    def __init__(
        self, arguments: list[SQLExpression], key_manager: SQLKeyManager = None
    ):
        super().__init__(key_manager=key_manager)
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

    def __init__(
        self,
        left: SQLExpression | str,
        right: SQLExpression | str,
        key_manager: SQLKeyManager = None,
    ):
        super().__init__(key_manager=key_manager)
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


class Is(SQLBinaryExpression):
    """Represents a SQL 'is' expression."""

    operator = " is "


class IsNull(Is):
    "Represents test for NULL"

    def __init__(self, left: SQLExpression | str):
        super().__init__(left, right=None)


class ColumnName(SQLExpression):
    """Represents a column name"""

    def __init__(self, name: str):
        super().__init__(None)
        self._name = name

    def sql(self) -> str:
        return self._name


class SQLString(SQLExpression):
    """Represents a string value"""

    def __init__(self, value: str):
        super().__init__(None)
        self._value = value

    def sql(self) -> str:
        return f"'{self._value}'"


FilterItem: TypeAlias = SQLExpression | str


class Filter(And):
    """Represent a filter condition matching all items.
    Keys and values of 'filter' are rendered in quotes if they have the class str.
    SQLExpressions are rendered according to their class.
    (Use ColumnName() to avoid rendering in quotes)
    """

    def __init__(self, filters: dict[FilterItem, FilterItem]):
        super().__init__(
            [
                (
                    Eq(
                        var if isinstance(var, SQLExpression) else SQLString(var),
                        val if isinstance(val, SQLExpression) else SQLString(val),
                    )
                    if val is not None
                    else IsNull(
                        var if isinstance(var, SQLExpression) else SQLString(var)
                    )
                )
                for var, val in filters.items()
            ]
        )


class SQLTernaryExpression(SQLExpression):
    """Abstract class to combine exactly three SQL expressions with two operators.
    Should not be instantiated directly."""

    def __init__(
        self,
        first: SQLExpression | str,
        second: SQLExpression | str,
        third: SQLExpression | str,
        key_manager: SQLKeyManager = None,
    ):
        super().__init__(key_manager=key_manager)
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

    ##### Need to add support to have lists and dictionaries as values that are serialized as json #####
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

    def __init__(
        self, name: str, value: any, key_manager: SQLKeyManager, key: str = ""
    ):
        # LOG.debug(f"Value({name=}, {value=})")
        super().__init__(key_manager=key_manager)
        key = self.create_param(key, value)
        self._key = key
        self._name = name
        self._value = value

    def name(self) -> str:
        "Name of the value"
        return self._name

    def get_params(self):
        """Get a dictionary of the value to be used in an SQL cursor"""
        value_string = ""
        if isinstance(self._value, (str, StrEnum, Flag)):
            value_string = f"'{self._value}'"
        if isinstance(self._value, dict | list):
            value_string = f"""'{json.dumps(self._value, separators=(",", ":"))}'"""
        else:
            value_string = str(self._value)
        return {self._key: value_string}

    def get_sql(self):
        return ":" + self._key


class Row(SQLExpression):
    """Represents a list of values defining a row in an SQL statement such as an INSERT."""

    def __init__(self, values: list[Value] = None, key_manager: SQLKeyManager = None):
        # LOG.debug(f"Row({values=})")
        super().__init__(key_manager=key_manager)
        self._values = [] if values is None else values

    def value(self, value: Value):
        """Add a value to the end of the row."""
        self._values.append(value)
        return self

    def names(self) -> str:
        "List of value names"
        return f"({', '.join([v.name() for v in self._values])})"

    def get_params(self):
        return {k: v for value in self._values for k, v in value.get_params().items()}

    def get_sql(self):
        """Return the SQL expression as a string."""
        return f"({', '.join([v.get_sql() for v in self._values])})"


class Values(SQLExpression):
    """Represents a list of rows in an SQL statement such as an INSERT."""

    def __init__(self, rows: list[Row], key_manager: SQLKeyManager = None):
        # LOG.debug(f"Values({rows=})")
        super().__init__(key_manager=key_manager)
        self._rows = rows

    def row(self, value: Row):
        """Add a row to the end of the list."""
        self._rows.append(value)
        return self

    def names(self) -> str:
        "List of value names"
        if not self._rows:
            return ""
        return self._rows[0].names()

    def get_params(self):
        value_dict: dict[str, str] = {}
        for row in self._rows:
            value_dict.update(row.get_params())
        return value_dict

    def get_sql(self):
        """Return the SQL expression as a string."""
        return f"VALUES {', '.join([row.get_sql() for row in self._rows])}"


class Assignment(SQLExpression):
    """Represents an assignment in an SQL statement such as an UPDATE."""

    def __init__(
        self,
        columns: list[str] | str,
        value: Value,
        key_manager: SQLKeyManager = None,
    ):
        super().__init__(key_manager=key_manager)
        self._columns = [columns] if isinstance(columns, str) else columns
        self._value = value
        self._where: Where = None

    def get_params(self):
        value_dict = self._value.get_params()
        if self._where is not None:
            value_dict.update(self._where.get_params())
        return value_dict

    def get_sql(self):
        sql = "(" + ",".join(self._columns) + ")=" + self._value.get_sql()
        if self._where is not None:
            sql += self._where.get_sql()
        return sql


class Where(SQLExpression):
    """Represents a WHERE clause in an SQL statement."""

    def __init__(self, condition: SQLExpression, key_manager: SQLKeyManager = None):
        super().__init__(key_manager=key_manager)
        self._condition = condition

    def get_params(self):
        return self._condition.get_params()

    def get_sql(self):
        return f" WHERE {self._condition.get_sql()}"


class GroupBy(SQLExpression):
    """Represents a GROUP BY clause in an SQL statement."""

    def __init__(self, column_list: list[str], key_manager: SQLKeyManager = None):
        super().__init__(key_manager=key_manager)
        self.column_list = column_list

    def get_sql(self):
        return f"GROUP BY {', '.join(self.column_list)}"


class Having(SQLExpression):
    """Represents a HAVING clause in an SQL statement."""

    def __init__(self, condition: SQLExpression, key_manager: SQLKeyManager = None):
        super().__init__(key_manager=key_manager)
        self._condition = condition

    def get_params(self):
        return self._condition.get_params()

    def get_sql(self):
        return f" HAVING {self._condition.get_sql()}"


class SQLColumnDefinition(SQLExpression):
    """Represents the definition of a column in an SQL table."""

    type_map = {}
    constraint_map = {}

    def __init__(
        self,
        name: str,
        data_type: type,
        constraint: str = None,
        key_manager: SQLKeyManager = None,
        **pars
    ):
        super().__init__(key_manager=key_manager)
        self._name = name
        if data_type in self.type_map:
            self._data_type = self.__class__.type_map[data_type]
        else:
            raise ValueError(
                f"Unsupported data type for a {self.__class__.__name__}: {data_type}"
            )
        if not constraint:
            self._constraint = ""
        elif constraint in self.constraint_map:
            self._constraint = self.__class__.constraint_map[constraint].format(
                **{
                    k: v.table if hasattr(v, "table") else str(v).lower()
                    for k, v in pars.items()
                }
            )
            # LOG.debug(f" - constraint={self.constraint}")
        else:
            raise ValueError(
                f"Unsupported column constraint for a {self.__class__.__name__}: {constraint}"
            )

    def get_sql(self):
        return f"{self._name} {self._data_type} {self._constraint}"
