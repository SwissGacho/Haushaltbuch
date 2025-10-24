"""Classes for building SQL expressions that can be used in SQLStatements."""

from typing import Any, Optional
import re


from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from database.sql_key_manager import SQLKeyManager, SQL_Dict


class SQLExpression:
    """Base class for an SQL expression.
    Can be instantiated directly with an str argument to create an expression verbatim
    from a string. Instantiated with any other type will create a SQL expression with
    the value of the type.
    """

    def __init__(self, expression: str | Any = ""):
        self._expression: str | SQL_Dict = expression or "Null"

    def get_query(self, km: SQLKeyManager) -> str:
        """Get the SQL query for this expression."""
        if isinstance(self._expression, str):
            return self._expression
        return km.merge_params(query=":param", params={"param": self._expression})


class SQLMultiExpression(SQLExpression):
    """Abstract class to combine any number of SQL expressions with an operator.
    Should not be instantiated directly."""

    def __init__(self, arguments: list[SQLExpression | str]):
        super().__init__()
        self._arguments: list[SQLExpression] = [
            arg if isinstance(arg, SQLExpression) else SQLExpression(arg)
            for arg in arguments
        ]

    operator: str = ""

    def get_query(self, km: SQLKeyManager) -> str:
        if not self.__class__.operator:
            raise NotImplementedError(
                "SQL_multi_expression is an abstract class and should not be instantiated."
            )
        if not self._arguments:
            raise ValueError(
                f"SQL_multi_expression must have at least one argument, got {len(self._arguments)}"
            )
        return (
            "("
            + re.sub(
                "  +",
                " ",
                f" {self.__class__.operator} ".join(
                    [expression.get_query(km=km) for expression in self._arguments]
                ),
            ).strip()
            + ")"
        )


class And(SQLMultiExpression):
    """Represents a SQL AND expression."""

    operator = "AND"


class Or(SQLMultiExpression):
    """Represents a SQL OR expression."""

    operator = "OR"


class SQLUnaryExpression(SQLMultiExpression):
    """Abstract class to combine exactly one SQL expression with an operator.
    Should not be instantiated directly."""

    left_operator: str = ""
    right_operator: str = ""
    operator: str = ""

    def __init__(self, expression: SQLExpression | str):
        if self.__class__.left_operator and self.__class__.right_operator:
            raise ValueError(
                "SQLUnaryExpression cannot have both left and right operators."
            )
        if self.__class__.left_operator:
            self.__class__.operator = self.__class__.left_operator
            super().__init__([" ", expression])
        if self.__class__.right_operator:
            self.__class__.operator = self.__class__.right_operator
            super().__init__([expression, " "])
        else:
            raise NotImplementedError(
                "SQLUnaryExpression must have either left or right operator."
            )


class Not(SQLUnaryExpression):
    """Represents a SQL NOT expression."""

    left_operator = "NOT"


class IsNull(SQLUnaryExpression):
    "Represents test for NULL"

    right_operator = "IS NULL"


class SQLBinaryExpression(SQLMultiExpression):
    """Abstract class to combine exactly two SQL expressions with an operator.
    Should not be instantiated directly."""

    def __init__(
        self,
        left: SQLExpression | str,
        right: SQLExpression | str,
    ):
        super().__init__([left, right])


class Eq(SQLBinaryExpression):
    """Represents a SQL = expression."""

    operator = "="


class Is(SQLBinaryExpression):
    """Represents a SQL 'is' expression."""

    operator = "IS"


class ColumnName(SQLExpression):
    """Represents a column name"""

    def __init__(self, name: str):
        super().__init__(None)
        self._name = name

    def get_query(self, km: SQLKeyManager) -> str:
        return self._name


class SQLString(SQLExpression):
    """Represents a string value"""

    def __init__(self, value: str):
        super().__init__()
        self._value = value

    def get_query(self, km: SQLKeyManager) -> str:
        return f"'{self._value}'"


class Filter(And):
    """Represent a filter condition matching all items.
    Keys and values of 'filter' are rendered in quotes if they have the class str.
    SQLExpressions are rendered according to their class.
    (Use ColumnName() to avoid rendering in quotes)
    """

    def __init__(self, filters: dict[SQLExpression, SQLExpression]):
        def _expression(value: Any) -> SQLExpression:
            if isinstance(value, str):
                return SQLString(value)
            if isinstance(value, SQLExpression):
                return value
            return SQLExpression(value)

        super().__init__(
            [
                (
                    IsNull(_expression(var))
                    if val is None
                    else Eq(_expression(var), _expression(val))
                )
                for var, val in filters.items()
            ]
        )


class SQLTernaryExpression(SQLExpression):
    """Abstract class to combine exactly three SQL expressions with two operators.
    Should not be instantiated directly."""

    def __init__(
        self,
        first: SQLExpression,
        second: SQLExpression,
        third: SQLExpression,
    ):
        super().__init__()
        self.first = first if isinstance(first, SQLExpression) else SQLExpression(first)
        self.second = (
            second if isinstance(second, SQLExpression) else SQLExpression(second)
        )
        self.third = third if isinstance(third, SQLExpression) else SQLExpression(third)

    operator_one = None
    operator_two = None

    def get_query(self, km: SQLKeyManager) -> str:
        return " ".join(
            [
                "(",
                f"{self.first.get_query(km=km)}",
                f"{self.__class__.operator_one}",
                f"{self.second.get_query(km=km)}",
                f"{self.__class__.operator_two}",
                f"{self.third.get_query(km=km)}",
                ")",
            ]
        )


class SQLBetween(SQLTernaryExpression):
    """Represents a SQL BETWEEN expression."""

    operator_one = "BETWEEN"
    operator_two = "AND"


class Value(SQLExpression):
    """Represents a value in an SQL statement."""

    def __init__(self, *args, **kwargs):
        """Initialize a value with a name and a value.
        Arguments named 'name' and 'value' are used to set the name and value of the value.
        If only one positional argument is provided, it is treated as the value.
        If only two positional arguments are provided, the first is treated as the name and
        the second as the value.
        """

        name: str = str(kwargs.get("name", ""))
        value: Any = kwargs.get("value", None)
        if len(args) == 1:
            if not value:
                value = args[0]
            else:
                name = args[0]
        elif len(args) == 2:
            name, value = args
        if not name:
            name = "param"
        if not value:
            raise ValueError("Value must be provided")
        if not isinstance(name, str):
            raise ValueError("Name must be a string")
        # LOG.debug(f"Value({name=}, {value=})")
        super().__init__()
        self._name: str = name
        # if not isinstance(value, (str, int, float)):
        #     raise ValueError("Value must be a string, int or float")
        self._value: Any = value

    def get_name(self) -> str:
        "Name of the value"
        return self._name

    def get_query(self, km: SQLKeyManager) -> str:
        return km.merge_params(query=":" + self._name, params={self._name: self._value})


class Row(SQLExpression):
    """Represents a list of values defining a row in an SQL statement such as an INSERT."""

    def __init__(self, values: Optional[list[Value]] = None):
        # LOG.debug(f"Row({values=})")
        super().__init__()
        self._values: list[Value] = values or []

    def value(self, value: Value):
        """Add a value to the end of the row."""
        self._values.append(value)
        return self

    def get_names(self) -> str:
        "List of value names"
        return f"({', '.join([v.get_name() for v in self._values])})"

    def get_query(self, km: SQLKeyManager) -> str:
        """Return the SQL expression as a string."""
        return f"({', '.join([v.get_query(km=km) for v in self._values])})"


log_exit(LOG)
