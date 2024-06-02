"""Creates SQL statements"""

from enum import Enum
from typing import List

from db.SQLExecutable import SQLExecutable
from core.app_logging import getLogger

LOG = getLogger(__name__)


class join_operator(Enum):
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class From(SQL_statement):
    def __init__(self, table):
        self.table = table
        self.joins: List[(join_operator, Table_Valued_Query | str, SQL_expression)] = []

    def sql(self) -> str:
        sql = ""
        if isinstance(self.table, Table_Valued_Query):
            sql = f" FROM ({self.table.sql()})"
        else:
            sql = f" FROM {self.table}"
            if len(self.joins) > 0:
                sql += f" ".join(
                    ["{join[0]} {join[1]} ON {join[2].sql()}" for join in self.joins]
                )
        return sql

    def join(
        self,
        table=None,
        join_constraint: "SQL_expression" = None,
        join_operator: join_operator = join_operator.FULL,
    ):
        self.joins.append((join_operator, table, join_constraint))


class SQL_expression:
    def __init__(self, expression: str):
        expression = "Null" if expression is None else expression
        self.expression = expression

    def sql(self) -> str:
        return self.expression


class SQL_multi_expression(SQL_expression):
    def __init__(self, arguments: List[SQL_expression]):
        self.arguments = arguments

    operator: str = None

    def sql(self) -> str:
        if self.operator is None:
            raise NotImplementedError(
                "SQL_multi_expression is an abstract class and should not be instantiated."
            )
        return self.operator.join([expression.sql() for expression in self.expression])


class AND(SQL_multi_expression):
    operator = "AND"


class OR(SQL_multi_expression):
    operator = "OR"


class SQL_binary_expression(SQL_expression):
    def __init__(self, left: SQL_expression | str, right: SQL_expression | str):
        self.left = left if isinstance(left, SQL_expression) else SQL_expression(left)
        self.right = (
            right if isinstance(right, SQL_expression) else SQL_expression(right)
        )

    operator = None

    def sql(self) -> str:
        if self.operator is None:
            raise NotImplementedError(
                "SQL_binary_expression is an abstract class and should not be instantiated."
            )
        return f" ({self.left.sql()} {self.__class__.operator} {self.right.sql()}) "


class eq(SQL_binary_expression):
    operator = "="


class SQL_ternary_expression(SQL_expression):

    def __init__(
        self,
        first: SQL_expression | str,
        second: SQL_expression | str,
        third: SQL_expression | str,
    ):
        self.first = (
            first if isinstance(first, SQL_expression) else SQL_expression(first)
        )
        self.second = (
            second if isinstance(second, SQL_expression) else SQL_expression(second)
        )
        self.third = (
            third if isinstance(third, SQL_expression) else SQL_expression(third)
        )

    operator_one = None
    operator_two = None

    def sql(self) -> str:
        print("""__CLASS.__NAME___""")
        print(self.__class__.operator_one)
        if self.__class__.operator_one is None or self.__class__.operator_two is None:
            raise NotImplementedError(
                "SQL_binary_expression is an abstract class and should not be instantiated."
            )
        return f" ({self.first.sql()} {self.__class__.operator_one} {self.second.sql()} {self.__class__.operator_two} {self.third.sql()}) "


class SQL_between(SQL_ternary_expression):
    operator_one = "BETWEEN"
    operator_two = "AND"


class Value(SQL_expression):
    def _init__(self, value: str):
        self.value = value

    def sql(self) -> str:
        return self.value


class Row(SQL_expression):
    def __init__(self, values: list[Value]):
        self.values = values

    def value(self, value: Value):
        self.values.append(value)
        return self

    def sql(self) -> str:
        return f"({', '.join([value.sql() for value in self.values])})"


class Values(SQL_expression):
    def __init__(self, rows: list[Row]):
        self.rows = rows

    def row(self, value: Row):
        self.rows.append(value)
        return self

    def sql(self) -> str:
        return f"VALUES {', '.join([row.sql() for row in self.rows])}"


class Assignment(SQL_expression):
    def __init__(
        self,
        columns: list[str] | str,
        value: Value,
    ):
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns
        self.value = value
        self.where: Where = None

    def sql(self) -> str:
        sql = f"{','.join(self.columns)} = {self.value.sql()}"
        if self.where is not None:
            sql += self.where.sql()
        return sql


class Where(SQL_expression):
    def __init__(self, condition: SQL_expression):
        self.condition = condition

    def sql(self) -> str:
        return f" WHERE {self.condition.sql()}"


class Group_By(SQL_expression):
    def __init__(self, column_list: list[str]):
        self.column_list = column_list

    def sql(self) -> str:
        "GROUP BY {', '.join(self.column_list)}"


class Having(SQL_expression):
    def __init__(self, condition: SQL_expression):
        self.condition = condition

    def sql(self) -> str:
        return f" HAVING {self.condition.sql()}"
