"""Creates SQL statements"""

from enum import Enum, auto
from typing import List
from backend.backend.src.db.SQLExecutable import SQLExecutable
from db.db_base import DB
from core.app_logging import getLogger
from db.SQLFactory import SQLFactory

LOG = getLogger(__name__)


class SQL_statement(SQLExecutable):

    def __init__(self, parent: SQLExecutable = None):
        self.parent = parent

    def sql(self) -> str:
        raise NotImplementedError(
            "SQL_statement is an abstract class and should not be instantiated."
        )


class SQL_script(SQL_statement):
    def __init__(self, script: str, parent: SQLExecutable = None):
        self.script = script
        super().__init__(parent)

    def sql(self) -> str:
        return self.script


class SQL_column_definition(SQL_statement):

    type_map = {}

    def __init__(self, name: str, data_type: type, constraint: str = None):
        self.name = name
        if data_type in self.type_map:
            self.data_type = self.__class__.type_map[data_type]
        else:
            raise ValueError(
                f"Unsupported data type for a {self.__class__.__name__}: {type}"
            )
        self.constraint = constraint

    def sql(self) -> str:
        return f"{self.name} {self.data_type} {self.constraint}"


class Create_Table(SQL_statement):
    def __init__(
        self,
        table: str = "",
        columns: list[(str, SQLDataType, str)] = [],
        parent: SQLExecutable = None,
    ):
        self.table = table
        super().__init__(parent)
        sQL_column_definition = self.sqlFactory.get_sql_class(SQL_column_definition)
        self.columns = [
            sQL_column_definition(name, data_type, constraint)
            for name, data_type, constraint in columns
        ]

    def column(self, name: str, data_type: SQLDataType, constraint: str = None):
        self.columns.append(
            self.sqlFactory.get_sql_class(SQL_column_definition)(
                name, data_type, constraint
            )
        )
        return self

    def sql(self) -> str:
        if self.table is None or len(self.table) == 0:
            raise InvalidSQLStatementException(
                "CREATE TABLE statement must have a table name."
            )
        return (
            f"CREATE TABLE {self.table} ({[column.sql() for column in self.columns]})"
        )


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


class Table_Valued_Query(SQL_statement):

    def __init__(self, parent: SQLExecutable):
        super().__init__(parent)

    def sql(self) -> str:
        raise NotImplementedError(
            "Table_Valued_Query is an abstract class and should not be instantiated."
        )


class Select(Table_Valued_Query):
    def __init__(
        self,
        column_list: list[str] = None,
        distinct: bool = False,
        parent: SQLExecutable = None,
    ):
        column_list = [] if column_list is None else column_list
        self.column_list = column_list
        self.distinct = distinct
        self.from_statement: Table_Valued_Query = None
        self.where: Where = None
        self.group_by: Group_By = None
        self.having: Having = None
        super().__init__(parent)

    def sql(self) -> str:
        if self.from_statement is None:
            raise InvalidSQLStatementException(
                "SELECT statement must have a FROM clause."
            )
        sql = f"SELECT {'DISTINCT ' if self.distinct else ''}{', '.join(self.column_list)}"
        if self.column_list is None or len(self.column_list) == 0:
            sql += "*"
        sql += self.from_statement.sql()
        if self.where is not None:
            sql += self.where.sql()
        if self.group_by is not None:
            sql += self.group_by.sql()
        if self.having is not None:
            sql += self.having.sql()
        return sql

    def Distinct(self, distinct: bool = True):
        self.distinct = distinct
        return self

    def Columns(self, column_list: list[str]):
        self.column_list = column_list
        return self

    def From(self, table: str | Table_Valued_Query):
        from_table = self.sqlFactory.get_sql_class(From)(table)
        self.from_statement = from_table
        return self

    def Where(self, condition: SQL_expression):
        where = self.sqlFactory.get_sql_class(Where)(condition)
        self.where = where
        return self

    def Having(self, condition: SQL_expression):
        having = self.sqlFactory.get_sql_class(Having)(condition)
        self.having = having
        return self


class Insert(SQL_statement):
    def __init__(
        self,
        table: str,
        columns: list[str] = [],
        rows=None,
        parent: SQLExecutable = None,
    ):
        self.table = table
        self.columns = columns
        self.rows = rows
        super().__init__(parent)
        self._return_str: str = ""

    def sql(self) -> str:
        sql = f"INSERT INTO {self.table} ({', '.join(self.columns)}) {self.rows.sql()}"
        return sql + self._return_str

    def single_row(self, row: list[(str, str | Value)]):
        row = self.get_sql_class(Row)()
        for column_name, value in row:
            self.column(column_name)
            row.value(value)
        self.rows = self.get_sql_class(Values)([row])
        return self

    def column(self, column: str):
        self.columns.append(column)
        return self

    def values(self, values):
        self.rows = values
        return self

    def returning(self, column: str):
        self._return_str = f" RETURNING {column}"
        return self


class Update(SQL_statement):
    def __init__(self, table: str, parent: SQLExecutable = None):
        self.table = table
        self.assignments: List[Assignment] = []
        super().__init__(parent)

    def assignment(self, columns: list[str] | str, value: Value):
        self.assignments.append(
            self.sqlFactory.get_sql_class(Assignment)(columns, value)
        )
        return self

    def Where(self, condition: SQL_expression):
        where: Where = self.sqlFactory.get_sql_class(Where)(condition)
        self.where = where
        return self

    def returning(self, column: str):
        self.sql += f" RETURNING {column}"
        return self

    def sql(self) -> str:
        sql = f"UPDATE {self.table} SET {', '.join([assignment.sql() for assignment in self.assignments])}"

        return sql
