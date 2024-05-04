"""Creates SQL statements"""

from enum import Enum, auto
from db.db_base import DB
from core.app_logging import getLogger
from typing import List
from enum import Enum

LOG = getLogger(__name__)


class SQL_data_type(Enum):
    TEXT = auto()
    INTEGER = auto()
    REAL = auto()
    BLOB = auto()


class InvalidSQLStatementException(Exception):
    """
    Exception raised when an invalid SQL statement is encountered.
    """
    pass


class SQL_Executable(object):
    parent:'SQL_Executable' = None

    def execute(self, params=None, close=False, commit=False):
        return self.parent.execute(params=params, close=close, commit=commit)

    def close(self):
        return self.parent.close()


class SQL(SQL_Executable):
    """Usage:
        sql = SQL_statement(db).create_table(
            "users",
            [
                ("name", SQL_data_type.TEXT),
                ("age",  SQL_data_type.INTEGER)
            ],
        ).execute()"""
    @property
    def sql(self)->str:
        return self.sql_statement.sql()
    
    rslt = None

    db: DB = None
    sql_statement:'SQL_statement' = None

    def __init__(self, db:DB):
        self.db = db

    def create_table(self, table:str, columns:list[(str, SQL_data_type)]):
        create_table = Create_Table(table, columns, self)
        self.sql_statement = create_table
        return self

    def select(self, column_list:list[str] = [], distinct:bool=False):
        select = Select(column_list, distinct)
        self.sql_statement = select
        return select
    
    def insert(self, table:str, columns:list[str] = []):
        insert = Insert(table, columns)
        self.sql_statement = insert
        return insert

    def execute(self, params=None, close=False, commit=False):
        self.rslt = self.db.execute(self.sql(), params, close, commit)
        return self

    def close(self):
        self.db.close()


class SQL_statement(SQL_Executable):

    def sql()->str:
        raise NotImplementedError("SQL_statement is an abstract class and should not be instantiated.")


class SQL_column_definition():
    def __init__(self, name:str, data_type:SQL_data_type):
        self.name = name
        self.data_type = data_type

    def sql(self)->str:
        return f"{self.name} {self.data_type.name}"


class Create_Table(SQL_statement):
    def __init__(self, table:str='', columns:list[(str, SQL_data_type)]=[], parent:SQL_Executable=None):
        self.parent = parent
        self.table = table
        self.columns = [SQL_column_definition(name, data_type) for name, data_type in columns]
        self.parent = parent

    def sql(self)->str:
        return f"CREATE TABLE {self.table} ({[column.sql() for column in self.columns]})"


class Table_Valued_Query(SQL_statement):
    def sql(self)->str:
        raise NotImplementedError("Table_Valued_Query is an abstract class and should not be instantiated.")


class join_operator(Enum):
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class From(SQL_statement):
    def __init__(self, table:Table_Valued_Query|str, parent:'Select'=None):
        self.parent = parent
        self.table = table
        self.joins = List[(join_operator, Table_Valued_Query|str, SQL_expression)]

    def sql(self)->str:
        sql = ""
        if isinstance(self.table, Table_Valued_Query):
            sql = f" FROM ({self.table.sql()})"
        else:
            sql = f" FROM {self.table}"
        " ".join(["{join[0]} {join[1]} ON {join[2].sql()}" for join in self.joins])
        return sql
    
    def join(
            self,
            table: Table_Valued_Query|str = None,
            join_constraint: "SQL_expression" = None,
            join_operator:join_operator = join_operator.FULL
        ):
        self.joins.append((join_operator, table, join_constraint))


class SQL_expression(SQL_statement):
    def __init__(self, expression:str):
        self.expression = expression

    def sql(self)->str:
        return self.expression


class SQL_multi_expression(SQL_expression):
    def __init__(self, arguments:List[SQL_expression]):
        self.arguments = arguments

    operator:str=None

    def sql(self)->str:
        if self.operator is None:
            raise NotImplementedError("SQL_multi_expression is an abstract class and should not be instantiated.")
        return self.operator.join([expression.sql() for expression in self.expression])


class AND(SQL_multi_expression):
    operator = 'AND'


class OR(SQL_multi_expression):
    operator = 'OR'


class SQL_binary_expression(SQL_expression):
    def __init__(self, left:SQL_expression|str, right:SQL_expression|str):
        self.left = left if isinstance(left, SQL_expression) else SQL_expression(left)
        self.right = right if isinstance(right, SQL_expression) else SQL_expression(right)

    operator = ''

    def sql(self)->str:
        if self.operator is None:
            raise NotImplementedError("SQL_binary_expression is an abstract class and should not be instantiated.")
        return f" ({self.left.sql()} {self.operator} {self.right.sql()}) "


class eq(SQL_binary_expression):
    operator = '='


class SQL_ternary_expression(SQL_expression):
    def __init__(self, first:SQL_expression|str, second:SQL_expression|str, third:SQL_expression|str):
        self.first = first if isinstance(first, SQL_expression) else SQL_expression(first)
        self.second = second if isinstance(second, SQL_expression) else SQL_expression(second)
        self.third = third if isinstance(third, SQL_expression) else SQL_expression(third)

    operator_one = None
    operator_two = None

    def sql(self)->str:
        if self.operator_none is None or self.operator_two is None:
            raise NotImplementedError("SQL_binary_expression is an abstract class and should not be instantiated.")
        return f" ({self.first.sql()} {self.operator_one} {self.second.sql()} {self.operator_two} {self.third.sql()}) "


class SQL_between(SQL_ternary_expression):
    operator_one = 'BETWEEN'
    operator_two = 'AND'


class Where(SQL_statement):
    def __init__(self, condition:SQL_expression):
        self.condition = condition

    def sql(self)->str:
        return f" WHERE {self.condition.sql()}"


class Group_By(SQL_statement):
    def __init__(self, column_list:list[str]):
        self.column_list = column_list

    def sql(self)->str:
        " GROUP BY {', '.join(self.column_list)}"
    

class Having(SQL_statement):
    def __init__(self, condition:SQL_expression):
        self.condition = condition

    def sql(self)->str:
        return f" HAVING {self.condition.sql()}"


class Select(Table_Valued_Query):
    def __init__(self, column_list:list[str] = [], distinct:bool=False):
        self.column_list = column_list
        self.distinct = distinct

    from_statement: Table_Valued_Query = None
    where: Where = None
    group_by: Group_By = None
    having: Having = None

    def sql(self)->str:
        if self.from_statement is None:
            raise InvalidSQLStatementException("SELECT statement must have a FROM clause.")
        if self.column_list is None or len(self.column_list) == 0:
            raise InvalidSQLStatementException("SELECT statement must have at least one column.")
        sql = f"SELECT {'DISTINCT' if self.distinct else ''} {', '.join(self.column_list)}"
        sql += " FROM " + self.from_statement.sql()
        if self.where is not None:
            sql += " WHERE " + self.where.sql()
        if self.group_by is not None:
            sql += " GROUP BY " + self.group_by.sql()
        if self.having is not None:
            sql += " HAVING " + self.having.sql()
        return sql
    
    def Distinct(self, distinct:bool=True):
        self.distinct = distinct
        return self

    def Columns(self, column_list:list[str]):
        self.column_list = column_list
        return self

    def From(self, table:str|Table_Valued_Query):
        from_table = From(table)
        self.from_statement = from_table
        return self

    def Where(self, condition:SQL_expression):
        where = Where(condition)
        self.where = where
        return self

    def Having(self, condition:SQL_expression):
        having = Having(condition)
        self.having = having
        return self


class Value(SQL_statement):
    def _init__(self, value:str):
        self.value = value

    def sql(self)->str:
        return self.value


class Row(SQL_statement):
    def __init__(self, values:list[Value]):
        self.values = values

    def sql(self)->str:
        return f"({', '.join([value.sql() for value in self.values])})"


class Values(SQL_statement):
    def __init__(self, rows:list[Row]):
        self.rows = rows

    def sql(self)->str:
        return f"VALUES {', '.join([row.sql() for row in self.rows])}"


class Insert(SQL_statement):
    def __init__(self, table:str, columns:list[str] = [], rows:Values | Select = None):
        self.table = table
        self.columns = columns
        self.rows = rows

    def sql(self)->str:
        return f"INSERT INTO {self.table} ({', '.join(self.columns)}) {self.rows.sql()}"

    def values(self, values: Values | Select):
        self.rows = values
        return self

    def returning(self, column:str):
        self.sql += f" RETURNING {column}"
        return self
