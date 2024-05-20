"""Creates SQL statements"""

from enum import Enum, auto
from db.db_base import DB
from core.app_logging import getLogger
from db.SQLFactory import SQLFactory
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

    async def execute(self, params=None, close=False, commit=False):
        return await self.parent.execute(params=params, close=close, commit=commit)
    
    async def close(self):
        await self.parent.close()
    
    def get_sql_class(self, sql_cls:type)->type:
        return self.sqlFactory.get_sql_class(sql_cls)
    
    @property
    def sqlFactory(self)->SQLFactory:
        return self.parent.sqlFactory


class SQL(SQL_Executable):
    """Usage:
        sql = SQL_statement(db).create_table(
            "users",
            [
                ("name", SQL_data_type.TEXT),
                ("age",  SQL_data_type.INTEGER)
            ],
        ).execute()"""

    def sql(self)->str:
        if self.sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return self.sql_statement.sql()

    @property
    def sqlFactory(self)->SQLFactory:
        return self.db.sqlFactory

    def __init__(self, db:DB):
        self.db = db
        self.rslt = None
        self.sql_statement = None
        self.sql_statment:'SQL_statement' = None

    def create_table(self, table:str, columns:list[(str, SQL_data_type)])->"Create_Table":
        create_table = self.get_sql_class(Create_Table)(table, columns, self)
        #create_table = Create_Table(table, columns, self)
        self.sql_statement = create_table
        return create_table

    def select(self, column_list:list[str] = [], distinct:bool=False)->"Select":
        select = self.get_sql_class(Select)(column_list, distinct, self)
        self.sql_statement = select
        return select

    def insert(self, table:str, columns:list[str] = [])->"Insert":
        insert = self.get_sql_class(Insert)(table, columns, parent=self)
        self.sql_statement = insert
        return insert

    def update(self, table:str)->"Update":
        update = self.get_sql_class(Update)(table, parent=self)
        self.sql_statement = update
        return update

    def script(self, script:str)->"SQL_script":
        self.sql_statement = self.get_sql_class(SQL_script)(script, self)
        return self.sql_statement

    async def execute(self, params=None, close=False, commit=False):
        if self.sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return await self.db.execute(self.sql(), params, close, commit)

    async def close(self):
        await self.db.close()


class SQL_statement(SQL_Executable):

    def __init__(self, parent:SQL_Executable = None):
        self.parent = parent

    def sql(self)->str:
        raise NotImplementedError("SQL_statement is an abstract class and should not be instantiated.")


class SQL_script(SQL_statement):
    def __init__(self, script:str, parent:SQL_Executable=None):
        self.script = script
        super().__init__(parent)

    def sql(self)->str:
        return self.script


class SQL_column_definition(SQL_statement):
    def __init__(self, name:str, data_type:type, constraint:str=None):
        self.name = name
        self.data_type = data_type
        self.constraint = constraint
        raise NotImplementedError("SQL_column_definition is an abstract class and should not be instantiated.")

    def sql(self)->str:
        return f"{self.name} {self.data_type} {self.constraint}"


class Create_Table(SQL_statement):
    def __init__(self, table:str='', columns:list[(str, SQL_data_type, str)]=[], parent:SQL_Executable=None):
        self.table = table
        super().__init__(parent)
        sQL_column_definition = self.sqlFactory.get_sql_class(SQL_column_definition)
        self.columns = [sQL_column_definition(name, data_type, constraint) for name, data_type,  constraint in columns]

    def column(self, name:str, data_type:SQL_data_type, constraint:str=None):
        self.columns.append(self.sqlFactory.get_sql_class(SQL_column_definition)(name, data_type, constraint))
        return self

    def sql(self)->str:
        if self.table is None or len(self.table) == 0:
            raise InvalidSQLStatementException("CREATE TABLE statement must have a table name.")
        return f"CREATE TABLE {self.table} ({[column.sql() for column in self.columns]})"


class join_operator(Enum):
    INNER = "INNER JOIN"
    LEFT = "LEFT JOIN"
    RIGHT = "RIGHT JOIN"
    FULL = "FULL OUTER JOIN"


class From(SQL_statement):
    def __init__(self, table):
        self.table = table
        self.joins: List[(join_operator, Table_Valued_Query|str, SQL_expression)] = []

    def sql(self)->str:
        sql = ""
        if isinstance(self.table, Table_Valued_Query):
            sql = f" FROM ({self.table.sql()})"
        else:
            sql = f" FROM {self.table}"
            if(len(self.joins) > 0):
                sql += f" ".join(["{join[0]} {join[1]} ON {join[2].sql()}" for join in self.joins])
        return sql
    
    def join(
            self,
            table = None,
            join_constraint: "SQL_expression" = None,
            join_operator:join_operator = join_operator.FULL
        ):
        self.joins.append((join_operator, table, join_constraint))


class SQL_expression():
    def __init__(self, expression:str):
        if expression is None:
            expression = 'Null'
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


class Value(SQL_expression):
    def _init__(self, value:str):
        self.value = value

    def sql(self)->str:
        return self.value


class Row(SQL_expression):
    def __init__(self, values:list[Value]):
        self.values = values

    def value(self, value:Value):
        self.values.append(value)
        return self

    def sql(self)->str:
        return f"({', '.join([value.sql() for value in self.values])})"


class Values(SQL_expression):
    def __init__(self, rows:list[Row]):
        self.rows = rows

    def row(self, value:Row):
        self.rows.append(value)
        return self

    def sql(self)->str:
        return f"VALUES {', '.join([row.sql() for row in self.rows])}"


class Assignment(SQL_expression):
    def __init__(self, columns:list[str] | str, value:Value, ):
        if type(columns) == str:
            columns = [columns]
        self.columns = columns
        self.value = value
        self.where:Where = None

    def sql(self)->str:
        sql = "{','.join([f'{column}' for column in self.columns])} = {self.value.sql()}"
        if self.where is not None:
            sql += self.where.sql()
        return sql


class Where(SQL_expression):
    def __init__(self, condition:SQL_expression):
        self.condition = condition

    def sql(self)->str:
        return f" WHERE {self.condition.sql()}"


class Group_By(SQL_expression):
    def __init__(self, column_list:list[str]):
        self.column_list = column_list

    def sql(self)->str:
        " GROUP BY {', '.join(self.column_list)}"


class Having(SQL_expression):
    def __init__(self, condition:SQL_expression):
        self.condition = condition

    def sql(self)->str:
        return f" HAVING {self.condition.sql()}"


class Table_Valued_Query(SQL_statement):

    def __init__(self, parent:SQL_Executable):
        super().__init__(parent)

    def sql(self)->str:
        raise NotImplementedError("Table_Valued_Query is an abstract class and should not be instantiated.")


class Select(Table_Valued_Query):
    def __init__(self, column_list:list[str] = [], distinct:bool=False, parent:SQL_Executable=None):
        self.column_list = column_list
        self.distinct = distinct
        self.from_statement:Table_Valued_Query = None
        self.where: Where = None
        self.group_by: Group_By = None
        self.having: Having = None
        super().__init__(parent)

    def sql(self)->str:
        if self.from_statement is None:
            raise InvalidSQLStatementException("SELECT statement must have a FROM clause.")
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
    
    def Distinct(self, distinct:bool=True):
        self.distinct = distinct
        return self

    def Columns(self, column_list:list[str]):
        self.column_list = column_list
        return self

    def From(self, table:str|Table_Valued_Query):
        from_table = self.sqlFactory.get_sql_class(From)(table)
        self.from_statement = from_table
        return self

    def Where(self, condition:SQL_expression):
        where = self.sqlFactory.get_sql_class(Where)(condition)
        self.where = where
        return self

    def Having(self, condition:SQL_expression):
        having = self.sqlFactory.get_sql_class(Having)(condition)
        self.having = having
        return self


class Insert(SQL_statement):
    def __init__(self, table:str, columns:list[str] = [], rows = None, parent:SQL_Executable=None):
        self.table = table
        self.columns = columns
        self.rows = rows
        super().__init__(parent)
        self._return_str:str = ""

    def sql(self)->str:
        sql = f"INSERT INTO {self.table} ({', '.join(self.columns)}) {self.rows.sql()}"
        return sql + self._return_str
    
    def column(self, column:str):
        self.columns.append(column)
        return self
    
    def values(self, values):
        self.rows = values
        return self

    def returning(self, column:str):
        self._return_str = f" RETURNING {column}"
        return self


class Update(SQL_statement):
    def __init__(self, table:str, parent:SQL_Executable=None):
        self.table = table
        self.assignments:List[Assignment] = []
        super().__init__(parent)

    def assignment(self, columns:list[str] | str, value:Value):
        self.assignments.append(self.sqlFactory.get_sql_class(Assignment)(columns, value))
        return self
    
    def Where(self, condition:SQL_expression):
        where:Where = self.sqlFactory.get_sql_class(Where)(condition)
        self.where = where
        return self

    def returning(self, column:str):
        self.sql += f" RETURNING {column}"
        return self
    
    def sql(self)->str:
        sql = f"UPDATE {self.table} SET {', '.join([assignment.sql() for assignment in self.assignments])}"

        return sql