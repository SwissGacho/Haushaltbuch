from backend.backend.src.db.SQLFactory import SQLFactory


class InvalidSQLStatementException(Exception):
    """
    Exception raised when an invalid SQL statement is encountered.
    """


class SQLDataType(Enum):
    TEXT = auto()
    INTEGER = auto()
    REAL = auto()
    BLOB = auto()


class SQLExecutable(object):
    parent: "SQLExecutable" = None

    async def execute(
        self,
        params=None,
        close=False,
        commit=False,
    ):
        return await self.parent.execute(params=params, close=close, commit=commit)

    async def close(self):
        await self.parent.close()

    def get_sql_class(self, sql_cls: type) -> type:
        return self.sqlFactory.get_sql_class(sql_cls)

    @property
    def sqlFactory(self) -> SQLFactory:
        return self.parent.sqlFactory


class SQL(SQLExecutable):
    """Usage:
    sql = SQL_statement(db).create_table(
        "users",
        [
            ("name", SQL_data_type.TEXT),
            ("age",  SQL_data_type.INTEGER)
        ],
    ).execute()"""

    def sql(self) -> str:
        if self.sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return self.sql_statement.sql()

    @property
    def sqlFactory(self) -> SQLFactory:
        return self.db.sqlFactory

    def __init__(self, db: DB):
        self.db = db
        self.rslt = None
        self.sql_statement = None
        self.sql_statment: "SQL_statement" = None

    def create_table(
        self, table: str, columns: list[(str, SQLDataType)] = None
    ) -> "Create_Table":
        """Sets the SQL statement to create a table and returns a create_table object"""
        create_table = self.get_sql_class(Create_Table)(table, columns, self)
        # create_table = Create_Table(table, columns, self)
        self.sql_statement = create_table
        return create_table

    def select(self, column_list: list[str] = None, distinct: bool = False) -> "Select":
        """Sets the SQL statement to a select statement and returns a select object"""
        select = self.get_sql_class(Select)(column_list, distinct, self)
        self.sql_statement = select
        return select

    def insert(self, table: str, columns: list[str] = None) -> "Insert":
        """Sets the SQL statement to a insert statement and returns an insert object"""
        insert = self.get_sql_class(Insert)(table, columns, parent=self)
        self.sql_statement = insert
        return insert

    def update(self, table: str) -> "Update":
        update = self.get_sql_class(Update)(table, parent=self)
        self.sql_statement = update
        return update

    def script(self, script: str) -> "SQL_script":
        self.sql_statement = self.get_sql_class(SQL_script)(script, self)
        return self.sql_statement

    async def execute(self, params=None, close=False, commit=False):
        if self.sql_statement is None:
            raise InvalidSQLStatementException("No SQL statement to execute.")
        return await self.db.execute(self.sql(), params, close, commit)

    async def close(self):
        await self.db.close()
