"""Connection to MySQL DB using aiomysql"""

from typing import Self
import datetime

from core.configuration.config import Config, DBConfig
from core.exceptions import ConfigurationError
from core.util import get_config_item
from database.dbms.db_base import DB, Connection, Cursor
from database.sql_factory import SQLFactory
from database.sql import SQL
from database.sql_statement import SQLTemplate, SQLScript, Insert, Update
from database.sql_clause import SQLColumnDefinition
from persistance.bo_descriptors import BOColumnFlag, BOBaseBase
from persistance.business_attribute_base import BaseFlag

from core.app_logging import getLogger

LOG = getLogger(__name__)

try:
    import aiomysql
except ModuleNotFoundError as err:
    AIOMYSQL_IMPORT_ERROR = err
else:
    AIOMYSQL_IMPORT_ERROR = None


class MySQLFactory(SQLFactory):

    @classmethod
    def get_sql_class(cls, sql_cls: type):
        # LOG.debug(f"MySQLFactory.get_sql_class({sql_cls=})")
        for mysql_class in [
            MySQLColumnDefinition,
            MySQLScript,
            MySQLInsert,
            MySQLUpdate,
        ]:
            if sql_cls.__name__ in [b.__name__ for b in mysql_class.__bases__]:
                return mysql_class
        return super().get_sql_class(sql_cls)


MYSQL_JSON_TYPE = "JSON"
MYSQL_BASEFLAG_TYPE = "BIT(64)"


class MySQLColumnDefinition(SQLColumnDefinition):

    type_map = {
        int: "INT",
        float: "DOUBLE",
        str: "VARCHAR(100)",
        datetime.datetime: "DATETIME",
        dict: MYSQL_JSON_TYPE,
        list: MYSQL_JSON_TYPE,
        BOBaseBase: "INT",
        BaseFlag: MYSQL_BASEFLAG_TYPE,
    }
    constraint_map = {
        BOColumnFlag.BOC_NONE: "",
        BOColumnFlag.BOC_NOT_NULL: "NOT NULL",
        BOColumnFlag.BOC_UNIQUE: "UNIQUE",
        BOColumnFlag.BOC_PK: "PRIMARY KEY",
        BOColumnFlag.BOC_PK_INC: "AUTO_INCREMENT PRIMARY KEY",
        BOColumnFlag.BOC_FK: "REFERENCES {relation} (id)",
        BOColumnFlag.BOC_DEFAULT: "DEFAULT",
        BOColumnFlag.BOC_DEFAULT_CURR: "DEFAULT CURRENT_TIMESTAMP",
        # BOColumnFlag.BOC_INC: "not available ! @%?°",
        # BOColumnFlag.BOC_CURRENT_TS: "not available ! @%?°",
    }


class MySQLScript(SQLScript):
    sql_templates = {
        SQLTemplate.TABLELIST: """ SELECT table_name FROM information_schema.tables
                                    WHERE table_schema = DATABASE()
                                """,
        SQLTemplate.TABLEINFO: """ SELECT columns.column_name AS name,
                                CONCAT_WS(' ',
                                    columns.COLUMN_NAME,
                                    UPPER(CASE WHEN SUBSTR(constraints.CHECK_CLAUSE, 1, 4) = 'json' THEN 'json'
                                        WHEN columns.DATA_TYPE IN ( 'varchar', 'bit' ) THEN columns.COLUMN_TYPE
                                        ELSE columns.DATA_TYPE END),
                                    UPPER(CASE WHEN columns.IS_NULLABLE <> 'YES' AND columns.COLUMN_KEY <> 'PRI' THEN 'NOT NULL' 
                                        ELSE NULL END),
                                    UPPER(CASE WHEN columns.EXTRA <> '' THEN columns.EXTRA ELSE NULL END),
                                    UPPER(CASE WHEN key_cols.CONSTRAINT_NAME = 'PRIMARY' THEN 'PRIMARY KEY' 
                                        ELSE NULL END),
                                    UPPER(CASE WHEN columns.COLUMN_DEFAULT IS NULL OR columns.COLUMN_DEFAULT = 'NULL' THEN NULL
                                        WHEN columns.COLUMN_DEFAULT = 'current_timestamp()' THEN 'default current_timestamp'
                                        ELSE CONCAT('default', columns.COLUMN_DEFAULT) END),
                                    CASE WHEN key_cols.REFERENCED_TABLE_NAME IS NOT NULL
                                        THEN CONCAT('REFERENCES ', key_cols.REFERENCED_TABLE_NAME, ' (', key_cols.REFERENCED_COLUMN_NAME, ')')
                                        ELSE NULL END
                                    ) AS column_info
                                FROM information_schema.columns columns
                                LEFT JOIN information_schema.check_constraints constraints 
                                    ON columns.TABLE_SCHEMA = constraints.CONSTRAINT_SCHEMA 
                                        AND columns.TABLE_NAME = constraints.TABLE_NAME 
                                        AND columns.COLUMN_NAME = constraints.CONSTRAINT_NAME
                                LEFT JOIN information_schema.key_column_usage key_cols
                                    ON columns.TABLE_SCHEMA = key_cols.TABLE_SCHEMA
                                        AND columns.TABLE_NAME = key_cols.TABLE_NAME
                                        AND columns.COLUMN_NAME = key_cols.COLUMN_NAME
                                WHERE columns.TABLE_NAME = :table
                                    AND columns.table_schema = DATABASE()
                            """,
        SQLTemplate.VIEWLIST: """ SELECT name as view_name FROM information_schema.views
                                    WHERE table_schema = DATABASE()
                                """,
        SQLTemplate.DBVERSION: """ SELECT VERSION() AS version """,
    }


class MySQLInsert(Insert):
    def returning(self, column: str):
        db_type = get_config_item(DBConfig.db_configuration, Config.CONFIG_DB_DB)
        LOG.debug(f"MySQLInsert.returning({column=});  {db_type=}")
        if db_type == "MySQL":
            raise NotImplementedError(
                "MySQL does not support returning values from INSERT statements."
            )
        return super().returning(column)


class MySQLUpdate(Update):
    def returning(self, column: str):
        db_type = get_config_item(DBConfig.db_configuration, Config.CONFIG_DB_DB)
        if db_type == "MySQL":
            raise NotImplementedError(
                "MySQL does not support returning values from UPDATE statements."
            )
        return super().returning(column)


def get_db(db_type: str = None, **cfg) -> DB:
    """Get a DB instance based on the db_type"""
    # LOG.debug(f"mysql.get_db({db_type=}, {cfg=})")
    if db_type in ["MySQL", "MariaDB"]:
        return MySQLDB(**cfg)


class MySQLDB(DB):
    def __init__(self, **cfg) -> None:
        if AIOMYSQL_IMPORT_ERROR:
            raise ModuleNotFoundError(f"Import error: {AIOMYSQL_IMPORT_ERROR}")
        super().__init__(**cfg)

    @property
    def sql_factory(self):
        return MySQLFactory

    async def _get_table_info(self, table_name: str) -> dict[str, str]:
        # LOG.debug(f"MySQLDB._get_table_info({table_name=})")
        async with SQL() as sql:
            table_info = await (
                await sql.script(SQLTemplate.TABLEINFO, table=table_name).execute()
            ).fetchall()
        # LOG.debug(f"MySQLDB._get_table_info({table_name=}) -> {table_info=}")
        return {columns["name"]: columns["column_info"] for columns in table_info}

    async def connect(self):
        "Open a connection"
        return await MySQLConnection(db_obj=self, **self._cfg).connect()


class MySQLConnection(Connection):
    "Connection to the MySQL DB"

    _version_checked = False

    async def _check_db_version(self):
        if MySQLConnection._version_checked:
            return
        async with SQL(connection=self) as sql:
            db_version = (
                await (await sql.script(SQLTemplate.DBVERSION).execute()).fetchone()
            )["version"]
            LOG.info(f"Connected to DB version {db_version}")
        if get_config_item(DBConfig.db_configuration, Config.CONFIG_DB_DB) == "MariaDB":
            if not "MariaDB" in db_version:
                raise ConfigurationError(
                    "Connected DB is not a MariaDB database."
                    " Consider changing the configuration to 'MySQL'."
                )
        else:
            if "MariaDB" in db_version:
                raise ConfigurationError(
                    "Connected DB is not a MySQL database."
                    " Consider changing the configuration to 'MariaDB'."
                )
        MySQLConnection._version_checked = True

    async def connect(self):
        # LOG.debug(f"MySQLConnection.connect({self._cfg=})")
        self._connection: aiomysql.Connection = await aiomysql.connect(
            host=self._cfg[Config.CONFIG_DBHOST],
            db=self._cfg[Config.CONFIG_DBDBNAME],
            user=self._cfg[Config.CONFIG_DBUSER],
            password=self._cfg[Config.CONFIG_DBPW],
        )
        await self._check_db_version()
        return self

    async def close(self):
        "close the connection"
        if self._connection:
            # LOG.debug("Connection.close: closing connection")
            self._connection.close()
            self._db._connections.remove(self)
            self._connection = None
            # LOG.debug(f"------------------------------- {self._db._connections=}")

    async def execute(self, query: str, params=None) -> "MySQLCursor":
        "execute an SQL statement and return a cursor"
        cur = MySQLCursor(
            cur=await self._connection.cursor(aiomysql.DictCursor), con=self
        )
        await cur.execute(query, params=params)
        return cur


class MySQLCursor(Cursor):

    async def execute(self, query: str, params=None):
        # LOG.debug(f"MySQLCursor.execute({query=}, {params=})")

        conv_sql, args = self.convert_params_named_2_format(
            query, params or {}, dump_json=True
        )
        # LOG.debug(f"MySQLCursor.execute: {conv_sql=}, {args=}")

        self._rowcount = await self._cursor.execute(conv_sql, args=args)
        return self
