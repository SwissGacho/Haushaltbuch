"""Connection to MySQL DB using asyncmy"""

import ssl
import datetime
from typing import Optional, Self
from pathlib import Path

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from core.configuration.config import Config, DBConfig
from core.exceptions import ConfigurationError, OperationalError
from core.util import get_config_item
from database.dbms.db_base import DB, Connection, Cursor, DBCursorProtocol
from database.sql_factory import SQLFactory
from database.sql import SQL
from database.sql_statement import SQLTemplate, SQLScript, Insert, Update
from database.sql_clause import SQLColumnDefinition
from business_objects.bo_descriptors import BOColumnFlag, BOBaseBase
from business_objects.business_attribute_base import BaseFlag


try:
    import asyncmy
    import asyncmy.errors
    from asyncmy.cursors import DictCursor
    from asyncmy.pool import Pool
except ModuleNotFoundError as err:
    asyncmy = None  # pylint: disable=invalid-name
    DictCursor: Optional[DBCursorProtocol] = None
    ASYNCMY_IMPORT_ERROR = err
else:
    ASYNCMY_IMPORT_ERROR = None


class MySQLFactory(SQLFactory):
    """DB specific SQL factory for MySQL"""

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


def baseflag_datatype(data_type: type, **args) -> str:
    "Return the datatype for a BaseFlag column"
    if data_type is not BaseFlag:
        raise ValueError(f"baseflag_datatype called with invalid type {data_type}")

    flag_type = args.get("flag_type")
    if not flag_type or not isinstance(flag_type, type):
        raise ValueError(f"baseflag_datatype called without valid flag_type: {args}")
    if not issubclass(flag_type, BaseFlag):
        raise ValueError(
            f"baseflag_datatype flag_type must be a BaseFlag subclass: {flag_type}"
        )

    flags = ",".join(
        [
            f"'{v.name.lower()}'"
            for v in list(flag_type)
            if v is not None and hasattr(v, "name") and v.name is not None
        ]
    )
    return f"SET ({flags})"


class MySQLColumnDefinition(SQLColumnDefinition):
    """Definition of a column in a CREATE TABLE statement for MySQL/MariaDB"""

    type_map = {
        int: "INT",
        float: "DOUBLE",
        str: "VARCHAR(100)",
        datetime.datetime: "DATETIME",
        dict: MYSQL_JSON_TYPE,
        list: MYSQL_JSON_TYPE,
        BOBaseBase: "INT",
        BaseFlag: baseflag_datatype,
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
    """MySQL specific SQL scripts"""

    sql_templates = {
        SQLTemplate.TABLELIST: """ SELECT table_name FROM information_schema.tables
                                    WHERE table_schema = DATABASE()
                                """,
        SQLTemplate.TABLEINFO: """ SELECT columns.column_name AS name,
                                CONCAT_WS(' ',
                                    columns.COLUMN_NAME,
                                    CASE WHEN SUBSTR(constraints.CHECK_CLAUSE, 1, 4) = 'json' THEN 'JSON'
                                        WHEN columns.DATA_TYPE IN ( 'varchar', 'bit' ) THEN UPPER(columns.COLUMN_TYPE)
                                        WHEN columns.DATA_TYPE = 'set' THEN CONCAT('SET ', SUBSTR(columns.COLUMN_TYPE,4))
                                        ELSE UPPER(columns.DATA_TYPE) END,
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
    """MySQL specific INSERT statement"""

    def returning(self, column: str):
        db_type = get_config_item(DBConfig.db_configuration, Config.CONFIG_DB_DB)
        LOG.debug(f"MySQLInsert.returning({column=});  {db_type=}")
        if db_type == "MySQL":
            raise NotImplementedError(
                "MySQL does not support returning values from INSERT statements."
            )
        return super().returning(column)


class MySQLUpdate(Update):
    """MySQL specific UPDATE statement"""

    def returning(self, column: str):
        db_type = get_config_item(DBConfig.db_configuration, Config.CONFIG_DB_DB)
        if db_type == "MySQL":
            raise NotImplementedError(
                "MySQL does not support returning values from UPDATE statements."
            )
        return super().returning(column)


class MySQLDB(DB):
    "DBMS class for MySQL/MariaDB databases"

    def __init__(self, **cfg) -> None:
        if ASYNCMY_IMPORT_ERROR:
            raise ModuleNotFoundError(f"Import error: {ASYNCMY_IMPORT_ERROR}")
        super().__init__(**cfg)
        self.connection_pool: Optional["Pool"] = None

    async def create_pool(self) -> None:
        """Create connection pool if not exists"""
        if self.connection_pool is not None:
            return

        ssl_cfg = self._cfg.get(Config.CONFIG_DBSSL)
        ssl_ctx: Optional[ssl.SSLContext] = None

        if ssl_cfg and isinstance(ssl_cfg, dict):
            cert_file: Optional[str] = ssl_cfg.get(Config.CONFIG_DBSSL_CERT)
            key_file: Optional[str] = ssl_cfg.get(Config.CONFIG_DBSSL_KEY)
            if not cert_file or not key_file:
                raise ConfigurationError(
                    "SSL configuration is incomplete. Both certificate and key files are required."
                )
            cert_path = Path(cert_file)
            key_path = Path(key_file)
            if not cert_path.exists() or not key_path.exists():
                raise ConfigurationError(
                    f"SSL certificate files not found: cert={cert_file}, key={key_file}"
                )

            ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            ssl_ctx.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))

        if asyncmy is None:
            raise ImportError("asyncmy module is not available.")

        self.connection_pool = await asyncmy.create_pool(
            host=self._cfg[Config.CONFIG_DBHOST],
            db=self._cfg[Config.CONFIG_DBDBNAME],
            port=self._cfg.get(Config.CONFIG_DBPORT, 3306),
            user=self._cfg.get(Config.CONFIG_DBUSER),
            password=self._cfg.get(Config.CONFIG_DBPW),
            ssl=ssl_ctx,
            minsize=1,
            maxsize=50,
            echo=False,
            pool_recycle=3600,  # Recycle connections after 1 hour
        )

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
        "Get a connection from pool"
        await self.create_pool()
        if self.connection_pool is None:
            raise OperationalError("Connection pool not initialized")
        return await MySQLConnection(db_obj=self, pool=self.connection_pool).connect()

    async def close(self):
        "Close the connection pool"
        if self.connection_pool:
            LOG.debug("MySQLDB.close(): Closing connection pool")
            # pylint: disable=protected-access
            LOG.debug(
                f"   {self.connection_pool.freesize} free, {len(self.connection_pool._used)} used, "
                f"{self.connection_pool.size} total, "
                f"{self.connection_pool.maxsize} possible connections"
            )
            self.connection_pool.close()
            await self.connection_pool.wait_closed()
            self.connection_pool = None


class MySQLConnection(Connection):
    "Connection to the MySQL DB using connection pool"

    _version_checked = False

    def __init__(self, db_obj: MySQLDB, pool: "Pool", **cfg) -> None:
        super().__init__(db_obj=db_obj, **cfg)
        self._pool = pool

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

    async def connect(self) -> Self:
        "Get connection from pool"
        # LOG.debug("MySQLConnection.connect()")
        if asyncmy is None:
            raise ImportError("asyncmy module is not available.")

        # LOG.debug(
        #     f"   {self._pool.freesize} free, {len(self._pool._used)} used, "
        #     f"{self._pool.size} total, "
        #     f"{self._pool.maxsize} possible connections"
        # )
        self._connection = await self._pool.acquire()
        # LOG.debug(
        #     f"   {self._pool.freesize} free, {len(self._pool._used)} used, "
        #     f"{self._pool.size} total, "
        #     f"{self._pool.maxsize} possible connections"
        # )
        if not MySQLConnection._version_checked:
            await self._check_db_version()
        return self

    async def close(self):
        "Return connection to pool"
        if self._connection:
            # LOG.debug("Connection.close: releasing connection")
            await self._pool.release(self._connection)
            self._db.db_connections.remove(self)
            self._connection = None
            # LOG.debug(f"------------------------------- {self._db._connections=}")

    async def execute(self, query: str, params=None) -> "MySQLCursor":
        "execute an SQL statement and return a cursor"
        if self._connection is None:
            raise OperationalError("Cannot execute query - connection is closed")
        if asyncmy is None or DictCursor is None:
            raise ImportError("asyncmy module is not available")

        cur = MySQLCursor(cur=self._connection.cursor(DictCursor), con=self)
        await cur.execute(query, params=params)
        return cur


class MySQLCursor(Cursor):
    "Cursor for MySQL/MariaDB DB"

    async def execute(self, query: str, params=None) -> Self:
        "Execute an SQL statement"
        # LOG.debug(f"MySQLCursor.execute({query=}, {params=})")
        if asyncmy is None:
            raise ImportError("asyncmy module is not available.")
        conv_sql, args = self.convert_params_named_2_format(
            query, params or {}, dump_json=True
        )

        if self._cursor is None:
            raise OperationalError(
                "Cursor is not initialized. "
                "Make sure to create the cursor before executing queries."
            )
        try:
            # LOG.debug(f"MySQLCursor.execute: {conv_sql=}, {args=}")
            self._rowcount = await self._cursor.execute(conv_sql, args=args)
        except (
            asyncmy.errors.MySQLError  # pylint: disable=c-extension-no-member
        ) as exc:
            raise OperationalError(f"{exc} during SQL execution") from exc
        return self


log_exit(LOG)
