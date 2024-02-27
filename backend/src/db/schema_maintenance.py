""" Manage DB schema versins and check compatibility """

import core
import db
import persistance

# import data.management
from data.management.db_schema import DB_Schema
from core.app_logging import getLogger

LOG = getLogger(__name__)

CURRENT_DB_SCHEMA_VERSION = 1
COMPATIBLE_DB_SCHEMA_VERSIONS = [1]


async def create_all_tables(db, objects):
    for bo in objects:
        LOG.info(f"creating table '{bo.table}'")
        await bo.sql_create_table()


async def upgrade_db_schema(db, from_version: int, to_version: int, objects):
    LOG.debug(f"upgrade from {from_version} to {to_version}")
    if from_version is None:
        await create_all_tables(db, objects)
        return


async def check_db_schema():
    """check the version of the DB
    upgrade if necessary
    verify compatibility of the persistance tables
    """
    database = core.app.App.db
    if database.__class__ == db.db_base.DB:
        raise TypeError("cannot check abstract DB")
    # cur = await database.execute(database.sql(query=db.sql.SQL.TABLE_LIST), close=True)
    # num_tables = await cur.rowcount
    # LOG.debug(f"Found {num_tables} tables in DB:")
    # tables = await cur.fetchall()
    # LOG.debug(f"{tables=}")

    all_business_objects = (
        persistance.business_object_base.BO_Base.all_business_objects.values()
    )
    try:
        db_schema = await DB_Schema().fetch(newest=True)
    except core.exceptions.OperationalError as err:
        db_schema = DB_Schema()
    except Exception as err:
        LOG.error(
            f"An error occurred fetching DB schema version in check_db_schema(): {err}"
        )
        db_schema = DB_Schema()
    if (
        db_schema.version_nr is None
        or db_schema.version_nr < CURRENT_DB_SCHEMA_VERSION
        or db_schema.version_nr not in COMPATIBLE_DB_SCHEMA_VERSIONS
    ):
        await upgrade_db_schema(
            database,
            db_schema.version_nr,
            CURRENT_DB_SCHEMA_VERSION,
            all_business_objects,
        )
        upgraded = True
    else:
        upgraded = False

    ok = True
    for bo in all_business_objects:
        ok = await database.check_table(bo) and ok
    if not ok:
        raise TypeError("DB schema not compatible")
    if upgraded:
        await DB_Schema(v_nr=CURRENT_DB_SCHEMA_VERSION).store()