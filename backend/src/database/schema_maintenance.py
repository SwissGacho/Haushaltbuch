"""Manage DB schema versins and check compatibility"""

import core
import database
import database.db_manager
import database.dbms.db_base
import persistance

# import data.management
from data.management.db_schema import DBSchema
from core.app_logging import getLogger
from core.exceptions import DBSchemaError
from database.sql_statement import SQL, SQLTemplate
import persistance.business_object_base

LOG = getLogger(__name__)

CURRENT_DB_SCHEMA_VERSION = 1
COMPATIBLE_DB_SCHEMA_VERSIONS = [1]


async def _create_all_tables(objects: list[persistance.business_object_base.BOBase]):
    for bo in objects:
        LOG.info(f"creating table '{bo.table}' for business class '{bo.__name__}'")
        await bo.sql_create_table()


async def upgrade_db_schema(
    from_version: int,
    to_version: int,
    objects: list[persistance.business_object_base.BOBase],
):
    "Apply changes to the DB schema"
    LOG.debug(f"upgrade from {from_version} to {to_version}")
    if from_version is None:
        await _create_all_tables(objects)
        return


async def check_db_schema():
    """check the version of the DB
    upgrade if necessary
    verify compatibility of the persistance tables
    """
    this_database = core.app.App.db
    if this_database.__class__ == database.dbms.db_base.DB:
        raise TypeError("cannot check abstract DB")
    LOG.debug("checking DB Schema")
    cur = await SQL().script(SQLTemplate.TABLELIST).execute()
    table_count = await cur.rowcount
    # if table_count < 1:
    #     raise DBSchemaError("No tables in DB")
    LOG.debug(f"Found {table_count} tables in DB:")
    if table_count > 0:
        LOG.debug(
            f"    tables: {', '.join([t['table_name'] for t in await cur.fetchall()])}"
        )

        try:
            db_schema = await DBSchema().fetch(newest=True)
        except core.exceptions.OperationalError:
            db_schema = DBSchema()
        except Exception as exc:  # pylint: disable=broad-exception-caught
            LOG.error(
                f"An error occurred fetching DB schema version in check_db_schema(): {exc}"
            )
            db_schema = DBSchema()
    else:
        db_schema = DBSchema()
    all_business_objects = (
        persistance.business_object_base.BOBase.all_business_objects.values()
    )
    if (
        db_schema.version_nr is None
        or db_schema.version_nr < CURRENT_DB_SCHEMA_VERSION
        or db_schema.version_nr not in COMPATIBLE_DB_SCHEMA_VERSIONS
    ):
        await upgrade_db_schema(
            db_schema.version_nr,
            CURRENT_DB_SCHEMA_VERSION,
            all_business_objects,
        )
        upgraded = True
    else:
        upgraded = False

    ok = True
    for bo in all_business_objects:
        ok = await this_database.check_table(bo) and ok
    if not ok:
        raise DBSchemaError("DB schema not compatible")
    if upgraded:
        await DBSchema(version_nr=CURRENT_DB_SCHEMA_VERSION).store()
