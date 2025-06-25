import asyncio
from calendar import c
from core.app import App

from persistance.bo_descriptors import BOColumnFlag

from database.dbms.sqlite import SQLiteDB
from database.sql import SQLConnection
from database.sql_statement import SQLTemplate
from database.sql_clause import Values


async def cleanup(sql):
    print("Removing all tables...")
    for table in [
        t["table_name"]
        for t in await (await sql.script(SQLTemplate.TABLELIST).execute()).fetchall()
    ]:
        print(f'      Dropping table "{table}"...')
        await sql.script(f'DROP TABLE IF EXISTS "{table}"').execute()
    print("Removing all views...")
    for view in [
        v["view_name"]
        for v in await (await sql.script(SQLTemplate.VIEWLIST).execute()).fetchall()
    ]:
        print(f'      Dropping view "{view}"...')
        await sql.script(f'DROP VIEW IF EXISTS "{view}"').execute()


async def main():
    print("Starting playing with DB...")
    cfg = {
        "db_cfg": {
            "db": "SQLite",
            "file": "C:\\ProgramData\\moneypilot\\money_pilotplayground.sqlite.db",
        }
    }
    App.db = SQLiteDB(**cfg["db_cfg"])
    async with SQLConnection() as conn:
        async with conn.transaction() as trx:
            async with trx.sql() as sql:
                await cleanup(sql)
                await sql.create_view("sql_schema").from_("sqlite_schema").execute()

                # Create a table
                await sql.create_table(
                    "test_table",
                    [
                        ("id", int, BOColumnFlag.BOC_PK_INC, {}),
                        ("name", str, BOColumnFlag.BOC_NONE, {}),
                        ("value", float, BOColumnFlag.BOC_NONE, {}),
                    ],
                ).execute()
                print("Created table 'test_table'.")
                print(f"    SQL: {sql.get_sql()}")
                await sql.insert(
                    "test_table",
                    [
                        [("name", "Test1"), ("value", 123.45)],
                        [("name", "Test2"), ("value", 987.65)],
                    ],
                ).execute()

                await sql.create_view("test_view").from_("test_table").execute()
                print("Created view 'test_view'.")
                print(f"    SQL: {sql.get_sql()}")
                await sql.create_view("test_temp_view", temporary=True).from_(
                    "test_table"
                ).execute()
                print("Created view 'test_temp_view'.")
                print(f"    SQL: {sql.get_sql()}")

                print(
                    f'    Tables in DB: {', '.join([t["table_name"] for t in await (await sql.script(SQLTemplate.TABLELIST).execute()).fetchall()])}'
                )
                print(
                    f'    Views in DB: {', '.join([v["view_name"] for v in await (await sql.script(SQLTemplate.VIEWLIST).execute()).fetchall()])}'
                )
                print(
                    await (
                        await sql.select().from_("test_temp_view").execute()
                    ).fetchall()
                )
                print(f"    SQL: {sql.get_sql()}")


if __name__ == "__main__":
    App.initialize(__file__)
    asyncio.run(main())
