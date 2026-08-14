import asyncio
import decimal
from encodings.punycode import T
import re
import sys
import os
import traceback
from pathlib import Path

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../../src")

from core.exceptions import OperationalError, RollBackRequested
from core.app import App
from core.configuration.file_config import FileConfig

from database.dbms.sqlite import SQLiteDB
from database.dbms.mysql import MySQLDB
from database.sql import SQL, SQLConnection, SQLTransaction
from business_objects.bo_descriptors import BOColumnConstraint

MARIADB_CONFIG = {
    "db": "MariaDB",
    "host": "db.gacho.duckdns.org",
    "port": 33306,
    "dbname": "quiffer",
    "dbuser": "quiffer",
    # "dbname": "heinz",
    # "dbuser": "heinz",
    "ssl": {
        "ssl_cert": os.path.dirname(os.path.abspath(__file__)) + "\\quiffer.cert.pem",
        "ssl_key": os.path.dirname(os.path.abspath(__file__)) + "\\quiffer.key.pem",
        # "ssl_cert": "C:\\Users\\heinz\\certs\\heinz\\heinz.cert.pem",
        # "ssl_key": "C:\\Users\\heinz\\certs\\heinz\\heinz.key.pem",
    },
}


def init_db():
    App.db = MySQLDB(**MARIADB_CONFIG)


tables = {
    "qif_categories": [
        ("id", int, BOColumnConstraint.BOC_PK_INC, {}),
        ("name", str, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("description", str, BOColumnConstraint.BOC_NONE, {}),
        ("type", str, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("parent_id", int, BOColumnConstraint.BOC_NONE, {}),
        ("source_file", str, BOColumnConstraint.BOC_NONE, {}),
    ],
    "qif_splits": [
        ("id", int, BOColumnConstraint.BOC_PK_INC, {}),
        ("duplicate", str, BOColumnConstraint.BOC_NONE, {}),
        ("transaction_id", int, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("amount", decimal.Decimal, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("memo", str, BOColumnConstraint.BOC_NONE, {}),
        ("category", int, BOColumnConstraint.BOC_NONE, {}),
        ("contra_account", int, BOColumnConstraint.BOC_NONE, {}),
        ("percent", decimal.Decimal, BOColumnConstraint.BOC_NONE, {}),
        ("contra_trans", int, BOColumnConstraint.BOC_NONE, {}),
        ("contra_split", int, BOColumnConstraint.BOC_NONE, {}),
        ("code", str, BOColumnConstraint.BOC_NONE, {}),
        ("edoc", str, BOColumnConstraint.BOC_NONE, {}),
        ("source_file", str, BOColumnConstraint.BOC_NONE, {}),
    ],
    "qif_transactions": [
        ("id", int, BOColumnConstraint.BOC_PK_INC, {}),
        ("duplicate", int, BOColumnConstraint.BOC_NONE, {}),
        ("date", str, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("account", int, BOColumnConstraint.BOC_NONE, {}),
        ("num_splits", int, BOColumnConstraint.BOC_NONE, {}),
        ("contra_account", int, BOColumnConstraint.BOC_NONE, {}),
        ("amount", decimal.Decimal, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("counterparty", str, BOColumnConstraint.BOC_NONE, {}),
        ("category", int, BOColumnConstraint.BOC_NONE, {}),
        ("memo", str, BOColumnConstraint.BOC_NONE, {}),
        ("contra_trans", int, BOColumnConstraint.BOC_NONE, {}),
        ("contra_split", int, BOColumnConstraint.BOC_NONE, {}),
        ("code", str, BOColumnConstraint.BOC_NONE, {}),
        ("edoc", str, BOColumnConstraint.BOC_NONE, {}),
        ("source_file", str, BOColumnConstraint.BOC_NONE, {}),
    ],
    "qif_investments": [
        ("id", int, BOColumnConstraint.BOC_PK_INC, {}),
        ("date", str, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("action", str, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("security", str, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("quantity", decimal.Decimal, BOColumnConstraint.BOC_NONE, {}),
        ("price", decimal.Decimal, BOColumnConstraint.BOC_NONE, {}),
        ("amount", decimal.Decimal, BOColumnConstraint.BOC_NONE, {}),
        ("memo", str, BOColumnConstraint.BOC_NONE, {}),
        ("cleared", str, BOColumnConstraint.BOC_NONE, {}),
        ("contra_account", int, BOColumnConstraint.BOC_NONE, {}),
        ("commission", decimal.Decimal, BOColumnConstraint.BOC_NONE, {}),
        ("source_file", str, BOColumnConstraint.BOC_NONE, {}),
    ],
    "qif_balances": [
        ("id", int, BOColumnConstraint.BOC_PK_INC, {}),
        ("account", int, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("date", str, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("amount", decimal.Decimal, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("source_file", str, BOColumnConstraint.BOC_NONE, {}),
    ],
    "qif_accounts": [
        ("id", int, BOColumnConstraint.BOC_PK_INC, {}),
        ("name", str, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("type", str, BOColumnConstraint.BOC_NOT_NULL, {}),
        ("source_file", str, BOColumnConstraint.BOC_NONE, {}),
    ],
}


async def create_table(tbl):
    async with SQL() as sql:
        sql.script(f"drop table if exists {tbl}")
        await sql.execute()
        sql.create_table(tbl, tables[tbl])
        await sql.execute()


async def create_tables():
    for tbl in tables:
        await create_table(tbl)


async def insert_data_into_db(table, data):
    if len(data[0]) != len(tables[table]):
        print(
            f"Data length {len(data[0])} does not match table definition length {len(tables[table])} for table {table}"
        )
        return
    query = f"INSERT INTO {table} ({', '.join([col[0] for col in tables[table]])}) VALUES ({', '.join(['%s' for _ in tables[table]])})"
    print(f"Query: {query}")
    async with SQLTransaction() as sqltx:
        sql = sqltx.sql()
        sql.script("TRUNCATE TABLE " + table)
        await sql.execute()
        cursor = sqltx._my_connection._connection.cursor()
        await cursor.executemany(query, data)
