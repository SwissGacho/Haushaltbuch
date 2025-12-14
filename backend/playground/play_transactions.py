import asyncio
from encodings.punycode import T
import re
import sys
import os
import traceback
from pathlib import Path

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from core.exceptions import OperationalError, RollBackRequested
from core.app import App
from core.configuration.db_config import DBConfig

from database.dbms.sqlite import SQLiteDB
from database.dbms.mysql import MySQLDB
from database.sql import SQL, SQLConnection, SQLTransaction

SQLiITE_CONFIG = {
    "db": "SQLite",
    "file": "playground.sqlite.db",
}
MARIADB_CONFIG = {
    "db": "MariaDB",
    "host": "db.gacho.duckdns.org",
    "port": 33306,
    "dbname": "playground",
    "dbuser": "playground",
    "ssl": {
        "ssl_cert": os.path.dirname(os.path.abspath(__file__))
        + "\\playground.cert.pem",
        "ssl_key": os.path.dirname(os.path.abspath(__file__)) + "\\playground.key.pem",
    },
}
SELECT = "select max(Column1) from :table"
SELECTALL = "select * from :table"
INSERT = "insert into :table (Column1) select COALESCE(MAX(Column1), 0) + 1 from :table returning Column1"
CLEAN = "delete from :table"
TABLES = ["toy_one", "toy_two", "toy_three"]

print_traceback = False
table = 1


def get_single_key():
    """Wait for a single key press and return it (cross-platform)."""
    print("->", end="", flush=True)
    try:
        # Windows
        import msvcrt

        ch = msvcrt.getch().decode("utf-8")
        print(ch)
        return ch
    except ImportError:
        import tty, termios

        fd = sys.stdin.fileno()
        old_settings = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        print(ch)
        return ch


def help():
    """Print help for the available commands."""
    print("Available commands:")
    print("  h: Help")
    print("  q: Quit")
    print("  1: Use table 'toy_one'")
    print("  2: Use table 'toy_two'")
    print("  3: Use table 'toy_three'")
    print("  k: Toggle 'keep connection' mode")
    print("  t: Toggle traceback printing")
    print("  b: Begin transaction")
    print("  c: Commit transaction")
    print("  r: Rollback transaction")
    print("  s: Select from table")
    print("  *: Select all from table")
    print("  i: Insert into table")
    print("  x: Cleanup (delete all entries in table)")


async def check_table(tab=1):
    async with SQL() as sql:
        try:
            res = await (
                await sql.script(f"select 1 from {TABLES[tab - 1]}").execute()
            ).fetchall()
            print(f"Table '{TABLES[tab-1]}' exists and is accessible: {res}")
        except Exception as e:
            print(f"Table '{TABLES[tab-1]}' does not exist or is not accessible: {e}")
            print("Creating table...")
            await sql.create_table(
                TABLES[tab - 1], [("Column1", int, None, {})]
            ).execute()


async def select(ctx, tab=1, all=False):
    async with ctx.sql() if ctx else SQL() as sql:
        sel = re.sub(":table", TABLES[tab - 1], SELECTALL if all else SELECT)
        print(f"Executing: {sel}")
        print(await (await sql.script(sel).execute()).fetchall())


async def insert(ctx, tab=1):
    async with ctx.sql() if ctx else SQL() as sql:
        ins = re.sub(":table", TABLES[tab - 1], INSERT)
        print(f"Trying: {ins}")
        try:
            print(f"Returning: {await (await sql.script(ins).execute()).fetchall()}")
        except OperationalError as e:
            print(f"-i-i-i-i-i-i-i-i-i-i-i-i-i-i-i-i-i-i-i> Error during insert: {e}")
            if print_traceback:
                traceback.print_exc()
            raise


async def cleanup(ctx, tab=1):
    """Cleanup the database by deleting all entries in table."""
    clean = re.sub(":table", TABLES[tab - 1], CLEAN)
    async with ctx.sql() if ctx else SQL() as sql:
        print(f"Executing: {clean}")
        await sql.script(clean).execute()
        print("Cleanup done.")


async def transaction(conn):
    global table
    try:
        async with conn.transaction() if conn else SQLTransaction() as trx:
            print(
                "_____________________________________________________________________________ Transaction started."
                f"{'' if conn else ' (in own connection)'}"
            )
            while True:
                key = await asyncio.get_event_loop().run_in_executor(
                    None, get_single_key
                )
                if key in ["q", "\x03"]:
                    print("Aborting transaction...")
                    raise KeyboardInterrupt
                elif key == "h":
                    help()
                elif key in ["1", "2", "3"]:
                    table = int(key)
                    print(f"Using table '{TABLES[table-1]}'")
                elif key == "c":
                    print("Exiting transaction...")
                    break
                elif key == "r":
                    print("Rolling back transaction...")
                    raise RollBackRequested
                elif key in ["s", "*"]:
                    await select(trx, table, all=(key == "*"))
                elif key == "i":
                    await insert(trx, table)
                else:
                    print(f"? ({key} unknown or not allowed in transaction)")
                    continue
    except KeyboardInterrupt:
        print("Transaction aborted by user.")
    except OperationalError as e:
        print(
            f"-t-t-t-t-t-t-t-t-t-t-t-t-t-t-t-t-t-t-t> '{e.__class__.__name__}' during transaction: {e}"
        )
        if print_traceback:
            traceback.print_exc()
    except RollBackRequested:
        print("Rolled back transaction")
    except Exception as e:
        print(
            f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!> Unexpected '{e.__class__.__name__}' exception during transaction: {e}"
        )
        if print_traceback:
            traceback.print_exc()
    finally:
        print(
            "_____________________________________________________________________________ Exiting transaction context."
        )


async def mainloop(conn):
    global print_traceback
    global table
    await check_table(1)
    await check_table(2)
    try:
        while True:
            try:
                key = await asyncio.get_event_loop().run_in_executor(
                    None, get_single_key
                )
                # print(f"\nKey pressed: {key}")
                if key in ["q", "\x03", "k"]:
                    break
                elif key == "h":
                    help()
                elif key in ["1", "2", "3"]:
                    table = int(key)
                    print(f"Using table '{TABLES[table-1]}'")
                elif key == "t":
                    print_traceback = not print_traceback
                    print(
                        f"Traceback printing is now {'enabled' if print_traceback else 'disabled'}."
                    )
                elif key == "b":
                    await transaction(conn)
                elif key == "c":
                    try:
                        await conn.commit()
                        print("Transaction committed.")
                    except OperationalError as e:
                        print(
                            f"--------------------------------------> '{e.__class__.__name__}' during commit: {e}"
                        )
                        if print_traceback:
                            traceback.print_exc()
                elif key == "r":
                    try:
                        await conn.rollback()
                        print("Transaction rolled back.")
                    except OperationalError as e:
                        print(
                            f"--------------------------------------> '{e.__class__.__name__}' during rollback: {e}"
                        )
                        if print_traceback:
                            traceback.print_exc()
                elif key in ["s", "*"]:
                    await select(conn, table, all=(key == "*"))
                elif key == "i":
                    await insert(conn, table)
                elif key == "x":
                    await cleanup(conn, table)
                    await conn.commit()
                else:
                    print(f"? ({key})")
            except OperationalError as e:
                print(
                    f"--------------------------------------> '{e.__class__.__name__}' during execution: {e}"
                )
                if print_traceback:
                    traceback.print_exc()
            except Exception as e:
                print(
                    f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!> Unexpected '{e.__class__.__name__}': {e}"
                )
                traceback.print_exc()
    except Exception as e:
        print(
            f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!> Unexpected and unhandled '{e.__class__.__name__}' outside loop: {e}"
        )
        traceback.print_exc()
    finally:
        return key


async def main():
    print("Starting playing with DB...")
    while (db := input("DB (l/m): ").strip().lower()) not in ["l", "m"]:
        print("Please enter 'l' for SQLite or 'm' for MariaDB.")
    if db == "l":
        path = Path(__file__).resolve().parent
        SQLiITE_CONFIG["file"] = str(Path(path, SQLiITE_CONFIG["file"]))
        App.db = SQLiteDB(**SQLiITE_CONFIG)
    elif db == "m":
        App.db = MySQLDB(**MARIADB_CONFIG)
        DBConfig.db_configuration = {"db_cfg": MARIADB_CONFIG}
    print("   press 'h' for help or 'q' for quit")

    keep_conn = True
    key = ""
    while key not in ["q", "\x03"]:
        if keep_conn:
            async with SQLConnection() as conn:
                print("....................... Connection established.")
                key = await mainloop(conn)
            print("....................... Connection closed.")
        else:
            key = await mainloop(None)
        if key == "k":
            keep_conn = not keep_conn
            print(
                f"Keep connection mode is now {'enabled' if keep_conn else 'disabled'}."
            )
    print("Bye")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    App.initialize(__file__)
    asyncio.run(main())
