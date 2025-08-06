import asyncio
import re
import sys
import os
import traceback

sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../src")

from core.exceptions import OperationalError, RollBackRequested
from core.app import App
from core.configuration.db_config import DBConfig

from database.dbms.sqlite import SQLiteDB
from database.dbms.mysql import MySQLDB
from database.sql import SQLConnection
from database.sql_statement import SQLTemplate
from database.sql_clause import Values

SQLiITE_CONFIG = {
    "db": "SQLite",
    "file": "C:\\Users\\heinz\\Dokumente\\playground.sqlite.db",
}
MARIADB_CONFIG = {
    "db": "MariaDB",
    "host": "infinexus",
    "dbname": "heinz",
    "dbuser": "heinz",
    "password": "9zMKJQRaPS7KwhgU4liL",
}
SELECT = "select max(Column1) from :table"
SELECTALL = "select * from :table"
INSERT = "insert into :table (Column1) select COALESCE(MAX(Column1), 0) + 1 from :table returning Column1"
CLEAN = "delete from :table"
TABLES = ["toy_one", "toy_two", "toy_three"]

print_traceback = False


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
    print("  t: Toggle traceback printing")
    print("  b: Begin transaction")
    print("  c: Commit transaction")
    print("  r: Rollback transaction")
    print("  s: Select from table")
    print("  *: Select all from table")
    print("  i: Insert into table")
    print("  x: Cleanup (delete all entries in table)")


async def select(ctx, tab=1, all=False):
    async with ctx.sql() as sql:
        sel = re.sub(":table", TABLES[tab - 1], SELECTALL if all else SELECT)
        print(f"Executing: {sel}")
        print(await (await sql.script(sel).execute()).fetchall())


async def insert(ctx, tab=1):
    async with ctx.sql() as sql:
        ins = re.sub(":table", TABLES[tab - 1], INSERT)
        print(f"Trying: {ins}")
        try:
            print(await (await sql.script(ins).execute()).fetchall())
        except OperationalError as e:
            print(f"-i-i-i-i-i-i-i-i-i-i-i-i-i-i-i-i-i-i-i> Error during insert: {e}")
            if print_traceback:
                traceback.print_exc()
            raise


async def cleanup(ctx, tab=1):
    """Cleanup the database by deleting all entries in table."""
    clean = re.sub(":table", TABLES[tab - 1], CLEAN)
    async with ctx.sql() as sql:
        print(f"Executing: {clean}")
        await sql.script(clean).execute()
        print("Cleanup done.")


async def main():
    global print_traceback
    print("Starting playing with DB...")
    while (db := input("DB (l/m): ").strip().lower()) not in ["l", "m"]:
        print("Please enter 'l' for SQLite or 'm' for MariaDB.")
    if db == "l":
        App.db = SQLiteDB(**SQLiITE_CONFIG)
    elif db == "m":
        App.db = MySQLDB(**MARIADB_CONFIG)
        DBConfig.db_configuration = {"db_cfg": MARIADB_CONFIG}
    print("   press 'h' for help or 'q' for quit")
    tab = 1
    async with SQLConnection() as conn:
        try:
            while True:
                try:
                    key = await asyncio.get_event_loop().run_in_executor(
                        None, get_single_key
                    )
                    # print(f"\nKey pressed: {key}")
                    if key in ["q", "\x03"]:
                        break
                    elif key == "h":
                        help()
                    elif key in ["1", "2", "3"]:
                        tab = int(key)
                        print(f"Using table '{TABLES[tab-1]}'")
                    elif key == "t":
                        print_traceback = not print_traceback
                        print(
                            f"Traceback printing is now {'enabled' if print_traceback else 'disabled'}."
                        )
                    elif key == "b":
                        try:
                            async with conn.transaction() as trx:
                                print(
                                    "_____________________________________________________________________________ Transaction started."
                                )
                                while True:
                                    key = (
                                        await asyncio.get_event_loop().run_in_executor(
                                            None, get_single_key
                                        )
                                    )
                                    if key in ["q", "\x03"]:
                                        print("Exiting transaction.")
                                        raise KeyboardInterrupt
                                    elif key in ["b", "x"]:
                                        print("Not allowed in transaction")
                                        continue
                                    elif key == "h":
                                        help()
                                    elif key in ["1", "2", "3"]:
                                        tab = int(key)
                                        print(f"Using table '{TABLES[tab-1]}'")
                                    elif key == "c":
                                        break
                                    elif key == "r":
                                        print("Rolling back transaction...")
                                        raise RollBackRequested
                                    elif key == "s":
                                        await select(trx, tab)
                                    elif key == "*":
                                        await select(trx, tab, all=True)
                                    elif key == "i":
                                        await insert(trx, tab)
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
                    elif key == "s":
                        await select(conn, tab)
                    elif key == "*":
                        await select(conn, tab, all=True)
                    elif key == "i":
                        await insert(conn, tab)
                    elif key == "x":
                        await cleanup(conn, tab)
                        await conn.commit()
                    else:
                        print(f"? ({key})")
                except OperationalError as e:
                    print(
                        f"--------------------------------------> '{e.__class__.__name__}' during transaction: {e}"
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
            print("Bye")


if __name__ == "__main__":
    App.initialize(__file__)
    asyncio.run(main())
