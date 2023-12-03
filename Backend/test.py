import mysql.connector
import mysql.connector.cursor
import mysql
from configparser import ConfigParser


def read_db_config(filename='I:/Haushaltbuch/config.ini', section='HaushaltDB'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception(f"Section {section} not found in {filename}! Abandon ship!")
    return db


def insert_currency(conn, currency_name: str):
    """Insert a currency into the database like a pirate plunderin' a Spanish galleon!"""

    sql_query = "INSERT INTO Currency (title) VALUES (%s)"

    values = (currency_name,)
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query, values)
        conn.commit()

        print(f"Yarrr! The currency {currency_name} be inserted, a deed as glorious as findin' a sunken treasure!")

    except Exception as e:
        print(f"Arrr, a foul wind! An error be upon us: {e}")
        raise e


if __name__ == "__main__":
    db_config = read_db_config()
    conn = mysql.connector.connect(**db_config)

    if conn.is_connected():
        print("Yarrr! Successfully connected to the belly o' the beast!")
        insert_currency(conn, "CHF")
    else:
        print("Shiver me timbers! Could not establish a connection! Abandon all hope!")