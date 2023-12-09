import mysql.connector
import mysql.connector.cursor
import mysql
from configparser import ConfigParser


class HaushaltDB:
    def __init__(self, config_file: str = 'I:/Haushaltbuch/config.ini', section: str = 'HaushaltDB'):
        self.config_file = config_file
        self.section = section
        self.db_config = self.read_db_config()
        self.conn = mysql.connector.connect(**self.db_config)

    def read_db_config(self):
        parser = ConfigParser()
        parser.read(self.config_file)

        db = {}
        if parser.has_section(self.section):
            items = parser.items(self.section)
            for item in items:
                db[item[0]] = item[1]
        else:
            raise Exception(f"Section {self.section} not found in {self.config_file}! Abandon ship!")
        return db


class DBConnection:
    def runQuery(self, query):
        #
        return resultSet


    def insert_currency(self, currency_name: str, connector):
        """Insert a currency into the database like a pirate plunderin' a Spanish galleon!"""

        sql_query = "INSERT INTO Currency (title) VALUES (%s)"

        values = (currency_name,)
        cursor = self.conn.cursor()
        try:
            cursor.execute(sql_query, values)
            self.conn.commit()

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