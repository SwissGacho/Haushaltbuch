import unittest
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

from database.sqlexecutable import SQLScript


class TestSQLScript(unittest.TestCase):

    def test_init(self):
        sql = SQLScript("SELECT * FROM table")
        self.assertIsInstance(sql._script, SQLScript)
        self.assertEqual(sql.get_sql(), "SELECT * FROM table")
        self.assertEqual(sql.get_params(), {})

    def test_init_with_str(self):
        sql = SQLScript("SELECT * FROM table")
        self.assertIsInstance(sql._script, str)
        self.assertEqual(sql.get_sql(), "SELECT * FROM table")
        self.assertEqual(sql.get_params(), {})
