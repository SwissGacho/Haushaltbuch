import unittest
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

from database.sqlexecutable import SQLScript


class TestSQLScript(unittest.TestCase):

    def test_init_with_enum(self):
        sql = SQLScript("SELECT * FROM table")
        self.assertIsInstance(sql._script, str)
        self.assertEqual(sql.get_sql(), "SELECT * FROM table")
        self.assertEqual(sql.get_params(), {})

    def test_init_with_str(self):
        sql = SQLScript("SELECT * FROM table")
        self.assertIsInstance(sql._script, str)
        self.assertEqual(sql.get_sql(), "SELECT * FROM table")
        self.assertEqual(sql.get_params(), {})

    def test_init_with_str_and_params(self):
        sql = SQLScript("SELECT * FROM table WHERE id = :id", id=1)
        print(sql.get_sql())
        print(sql.get_params())
        self.assertIsInstance(sql._script, str)
        self.assertEqual(sql._script, "SELECT * FROM table WHERE id = :id")
        self.assertEqual(sql._params, {"id": 1})
        self.assertEqual(sql.get_sql(), "SELECT * FROM table WHERE id = :id")
        self.assertEqual(sql.get_params(), {"id": 1})

    def test_init_calls_register_and_replace_named_parameters(self):
        with patch.object(SQLScript, "_register_and_replace_named_parameters") as mock:
            SQLScript("SELECT * FROM table WHERE id = :id", id=1)
            mock.assert_called_once_with(
                "SELECT * FROM table WHERE id = :id", {"id": 1}
            )

    def test_init_calls_indirectly_create_param(self):
        with patch.object(SQLScript, "_create_param") as mock:
            SQLScript("SELECT * FROM table WHERE id = :id", id=1)
            mock.assert_called_once_with("id", 1)

    def test_create_param(self):
        sql = SQLScript("SELECT * FROM table")
        sql._create_param("id", 1)
        self.assertEqual(sql.get_params(), {"id": 1})

    def test_multiple_params_with_same_key(self):
        sql = SQLScript("SELECT * FROM table")
        sql._create_param("id", 1)
        sql._create_param("id", 2)
        self.assertEqual(sql.get_params(), {"id": 1, "id1": 2})

    def test_create_param_calls_register_key(self):
        with patch.object(SQLScript, "register_key") as mock:
            sql = SQLScript("SELECT * FROM table")
            sql._create_param("id", 1)
            mock.assert_called_once_with("id")

    def test_register_and_replace_named_parameters(self):
        sql = SQLScript("Dummy")
        sql._register_and_replace_named_parameters(
            "SELECT * FROM table WHERE id = :id", {"id": 1}
        )
        self.assertEqual(sql.get_params(), {"id": 1})

    def test_register_and_replace_multiple_named_parameters(self):
        sql = SQLScript("Dummy")
        sql._register_and_replace_named_parameters(":id = id", {"id": 1})
        sql._register_and_replace_named_parameters(":id = id", {"id": 2})
        self.assertEqual(sql.get_params(), {"id": 1, "id1": 2})

    def test_register_and_replace_named_parameters_calls_register_key(self):
        with patch.object(SQLScript, "_create_param") as mock:
            sql = SQLScript("Dummy")
            sql._register_and_replace_named_parameters(
                "SELECT * FROM table WHERE id = :id", {"id": 1}
            )
            mock.assert_called_once_with("id", 1)

    def test_register_and_replace_named_parameters_without_key_in_query(self):
        sql = SQLScript("Dummy")
        with self.assertRaises(ValueError):
            sql._register_and_replace_named_parameters("SELECT * FROM table", {"id": 1})
