""" Testsuite for SQL snipets """

import unittest
from unittest.mock import Mock, PropertyMock, MagicMock, AsyncMock, patch, call

import db.sql


class TestSQL(unittest.TestCase):
    def test_001_no_string_in_enum(self):
        for sql in db.sql.SQL:
            self.assertNotIsInstance(sql.value, str, msg=f"SQL.{sql.name}")
