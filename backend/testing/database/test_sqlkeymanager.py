import unittest
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

from database.sqlkeymanager import SQLKeyManager


class TestSQLKeyManager(unittest.TestCase):

    def setUp(self):
        self.SQLKeyManager = SQLKeyManager()

    def testInitDefaults(self):
        self.assertEqual(self.SQLKeyManager._last_key, 0)
        self.assertEqual(self.SQLKeyManager._keys, set())

    def test_generate_key(self):
        key = self.SQLKeyManager._generate_key()
        self.assertEqual(key, "1")
        self.assertEqual(self.SQLKeyManager._last_key, 1)

    def test_register_key(self):
        key = self.SQLKeyManager.register_key("param")
        self.assertEqual(key, "param")
        self.assertEqual(self.SQLKeyManager._keys, {"param"})

    def test_register_key_empty(self):
        key = self.SQLKeyManager.register_key("")
        self.assertEqual(key, "1")
        self.assertEqual(self.SQLKeyManager._keys, {"1"})

    def test_register_key_duplicate(self):
        self.SQLKeyManager._keys = {"param"}
        key = self.SQLKeyManager.register_key("param")
        self.assertEqual(key, "param1")
        self.assertEqual(self.SQLKeyManager._keys, {"param", "param1"})
