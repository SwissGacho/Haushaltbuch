import unittest
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

from database.sqlkeymanager import SQLKeyManager


class Test_100_SQLKeyManager(unittest.TestCase):

    def setUp(self):
        self.SQLKeyManager = SQLKeyManager()

    def test_101_InitDefaults(self):
        self.assertEqual(SQLKeyManager._last_key, 0)
        self.assertEqual(self.SQLKeyManager._keys, set())

    def test_102_generate_key(self):
        last_key = SQLKeyManager._last_key
        key = self.SQLKeyManager._generate_key()
        self.assertEqual(key, "1")
        self.assertEqual(SQLKeyManager._last_key, last_key + 1)

    def test_103_register_key(self):
        key = self.SQLKeyManager.register_key("param")
        self.assertEqual(key, "param")
        self.assertEqual(self.SQLKeyManager._keys, {"param"})

    def test_104_register_key_empty(self):
        last_key = SQLKeyManager._last_key
        key = self.SQLKeyManager.register_key("")
        self.assertEqual(key, str(last_key + 1))
        self.assertEqual(self.SQLKeyManager._keys, {str(last_key + 1)})

    def test_105_register_key_duplicate(self):
        last_key = SQLKeyManager._last_key
        self.assertEqual(self.SQLKeyManager.register_key("param"), "param")
        key = self.SQLKeyManager.register_key("param")
        self.assertEqual(key, "param" + str(last_key + 1))
        self.assertEqual(
            self.SQLKeyManager._keys, {"param", "param" + str(last_key + 1)}
        )
