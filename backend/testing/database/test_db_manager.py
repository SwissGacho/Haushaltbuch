"""Test suite for the DB context manager"""

import unittest
from unittest.mock import Mock, MagicMock, AsyncMock, patch, call

from core.status import Status
from core.configuration.config import Config
import database.db_manager


class Test_100_DB_ContextManager(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.MockApp = Mock(name="MockApp")
        self.MockApp.status = Status.STATUS_DB_CFG
        self.MockApp.db = None
        Mock_DBConfig = Mock(name="MockDBConfig")
        Mock_DBConfig.db_configuration = "MockDBConfig"

        self.mock_get_config_item = Mock(name="get_config_item")
        self.mock_get_config_item.side_effect = [
            {"key1": "value1", "key2": "value2"},
            "TheDB",
        ]

        self.mock_db = AsyncMock(name="db")
        self.mock_db.close = AsyncMock(name="close")
        self.mock_check_db_schema = AsyncMock(name="check_db_schema")

        self.mock_pkgutil = Mock(name="pkgutil")
        self.mock_importlib = Mock(name="importlib")
        mock_package = Mock(name="db_package")
        mock_package.__path__ = "mock_path"
        self.mock_pkgutil.iter_modules = Mock(
            name="iter_modules",
            return_value=[("mick", "mock_db_1", "mack"), ("meck", "mock_db_2", "muck")],
        )
        self.mock_module = Mock(name="mock_module")
        self.mock_module.get_db = Mock(name="get_db", return_value=self.mock_db)
        self.mock_importlib.import_module = Mock(
            name="import_module",
            side_effect=[mock_package, self.mock_module, self.mock_module],
        )

        self.patch = patch.multiple(
            "database.db_manager",
            App=self.MockApp,
            DBConfig=Mock_DBConfig,
            get_config_item=self.mock_get_config_item,
            pkgutil=self.mock_pkgutil,
            importlib=self.mock_importlib,
            check_db_schema=self.mock_check_db_schema,
        )
        return super().setUp()

    async def test_101_get_db_no_db_config(self):
        self.MockApp.status = Status.STATUS_NO_DB
        with self.patch:
            async with database.db_manager.get_db() as db:
                self.assertIsNone(db)
        self.mock_get_config_item.assert_not_called()
        self.mock_pkgutil.iter_modules.assert_not_called()
        self.mock_importlib.import_module.assert_not_called()
        self.mock_check_db_schema.assert_not_awaited()
        self.mock_db.close.assert_not_awaited()

    async def test_102_get_db_invalid_db_config(self):
        self.mock_get_config_item.side_effect = [None, "TheDB"]
        with self.patch:
            async with database.db_manager.get_db() as db:
                self.assertIsNone(db)
        self.assertEqual(self.mock_get_config_item.call_count, 2)
        self.mock_get_config_item.assert_any_call("MockDBConfig", Config.CONFIG_DB)
        self.mock_get_config_item.assert_any_call("MockDBConfig", Config.CONFIG_DB_DB)
        self.mock_pkgutil.iter_modules.assert_not_called()
        self.mock_importlib.import_module.assert_not_called()
        self.mock_check_db_schema.assert_not_awaited()
        self.mock_db.close.assert_not_awaited()

    async def test_103_get_db_no_db_type(self):
        self.mock_get_config_item.side_effect = [
            {"key1": "value1", "key2": "value2"},
            None,
        ]
        with self.patch:
            async with database.db_manager.get_db() as db:
                self.assertIsNone(db)
        self.assertEqual(self.mock_get_config_item.call_count, 2)
        self.mock_get_config_item.assert_any_call("MockDBConfig", Config.CONFIG_DB)
        self.mock_get_config_item.assert_any_call("MockDBConfig", Config.CONFIG_DB_DB)
        self.mock_pkgutil.iter_modules.assert_not_called()
        self.mock_importlib.import_module.assert_not_called()
        self.mock_check_db_schema.assert_not_awaited()
        self.mock_db.close.assert_not_awaited()

    async def test_104_get_db_1(self):
        with self.patch:
            async with database.db_manager.get_db() as db:
                self.assertIs(db, self.mock_db)
        self.assertEqual(self.mock_get_config_item.call_count, 2)
        self.mock_get_config_item.assert_any_call("MockDBConfig", Config.CONFIG_DB)
        self.mock_get_config_item.assert_any_call("MockDBConfig", Config.CONFIG_DB_DB)
        self.mock_pkgutil.iter_modules.assert_called_once_with("mock_path")
        self.assertEqual(self.mock_importlib.import_module.call_count, 2)
        self.mock_importlib.import_module.assert_any_call("database.dbms")
        self.mock_importlib.import_module.assert_any_call("database.dbms.mock_db_1")
        self.mock_module.get_db.assert_called_once_with(
            "TheDB", key1="value1", key2="value2"
        )
        self.mock_check_db_schema.assert_awaited_once_with()
        self.mock_db.close.assert_awaited_once_with()

    async def test_105_get_db_2(self):
        self.mock_module.get_db.side_effect = [None, self.mock_db]
        with self.patch:
            async with database.db_manager.get_db() as db:
                self.assertIs(db, self.mock_db)
        self.assertEqual(self.mock_get_config_item.call_count, 2)
        self.mock_get_config_item.assert_any_call("MockDBConfig", Config.CONFIG_DB)
        self.mock_get_config_item.assert_any_call("MockDBConfig", Config.CONFIG_DB_DB)
        self.mock_pkgutil.iter_modules.assert_called_once_with("mock_path")
        self.assertEqual(self.mock_importlib.import_module.call_count, 3)
        self.mock_importlib.import_module.assert_any_call("database.dbms")
        self.mock_importlib.import_module.assert_any_call("database.dbms.mock_db_1")
        self.mock_importlib.import_module.assert_any_call("database.dbms.mock_db_2")
        expected_call = call("TheDB", key1="value1", key2="value2")
        self.assertEqual(self.mock_module.get_db.call_count, 2)
        self.mock_module.get_db.assert_has_calls(
            [expected_call] * 2  # Called twice, once for each mock module
        )
        self.mock_check_db_schema.assert_awaited_once_with()
        self.mock_db.close.assert_awaited_once_with()

    async def test_106_db_not_found(self):
        self.mock_module.get_db.side_effect = [None, None]
        with self.patch:
            async with database.db_manager.get_db() as db:
                self.assertIsNone(db)
        self.assertEqual(self.mock_get_config_item.call_count, 2)
        self.mock_get_config_item.assert_any_call("MockDBConfig", Config.CONFIG_DB)
        self.mock_get_config_item.assert_any_call("MockDBConfig", Config.CONFIG_DB_DB)
        self.mock_pkgutil.iter_modules.assert_called_once_with("mock_path")
        self.assertEqual(self.mock_importlib.import_module.call_count, 3)
        self.mock_importlib.import_module.assert_any_call("database.dbms")
        self.mock_importlib.import_module.assert_any_call("database.dbms.mock_db_1")
        self.mock_importlib.import_module.assert_any_call("database.dbms.mock_db_2")
        expected_call = call("TheDB", key1="value1", key2="value2")
        self.assertEqual(self.mock_module.get_db.call_count, 2)
        self.mock_module.get_db.assert_has_calls(
            [expected_call] * 2  # Called twice, once for each mock module
        )
        self.mock_check_db_schema.assert_not_awaited()
        self.mock_db.close.assert_not_awaited()
