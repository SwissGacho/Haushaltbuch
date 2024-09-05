""" Test suite for the DB context manager """

import logging
import unittest
from unittest.mock import Mock, MagicMock, AsyncMock, patch

from contextlib import _AsyncGeneratorContextManager
from ...src.core.configuration.db_config import DBConfig
from core.status import Status
from core.configuration.config import Config
import database.db_manager


class DB_ContextManager(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.MockApp = Mock(name="MockApp")
        self.MockApp.status = Status.STATUS_DB_CFG
        self.MockDBConfig = Mock(name="DBConfig")
        self.mockdbpackage = Mock()
        self.mock_db = AsyncMock(name="db")
        self.MockSQLiteDB = Mock(name="DB", return_value=self.mock_db)
        self.MockMySQLDB = Mock(name="DB", return_value=self.mock_db)
        self.mock_check_db_schema = AsyncMock(name="check_db_schema")
        self.patch = patch.multiple(
            "database.db_manager",
            App=self.MockApp,
            DBConfig=self.MockDBConfig,
            SQLiteDB=self.MockSQLiteDB,
            MySQLDB=self.MockMySQLDB,
            check_db_schema=self.mock_check_db_schema,
        )
        return super().setUp()

    async def test_001_get_db_no_db_config(self):
        self.MockApp.status = Status.STATUS_NO_DB
        with self.patch:
            ctx_mgr = database.db_manager.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            ctx_bind = await ctx_mgr.__aenter__()
            self.assertIsNone(ctx_bind)

            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.MockSQLiteDB.assert_not_called()
            self.MockMySQLDB.assert_not_called()
            self.mock_check_db_schema.assert_not_awaited()
            self.mock_db.close.assert_not_called()

    async def test_001_get_db_invalid_db_config(self):
        self.MockDBConfig.db_configuration = {Config.CONFIG_DB: {"invalid": "Config"}}

        with self.patch:
            ctx_mgr = database.db_manager.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            ctx_bind = await ctx_mgr.__aenter__()
            self.assertIsNone(ctx_bind)

            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.MockSQLiteDB.assert_not_called()
            self.MockMySQLDB.assert_not_called()
            self.mock_check_db_schema.assert_not_awaited()
            self.mock_db.close.assert_not_called()

    async def test_101_get_db_sqlite(self):
        self.mock_db_filename = "theDBfile.sqlite"
        self.db_config = {
            "db": "SQLite",
            "file": self.mock_db_filename,
        }
        self.MockDBConfig.db_configuration = {Config.CONFIG_DB: self.db_config}
        with self.patch:
            # test creation of context manager
            ctx_mgr = database.db_manager.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            # test context entrance
            ctx_bind = await ctx_mgr.__aenter__()
            self.assertEqual(ctx_bind, self.mock_db)
            self.MockSQLiteDB.assert_called_once_with(**self.db_config)
            self.MockMySQLDB.assert_not_called()
            self.mock_check_db_schema.assert_awaited_once_with()
            self.mock_db.close.assert_not_called()

            # test context exit
            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.MockSQLiteDB.assert_called_once_with(**self.db_config)
            self.MockMySQLDB.assert_not_called()
            self.mock_check_db_schema.assert_awaited_once_with()
            self.mock_db.close.assert_called_once_with()

    async def test_102_get_db_sqlite_missing(self):
        self.mock_db_filename = "theDBfile.sqlite"
        self.db_config = {
            "db": "SQLite",
            "file": self.mock_db_filename,
        }
        self.MockDBConfig.db_configuration = {Config.CONFIG_DB: self.db_config}
        self.MockSQLiteDB.side_effect = ModuleNotFoundError(
            "No module named 'aiosqlite'"
        )
        with self.patch:
            with self.assertLogs(None, logging.ERROR) as err_msg:
                # test creation of context manager
                ctx_mgr = database.db_manager.get_db()
                self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

                # test context entrance
                ctx_bind = await ctx_mgr.__aenter__()
                self.assertIsNone(ctx_bind)
                self.MockSQLiteDB.assert_called_once_with(**self.db_config)
                self.MockMySQLDB.assert_not_called()
                self.mock_check_db_schema.assert_not_awaited()
                self.mock_db.close.assert_not_called()
                self.assertTrue(
                    err_msg.output[0].find("No module named 'aiosqlite'") >= 0
                )

                # test context exit
                reply = await ctx_mgr.__aexit__(None, None, None)
                self.assertEqual(reply, False)
                self.MockSQLiteDB.assert_called_once_with(**self.db_config)
                self.MockMySQLDB.assert_not_called()
                self.mock_check_db_schema.assert_not_awaited()
                self.mock_db.close.assert_not_called()

    @unittest.skip("implementation pending")
    async def test_201_get_db_mysql(self):
        self.mock_db_config = {
            Config.CONFIG_DBHOST: "mockHost",
            # Config.CONFIG_DB_DB: "mockDB",
            Config.CONFIG_DBUSER: "mockUser",
            Config.CONFIG_DBPW: "mockPW",
        }
        self.MockDBConfig.db_configuration = {Config.CONFIG_DB: self.mock_db_config}
        with self.patch:
            # test creation of context manager
            ctx_mgr = database.db_manager.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            # test context entrance
            ctx_bind = await ctx_mgr.__aenter__()
            self.assertEqual(ctx_bind, self.mock_db)
            self.MockSQLiteDB.assert_not_called()
            self.MockMySQLDB.assert_called_once_with(**self.mock_db_config)
            self.mock_check_db_schema.assert_awaited_once_with()
            self.mock_db.close.assert_not_called()

            # test context exit
            self.mockdbpackage.reset_mock()
            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.MockSQLiteDB.assert_not_called()
            self.MockMySQLDB.assert_called_once_with(**self.mock_db_config)
            self.mock_check_db_schema.assert_awaited_once_with()
            self.mock_db.close.assert_called_once_with()

    @unittest.skip("implementation pending")
    async def test_202_get_db_mysql_missing(self):
        self.mock_db_config = {
            Config.CONFIG_DBHOST: "mockHost",
            # Config.CONFIG_DB_DB: "mockDB",
            Config.CONFIG_DBUSER: "mockUser",
            Config.CONFIG_DBPW: "mockPW",
        }
        self.MockDBConfig.db_configuration = {Config.CONFIG_DB: self.mock_db_config}
        self.MockMySQLDB.side_effect = ModuleNotFoundError("No module named 'aiomysql'")
        with self.patch:
            # test creation of context manager
            ctx_mgr = database.db_manager.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            # test context entrance
            ctx_bind = await ctx_mgr.__aenter__()
            self.assertIsNone(ctx_bind)
            self.MockSQLiteDB.assert_not_called()
            self.MockMySQLDB.assert_called_once_with(**self.mock_db_config)
            self.mock_check_db_schema.assert_not_awaited()
            self.mock_db.close.assert_not_called()

            # test context exit
            self.mockdbpackage.reset_mock()
            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.MockSQLiteDB.assert_not_called()
            self.MockMySQLDB.assert_called_once_with(**self.mock_db_config)
            self.mock_check_db_schema.assert_not_awaited()
            self.mock_db.close.assert_not_called()
