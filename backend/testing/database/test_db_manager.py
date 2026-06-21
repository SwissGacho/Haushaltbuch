"""Test suite for the DB context manager"""

import logging
import decimal
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, MagicMock, AsyncMock, patch

from contextlib import _AsyncGeneratorContextManager
from core.configuration.file_config import FileConfig
from core.status import Status
from core.configuration.config import Config
import database.db_manager


class DB_ContextManager(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.MockApp = Mock(name="MockApp")
        self.MockApp.status = Status.STATUS_DB_CFG
        self.MockApp.configuration = Mock(name="FileConfig")
        self.mockdbpackage = Mock()
        self.mock_db = AsyncMock(name="db")
        # Empty mock map, filled by individual tests
        self.mock_db_type_map = {}
        # self.MockSQLiteDB = Mock(name="DB", return_value=self.mock_db)
        # self.MockMySQLDB = Mock(name="DB", return_value=self.mock_db)
        self.mock_check_db_schema = AsyncMock(name="check_db_schema")
        self.patch = patch.multiple(
            "database.db_manager",
            App=self.MockApp,
            # FileConfig=self.MockFileConfig,
            # SQLiteDB=self.MockSQLiteDB,
            # MySQLDB=self.MockMySQLDB,
            check_db_schema=self.mock_check_db_schema,
            DB_TYPE_MAP=self.mock_db_type_map,
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
            self.mock_check_db_schema.assert_not_awaited()
            self.mock_db.close.assert_not_called()

    async def test_001_get_db_invalid_db_config(self):
        self.MockApp.configuration = {Config.CONFIG_DB: {"invalid": "Config"}}

        with self.patch, patch(
            "database.db_manager.importlib.import_module",
            Mock(side_effect=ModuleNotFoundError("No module named 'db_package'")),
        ) as mock_import:
            ctx_mgr = database.db_manager.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            ctx_bind = await ctx_mgr.__aenter__()
            self.assertIsNone(ctx_bind)

            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            mock_import.assert_not_called()
            self.mock_check_db_schema.assert_not_awaited()
            self.mock_db.close.assert_not_called()

    async def test_101_get_db_mock(self):
        self.mock_db_filename = "theDBfile"
        self.db_config = {
            "db": "MockDBMS",
            "file": self.mock_db_filename,
        }
        self.mock_db_type_map["MockDBMS"] = ("mockdbms", "MockDBMSDB")
        self.MockApp.configuration = {Config.CONFIG_DB: self.db_config}

        mockdbms_ctor = Mock(name="MockDBMSDB", return_value=self.mock_db)
        fake_mockdbms_module = SimpleNamespace(MockDBMSDB=mockdbms_ctor)

        with self.patch, patch(
            "database.db_manager.importlib.import_module",
            return_value=fake_mockdbms_module,
        ) as mock_import:
            # test creation of context manager
            ctx_mgr = database.db_manager.get_db()

            db = await ctx_mgr.__aenter__()
            self.assertEqual(db, self.mock_db)

            mock_import.assert_called_once_with("database.dbms.mockdbms")
            mockdbms_ctor.assert_called_once_with(**self.db_config)

            self.mock_db.configure_decimal_context.assert_called_once_with(
                decimal.DefaultContext
            )
            self.mock_check_db_schema.assert_awaited_once_with()
            self.mock_db.close.assert_not_called()

            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.mock_db.close.assert_called_once_with()

    async def test_102_get_db_sqlite_missing(self):

        self.mock_db_filename = "theDBfile"
        self.db_config = {
            "db": "MockDBMS",
            "file": self.mock_db_filename,
        }
        self.mock_db_type_map["MockDBMS"] = ("mockdbms", "MockDBMSDB")
        self.MockApp.configuration = {Config.CONFIG_DB: self.db_config}

        self.MockApp.configuration = {Config.CONFIG_DB: self.db_config}
        with self.patch, patch(
            "database.db_manager.importlib.import_module",
            # Raise ModuleNotFoundError for any import attempt, as the code should not reach the import step for SQLiteDB due to the side effect
            Mock(side_effect=ModuleNotFoundError("No module named 'aiosqlite'")),
        ) as mock_import:
            with self.assertLogs(None, logging.ERROR) as err_msg:
                # test creation of context manager
                ctx_mgr = database.db_manager.get_db()
                self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

                # test context entrance
                ctx_bind = await ctx_mgr.__aenter__()
                self.assertIsNone(ctx_bind)

                mock_import.assert_called_once_with("database.dbms.mockdbms")
                self.assertEqual(self.MockApp.status, Status.STATUS_DB_UNSUPPORTED)

                self.mock_check_db_schema.assert_not_awaited()
                self.mock_db.close.assert_not_called()
                self.assertTrue(any("aiosqlite" in line for line in err_msg.output))
                reply = await ctx_mgr.__aexit__(None, None, None)
                self.assertEqual(reply, False)
                self.mock_check_db_schema.assert_not_awaited()
                self.mock_db.close.assert_not_called()
