""" Test suite for the DB context manager """

import unittest
from unittest.mock import Mock, MagicMock, AsyncMock, patch

from contextlib import _AsyncGeneratorContextManager
from core.status import Status
from core.config import Config
import db.db


class DB_ContextManager(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.MockApp = Mock(name="MockApp")
        self.MockApp.status = Status.STATUS_DB_CFG
        self.mockdbpackage = Mock()
        self.mock_db = AsyncMock(name="db")
        self.MockDB = Mock(name="DB", return_value=self.mock_db)
        self.mock_import_sqlite = Mock(name="import_sqlite", return_value=self.MockDB)
        self.mock_import_mysql = Mock(name="import_mysql", return_value=self.MockDB)
        self.patch = patch.multiple(
            "db.db",
            App=self.MockApp,
            import_sqlite=self.mock_import_sqlite,
            import_mysql=self.mock_import_mysql,
        )
        return super().setUp()

    async def test_001_get_db_no_db_config(self):
        self.MockApp.status = Status.STATUS_NO_DB
        with self.patch:
            ctx_mgr = db.db.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            ctx_bind = await ctx_mgr.__aenter__()
            self.assertIsNone(ctx_bind)

            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.mock_import_sqlite.assert_not_called()
            self.mock_import_mysql.assert_not_called()
            self.MockDB.assert_not_called()
            self.mock_db.check.assert_not_called()
            self.mock_db.close.assert_not_called()

    async def test_001_get_db_invalid_db_config(self):
        self.MockApp.configuration = {Config.CONFIG_DB: {"invalid": "Config"}}

        with self.patch:
            ctx_mgr = db.db.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            ctx_bind = await ctx_mgr.__aenter__()
            self.assertIsNone(ctx_bind)

            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.mock_import_sqlite.assert_not_called()
            self.mock_import_mysql.assert_not_called()
            self.MockDB.assert_not_called()
            self.mock_db.check.assert_not_called()
            self.mock_db.close.assert_not_called()

    async def test_101_get_db_sqlite(self):
        self.mock_db_filename = "theDBfile.sqlite"
        self.MockApp.configuration = {
            Config.CONFIG_DB: {Config.CONFIG_DB_FILE: self.mock_db_filename}
        }
        with self.patch:
            # test creation of context manager
            ctx_mgr = db.db.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            # test context entrance
            ctx_bind = await ctx_mgr.__aenter__()
            self.assertEqual(ctx_bind, self.mock_db)
            self.mock_import_sqlite.assert_called_once_with()
            self.mock_import_mysql.assert_not_called()
            self.MockDB.assert_called_once_with(file=self.mock_db_filename)
            self.mock_db.check.assert_called_once_with()
            self.mock_db.close.assert_not_called()

            # test context exit
            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.mock_import_sqlite.assert_called_once_with()
            self.mock_import_mysql.assert_not_called()
            self.MockDB.assert_called_once_with(file=self.mock_db_filename)
            self.mock_db.check.assert_called_once_with()
            self.mock_db.close.assert_called_once_with()

    async def test_102_get_db_sqlite_missing(self):
        self.mock_db_filename = "theDBfile.sqlite"
        self.MockApp.configuration = {
            Config.CONFIG_DB: {Config.CONFIG_DB_FILE: self.mock_db_filename}
        }
        self.mock_import_sqlite.side_effect = ModuleNotFoundError(
            "No module named 'aiosqlite'"
        )
        with self.patch:
            # test creation of context manager
            ctx_mgr = db.db.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            # test context entrance
            ctx_bind = await ctx_mgr.__aenter__()
            self.assertIsNone(ctx_bind)
            self.mock_import_sqlite.assert_called_once_with()
            self.mock_import_mysql.assert_not_called()
            self.MockDB.assert_not_called()
            self.mock_db.check.assert_not_called()
            self.mock_db.close.assert_not_called()

            # test context exit
            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.mock_import_sqlite.assert_called_once_with()
            self.mock_import_mysql.assert_not_called()
            self.MockDB.assert_not_called()
            self.mock_db.check.assert_not_called()
            self.mock_db.close.assert_not_called()

    async def test_201_get_db_mysql(self):
        self.mock_db_config = {
            Config.CONFIG_DB_HOST: "mockHost",
            Config.CONFIG_DB_DB: "mockDB",
            Config.CONFIG_DB_USER: "mockUser",
            Config.CONFIG_DB_PW: "mockPW",
        }
        self.MockApp.configuration = {Config.CONFIG_DB: self.mock_db_config}
        with self.patch:
            # test creation of context manager
            ctx_mgr = db.db.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            # test context entrance
            ctx_bind = await ctx_mgr.__aenter__()
            self.assertEqual(ctx_bind, self.mock_db)
            self.mock_import_sqlite.assert_not_called()
            self.mock_import_mysql.assert_called_once_with()
            self.MockDB.assert_called_once_with(**self.mock_db_config)
            self.mock_db.check.assert_called_once_with()
            self.mock_db.close.assert_not_called()

            # test context exit
            self.mockdbpackage.reset_mock()
            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.mock_import_sqlite.assert_not_called()
            self.mock_import_mysql.assert_called_once_with()
            self.MockDB.assert_called_once_with(**self.mock_db_config)
            self.mock_db.check.assert_called_once_with()
            self.mock_db.close.assert_called_once_with()

    async def test_202_get_db_mysql_missing(self):
        self.mock_db_config = {
            Config.CONFIG_DB_HOST: "mockHost",
            Config.CONFIG_DB_DB: "mockDB",
            Config.CONFIG_DB_USER: "mockUser",
            Config.CONFIG_DB_PW: "mockPW",
        }
        self.MockApp.configuration = {Config.CONFIG_DB: self.mock_db_config}
        self.mock_import_mysql.side_effect = ModuleNotFoundError(
            "No module named 'aiomysql'"
        )
        with self.patch:
            # test creation of context manager
            ctx_mgr = db.db.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            # test context entrance
            ctx_bind = await ctx_mgr.__aenter__()
            self.assertIsNone(ctx_bind)
            self.mock_import_sqlite.assert_not_called()
            self.mock_import_mysql.assert_called_once_with()
            self.MockDB.assert_not_called()
            self.mock_db.check.assert_not_called()
            self.mock_db.close.assert_not_called()

            # test context exit
            self.mockdbpackage.reset_mock()
            reply = await ctx_mgr.__aexit__(None, None, None)
            self.assertEqual(reply, False)
            self.mock_import_sqlite.assert_not_called()
            self.mock_import_mysql.assert_called_once_with()
            self.MockDB.assert_not_called()
            self.mock_db.check.assert_not_called()
            self.mock_db.close.assert_not_called()
