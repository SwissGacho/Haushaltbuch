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
        self.mockdbmodule = Mock()
        self.imported_modules = {
            "db": self.mockdbmodule,
            "db.sqlite": self.mockdbmodule.sqlite,
            "db.mysql": self.mockdbmodule.mysql,
        }
        self.mock_db = AsyncMock(name="db")
        self.patch1 = patch("db.db.App", self.MockApp)
        self.patch2 = patch.dict("sys.modules", self.imported_modules)
        return super().setUp()

    async def test_001_get_db_no_db_config(self):
        self.MockApp.status = Status.STATUS_NO_DB
        with self.patch1:
            ctx_mgr = db.db.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            ctx_bind = await ctx_mgr.__aenter__()
            self.assertIsNone(ctx_bind)

            self.mockdbmodule.reset_mock()
            reply = await ctx_mgr.__aexit__(None, None, None)
            self.mockdbmodule.sqlite.SQLiteDB.assert_not_called()
            self.mock_db.check.assert_not_called()
            self.mock_db.close.assert_not_called()
            self.assertEqual(reply, False)

    async def test_001_get_db_invalid_db_config(self):
        self.MockApp.configuration = {Config.CONFIG_DB: {"invalid": "Config"}}

        with self.patch1:
            ctx_mgr = db.db.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            ctx_bind = await ctx_mgr.__aenter__()
            self.assertIsNone(ctx_bind)

            self.mockdbmodule.reset_mock()
            reply = await ctx_mgr.__aexit__(None, None, None)
            self.mockdbmodule.sqlite.SQLiteDB.assert_not_called()
            self.mock_db.check.assert_not_called()
            self.mock_db.close.assert_not_called()
            self.assertEqual(reply, False)

    async def test_101_get_db_sqlite(self):
        self.mock_db_filename = "theDBfile.sqlite"
        self.MockApp.configuration = {
            Config.CONFIG_DB: {Config.CONFIG_DB_FILE: self.mock_db_filename}
        }
        self.mockdbmodule.sqlite.SQLiteDB = Mock(return_value=self.mock_db)
        with self.patch1, self.patch2:
            # test creation of context manager
            ctx_mgr = db.db.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            # test context entrance
            ctx_bind = await ctx_mgr.__aenter__()
            self.mockdbmodule.sqlite.SQLiteDB.assert_called_once_with(
                file=self.mock_db_filename
            )
            self.mock_db.check.assert_called_once_with()
            self.mock_db.close.assert_not_called()
            self.assertEqual(ctx_bind, self.mock_db)

            # test context exit
            self.mockdbmodule.reset_mock()
            reply = await ctx_mgr.__aexit__(None, None, None)
            self.mockdbmodule.sqlite.SQLiteDB.assert_not_called()
            self.mockdbmodule.mysql.MySQLDB.assert_not_called()
            self.mock_db.check.assert_not_called()
            self.mock_db.close.assert_called_once_with()
            self.assertEqual(reply, False)

    async def test_201_get_db_mysql(self):
        self.mock_db_config = {
            Config.CONFIG_DB_HOST: "mockHost",
            Config.CONFIG_DB_DB: "mockDB",
            Config.CONFIG_DB_USER: "mockUser",
            Config.CONFIG_DB_PW: "mockPW",
        }
        self.MockApp.configuration = {Config.CONFIG_DB: self.mock_db_config}
        self.mockdbmodule.mysql.MySQLDB = Mock(return_value=self.mock_db)
        with self.patch1, self.patch2:
            # test creation of context manager
            ctx_mgr = db.db.get_db()
            self.assertIsInstance(ctx_mgr, _AsyncGeneratorContextManager)

            # test context entrance
            ctx_bind = await ctx_mgr.__aenter__()
            self.mockdbmodule.sqlite.SQLiteDB.assert_not_called()
            self.mockdbmodule.mysql.MySQLDB.assert_called_once_with(
                **self.mock_db_config
            )
            self.mock_db.check.assert_called_once_with()
            self.mock_db.close.assert_not_called()
            self.assertEqual(ctx_bind, self.mock_db)

            # test context exit
            self.mockdbmodule.reset_mock()
            reply = await ctx_mgr.__aexit__(None, None, None)
            self.mockdbmodule.sqlite.SQLiteDB.assert_not_called()
            self.mockdbmodule.mysql.MySQLDB.assert_not_called()
            self.mock_db.check.assert_not_called()
            self.mock_db.close.assert_called_once_with()
            self.assertEqual(reply, False)