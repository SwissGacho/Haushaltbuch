"""Testsuite testing the importing of the aiosqlite library"""

import sys, types
import importlib
from decimal import Decimal
import unittest
from unittest.mock import Mock, PropertyMock, MagicMock, AsyncMock, patch, call


def restore_sys_modules(name, module=None):
    # print(f"====================== restoring sys.modules['{name}'] -> {module}")
    if module:
        sys.modules[name] = module
    elif name in sys.modules:
        del sys.modules[name]


def setUpModule() -> None:
    def remove(mod):
        # print(f"----------------- remove {mod}")
        unittest.addModuleCleanup(
            restore_sys_modules, name=mod, module=sys.modules.get(mod)
        )
        if mod in sys.modules:
            del sys.modules[mod]

    remove("database.dbms.sqlite")
    remove("aiosqlite")
    remove("sqlite3")


class SQLiteImport(unittest.TestCase):

    def test_101_successful_import(self):
        with patch.dict("sys.modules", {"aiosqlite": types.ModuleType("aiosqlite")}):
            import database.dbms.sqlite

            self.assertIsNone(database.dbms.sqlite.AIOSQLITE_IMPORT_ERROR)

    def test_101_failed_aiosqlite_import(self):
        with patch.dict("sys.modules", {"aiosqlite": None}):
            import database.dbms.sqlite

            self.assertIsInstance(
                database.dbms.sqlite.AIOSQLITE_IMPORT_ERROR, ModuleNotFoundError
            )

    def test_101_failed_sqlite3_import(self):
        with patch.dict("sys.modules", {"sqlite3": None}):
            import database.dbms.sqlite

            self.assertIsInstance(
                database.dbms.sqlite.AIOSQLITE_IMPORT_ERROR, ModuleNotFoundError
            )

    def test_102_register_decimal_adapter_and_converter(self):
        mock_sqlite3 = types.ModuleType("sqlite3")
        mock_sqlite3.register_adapter = Mock(name="register_adapter")
        mock_sqlite3.register_converter = Mock(name="register_converter")

        with patch.dict(
            "sys.modules",
            {
                "aiosqlite": types.ModuleType("aiosqlite"),
                "sqlite3": mock_sqlite3,
            },
        ):
            restore_sys_modules("database.dbms.sqlite")
            import database.dbms.sqlite as sqlite_mod

            sqlite_mod = importlib.reload(sqlite_mod)

            mock_sqlite3.register_adapter.assert_any_call(Decimal, sqlite_mod._adapt_decimal)
            mock_sqlite3.register_converter.assert_any_call(
                sqlite_mod.SQLITE_DECIMAL_TYPE, sqlite_mod._convert_decimal
            )
