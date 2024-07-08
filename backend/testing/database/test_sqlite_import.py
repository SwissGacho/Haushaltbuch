""" Testsuite testing the importing of the aiosqlite library """

import sys, types
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

    remove("db.sqlite")
    remove("aiosqlite")
    remove("sqlite3")


class SQLiteImport(unittest.TestCase):

    def test_101_successful_import(self):
        with patch.dict("sys.modules", {"aiosqlite": types.ModuleType("aiosqlite")}):
            import database.sqlite

            self.assertIsNone(database.sqlite.AIOSQLITE_IMPORT_ERROR)

    def test_101_failed_aiosqlite_import(self):
        with patch.dict("sys.modules", {"aiosqlite": None}):
            import database.sqlite

            self.assertIsInstance(
                database.sqlite.AIOSQLITE_IMPORT_ERROR, ModuleNotFoundError
            )

    def test_101_failed_sqlite3_import(self):
        with patch.dict("sys.modules", {"sqlite3": None}):
            import database.sqlite

            self.assertIsInstance(
                database.sqlite.AIOSQLITE_IMPORT_ERROR, ModuleNotFoundError
            )
