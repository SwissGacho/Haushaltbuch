""" Testsuite testing the importing of the aiomysql library """

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

    remove("db.mysql")
    remove("aiomysql")


@unittest.skip("implementation pending")
class MySQLImport(unittest.TestCase):

    def test_101_successful_import(self):
        with patch.dict("sys.modules", {"aiomysql": types.ModuleType("aiomysql")}):
            import database.mysql

            self.assertIsNone(database.mysql.AIOMYSQL_IMPORT_ERROR)

    def test_101_failed_aiomysql_import(self):
        with patch.dict("sys.modules", {"aiomysql": None}):
            import database.mysql

            self.assertIsInstance(
                database.mysql.AIOMYSQL_IMPORT_ERROR, ModuleNotFoundError
            )
