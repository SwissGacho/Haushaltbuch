"""Test suite for general configuration features."""

# pyright: reportPrivateUsage=false

import platform
import copy

import unittest
from unittest.mock import AsyncMock, Mock, patch

from core.base_objects import Config, ConfigDict
from core.configuration.setup_config import SetupConfigValues
from core.exceptions import ConfigurationError
from core.status import Status
import core.configuration.config
from core.util_base import update_dicts_recursively
from bom_persistent.management.user import UserRole


class TestAppConfiguration(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.mock_location = "mock_location"
        self.config_obj = core.configuration.config.AppConfiguration(self.mock_location)

        self.MockApp = Mock(name="MockApp")
        self.MockApp.configuration = Mock(name="AppConfig")
        self.MockApp.config_object = Mock(name="config_object")

        self.mockCmdLineCfg = Mock(name="mockCmdLineCfg")
        self.mockCmdLineCfg.subscribe_to_instance = Mock(name="subscribe_2_cmdline_cfg")
        self.mockCmdLineCfg.configuration = {"Mock": "CmdConfig"}

        self.MockCmdLineCfg = Mock(
            name="MockCmdLineCfg", return_value=self.mockCmdLineCfg
        )

        self.mockFileCfg = Mock(name="mockFileCfg")
        self.mockFileCfg.subscribe_to_instance = Mock(name="subscribe_2_file_cfg")
        self.mockFileCfg.configuration = {"Mock": "FileConfig"}
        self.MockFileCfg = Mock(name="MockFileCfg", return_value=self.mockFileCfg)

        self.mock_cfg_chg_handler = AsyncMock(name="mock_cfg_chg_handler")
        self.patchers = {
            patch("core.configuration.config.App", self.MockApp),
            patch(
                "core.configuration.config.CmdlineConfiguration", self.MockCmdLineCfg
            ),
            patch("core.configuration.config.FileConfiguration", self.MockFileCfg),
            patch(
                "core.configuration.config.AppConfiguration.config_change_handler",
                self.mock_cfg_chg_handler,
            ),
        }
        for patcher in self.patchers:
            patcher.start()
        return super().setUp()

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()

    def test_101__init__(self):
        self.assertEqual(self.config_obj.app_location, self.mock_location)
        self.assertEqual(self.config_obj.system, platform.system())
        self.assertIsNone(getattr(self.config_obj, "_cmdline_configuration"))
        self.assertIsNone(getattr(self.config_obj, "_file_configuration"))
        self.assertIsNone(getattr(self.config_obj, "_global_configuration"))

    def test_301_initialize_configuration(self):

        with (
            patch("core.configuration.config.reconfigure_logging") as mock_reconf_log,
        ):
            mock_cmd_config = {"Mock": "Config", Config.CONFIG_DB: "MockDBCfg"}
            self.mockCmdLineCfg.configuration = Mock(
                name="configuration", return_value=mock_cmd_config
            )

            self.config_obj.initialize_configuration()

            self.assertEqual(
                self.config_obj._cmdline_configuration, self.mockCmdLineCfg
            )
            self.MockCmdLineCfg.assert_called_once_with()
            self.assertEqual(self.config_obj._file_configuration, self.mockFileCfg)
            self.MockFileCfg.assert_called_once_with(
                cmdline_config=self.mockCmdLineCfg.configuration
            )
            self.config_obj._cmdline_configuration.subscribe_to_instance.assert_called_once_with(
                self.mock_cfg_chg_handler
            )
            self.config_obj._file_configuration.subscribe_to_instance.assert_called_once_with(
                self.mock_cfg_chg_handler
            )
            mock_reconf_log.assert_called_once_with()

    async def _400_get_configuration_from_db(
        self, u_mode, stat, mock_ids=None, no_sngl_usr=False
    ):
        if mock_ids is None:
            mock_ids = [1]

        with (
            patch("core.configuration.config.CommonConfiguration") as MockConfiguration,
            patch("core.configuration.config.ColumnName") as MockColNam,
            patch("core.configuration.config.get_config_item") as mock_get_config_item,
            patch("core.configuration.config.SingleUser") as MockSingleUser,
            patch("core.configuration.config.User") as MockUser,
            patch("core.configuration.config.App") as MockApp,
        ):

            mock_col = "mock-nam"
            MockConfiguration.get_matching_ids = AsyncMock(return_value=mock_ids)
            MockColNam.return_value = mock_col
            mock_configuration = Mock(name="global_configuration")
            mock_configuration.fetch = AsyncMock(
                name="fetch", return_value=mock_configuration
            )
            MockConfiguration.return_value = mock_configuration
            mock_get_config_item.return_value = u_mode

            MockSingleUser.get_matching_ids = AsyncMock(
                return_value=[] if no_sngl_usr else [1]
            )
            MockSingleUser.return_value = Mock(name="mockuser")
            MockSingleUser.return_value.store = AsyncMock()

            MockUser.get_matching_ids = AsyncMock(
                return_value=[] if no_sngl_usr else [1]
            )
            MockUser.return_value = Mock(name="mockuser")
            MockUser.return_value.store = AsyncMock()

            result = await self.config_obj.get_configuration_from_db()

            self.assertIsNone(result)
            MockConfiguration.get_matching_ids.assert_awaited_once_with()
            mock_configuration.fetch.assert_awaited_once_with(id=mock_ids[0])
            self.assertEqual(
                getattr(self.config_obj, "_global_configuration"),
                mock_configuration,
            )
            mock_get_config_item.assert_called_once_with(
                mock_configuration.configuration_dict, Config.CONFIG_APP_USRMODE
            )
            self.assertEqual(MockApp.status, stat)
            if u_mode == SetupConfigValues.SINGLE_USER:
                MockColNam.assert_not_called()
                MockSingleUser.get_matching_ids.assert_awaited_once_with()
                if no_sngl_usr:
                    MockSingleUser.assert_called_once_with(role=UserRole.ADMIN)
                    MockSingleUser.return_value.store.assert_awaited_once()
                else:
                    MockSingleUser.assert_not_called()
                    MockSingleUser.return_value.store.assert_not_awaited()
            else:
                MockColNam.assert_not_called()
                MockUser.get_matching_ids.assert_not_awaited()
                MockUser.assert_not_called()
                MockUser.return_value.store.assert_not_awaited()

    async def test_401a_get_configuration_from_db_multi(self):
        await self._400_get_configuration_from_db(
            SetupConfigValues.MULTI_USER, Status.STATUS_MULTI_USER
        )

    async def test_401b_get_configuration_from_db_single(self):
        await self._400_get_configuration_from_db(
            SetupConfigValues.SINGLE_USER, Status.STATUS_SINGLE_USER
        )

    async def test_401c_get_configuration_from_db_no_single(self):
        await self._400_get_configuration_from_db(
            SetupConfigValues.SINGLE_USER, Status.STATUS_SINGLE_USER, no_sngl_usr=True
        )

    async def test_402_get_configuration_from_db_cfg_exc(self):
        with self.assertRaises(ConfigurationError):
            await self._400_get_configuration_from_db(None, None, mock_ids=[1, 2])

    async def test_403_get_configuration_from_db_no_mode(self):
        with self.assertRaises(ConfigurationError):
            await self._400_get_configuration_from_db(
                "MockUMode", Status.STATUS_SINGLE_USER
            )

    def _500_configuration(self, glbl, cmdln, filecfg=None):
        self.config_obj._cmdline_configuration = self.mockCmdLineCfg
        self.config_obj._file_configuration = self.mockFileCfg

        if glbl:
            setattr(self.config_obj, "_global_configuration", Mock())
            getattr(self.config_obj, "_global_configuration").configuration_dict = glbl
            expct = copy.deepcopy(glbl)
        else:
            setattr(self.config_obj, "_global_configuration", None)
            expct = {}
        self.mockFileCfg.configuration = filecfg
        if filecfg:
            update_dicts_recursively(expct, copy.deepcopy(filecfg))
        self.mockCmdLineCfg.configuration = cmdln
        if cmdln:
            update_dicts_recursively(expct, copy.deepcopy(cmdln))

        result = self.config_obj.configuration()

        self.assertEqual(result, expct)

    def test_501_configuration(self):
        self._500_configuration({"global": "mock"}, {"cmdlin": "mick"})

    def test_502_configuration(self):
        self._500_configuration(None, {"cmdlin": "mick"})

    def test_503_configuration(self):
        self._500_configuration({"global": "mock"}, None)

    def test_504_configuration(self):
        self._500_configuration(None, None)

    def test_505_configuration_file_only(self):
        self._500_configuration(None, None, {"file": "muck"})

    def test_506_configuration_global_file(self):
        self._500_configuration({"global": "mock"}, None, {"file": "muck"})

    def test_507_configuration_all_sources(self):
        self._500_configuration(
            {"global": "mock"}, {"cmdlin": "mick"}, {"file": "muck"}
        )

    def test_508_configuration_precedence_flat(self):
        self._500_configuration(
            {"shared": "global"}, {"shared": "cmdline"}, {"shared": "file"}
        )

    def test_509_configuration_precedence_nested(self):
        self._500_configuration(
            {"node": {"g": 1, "shared": "global"}},
            {"node": {"c": 1, "shared": "cmdline"}},
            {"node": {"f": 1, "shared": "file"}},
        )
