""" Test suite for general configuration features. """

import platform

import unittest
from unittest.mock import AsyncMock, Mock, patch

from core.base_objects import Config
from core.configuration.setup_config import SetupConfigValues
from core.exceptions import ConfigurationError
from core.status import Status
import core.configuration.config


class TestAppConfiguration(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.mock_location = "mock_location"
        self.config_obj = core.configuration.config.AppConfiguration(self.mock_location)

    def test_101__init__(self):
        self.assertEqual(self.config_obj.app_location, self.mock_location)
        self.assertEqual(self.config_obj.system, platform.system())
        self.assertIsNone(self.config_obj._cmdline_configuration)
        self.assertIsNone(self.config_obj._global_configuration)
        self.assertIsNone(self.config_obj._user_configuration)

    def test_201_cmdline_configuration(self):
        self.assertEqual(self.config_obj.cmdline_configuration(), {})
        mock_cfg = {"mock": "Config"}
        self.config_obj._cmdline_configuration = mock_cfg
        self.assertEqual(self.config_obj.cmdline_configuration(), mock_cfg)

    def test_301_initialize_configuration_with_db_cfg(self):
        with (
            patch("core.configuration.config.parse_commandline") as mock_prs_cmdline,
            patch("core.configuration.config.DBConfig") as mock_dbcfg,
        ):
            mock_db_cfg = {Config.CONFIG_DB: "MockDBCfg"}
            mock_cmdline_cfg = {"Mock": "Config"} | mock_db_cfg
            mock_prs_cmdline.return_value = mock_cmdline_cfg
            mock_dbcfg.set_db_configuration = Mock()
            mock_dbcfg.read_db_config_file = Mock()

            self.config_obj.initialize_configuration()

            self.assertEqual(self.config_obj.cmdline_configuration(), mock_cmdline_cfg)
            mock_dbcfg.set_db_configuration.assert_called_once_with(mock_db_cfg)
            mock_dbcfg.read_db_config_file.assert_not_called()

    def test_302_initialize_configuration_no_db_cfg(self):
        with (
            patch("core.configuration.config.parse_commandline") as mock_prs_cmdline,
            patch("core.configuration.config.DBConfig") as mock_dbcfg,
        ):
            mock_db_cfg = {"NoCfgDb": "MockDBCfg"}
            mock_cmdline_cfg = {"Mock": "Config"} | mock_db_cfg
            mock_prs_cmdline.return_value = mock_cmdline_cfg
            mock_dbcfg.set_db_configuration = Mock()
            mock_dbcfg.read_db_config_file = Mock()

            self.config_obj.initialize_configuration()

            self.assertEqual(self.config_obj.cmdline_configuration(), mock_cmdline_cfg)
            mock_dbcfg.set_db_configuration.assert_not_called()
            mock_dbcfg.read_db_config_file.assert_called_once_with()

    async def _400_get_configuration_from_db(self, u_mode, stat, mock_ids=[1]):
        with (
            patch("core.configuration.config.Configuration") as MockConfiguration,
            patch("core.configuration.config.ColumnName") as MockColNam,
            patch("core.configuration.config.get_config_item") as mock_get_config_item,
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

            result = await self.config_obj.get_configuration_from_db()

            self.assertIsNone(result)
            MockColNam.assert_called_once_with("user_id")
            MockConfiguration.get_matching_ids.assert_awaited_once_with(
                {mock_col: None}
            )
            mock_configuration.fetch.assert_awaited_once_with(id=mock_ids[0])
            self.assertEqual(self.config_obj._global_configuration, mock_configuration)
            mock_get_config_item.assert_called_once_with(
                mock_configuration.configuration_dict, Config.CONFIG_APP_USRMODE
            )
            self.assertEqual(MockApp.status_object.status, stat)

    async def test_401a_get_configuration_from_db_multi(self):
        await self._400_get_configuration_from_db(
            SetupConfigValues.MULTI_USER, Status.STATUS_MULTI_USER
        )

    async def test_401b_get_configuration_from_db_single(self):
        await self._400_get_configuration_from_db(
            SetupConfigValues.SINGLE_USER, Status.STATUS_SINGLE_USER
        )

    async def test_402_get_configuration_from_db_cfg_exc(self):
        with self.assertRaises(ConfigurationError):
            await self._400_get_configuration_from_db(None, None, mock_ids=[1, 2])

    async def test_403_get_configuration_from_db_no_mode(self):
        with self.assertRaises(ConfigurationError):
            await self._400_get_configuration_from_db(
                "MockUMode", Status.STATUS_SINGLE_USER
            )

    def _500_configuration(self, glbl, cmdln):
        if glbl:
            self.config_obj._global_configuration = Mock()
            self.config_obj._global_configuration.configuration_dict = glbl
            expct = glbl
        else:
            self.config_obj._global_configuration = None
            expct = {}
        self.config_obj._cmdline_configuration = cmdln

        result = self.config_obj.configuration()
        if cmdln:
            expct |= cmdln

        self.assertEqual(result, expct)

    def test_501_configuration(self):
        self._500_configuration({"global": "mock"}, {"cmdlin": "mick"})

    def test_502_configuration(self):
        self._500_configuration(None, {"cmdlin": "mick"})

    def test_503_configuration(self):
        self._500_configuration({"global": "mock"}, None)

    def test_504_configuration(self):
        self._500_configuration(None, None)
