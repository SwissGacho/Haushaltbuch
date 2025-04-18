"""Test suite for configuration setup"""

import asyncio
import enum
import copy

import unittest
from unittest.mock import Mock, AsyncMock, patch, call, mock_open

from core.configuration.setup_config import (
    ConfigSetup,
    WAIT_AVAILABLE_TASK,
    WAIT_FAILURE_TASK,
    SetupConfigKeys,
    SetupConfigValues,
)
from core.base_objects import Config
from core.status import Status
from core.exceptions import ConfigurationError, DataError


class MockTask:
    def __init__(self) -> None:
        self.cancel = Mock()
        self.get_name = Mock()


class TestConfigSetup(unittest.IsolatedAsyncioTestCase):

    async def _100_wait_for_db(self, available=True):
        mock_done_task = MockTask()
        mock_done_task.get_name.return_value = (
            WAIT_AVAILABLE_TASK if available else WAIT_FAILURE_TASK
        )
        mock_pending_task = MockTask()
        mock_avail_task = "mock-available-task"
        mock_fail_task = "mock-failure-task"
        with (
            patch("core.configuration.setup_config.App") as MockApp,
            patch("asyncio.wait") as MockAsyncioWait,
            patch("asyncio.create_task") as MockAsyncioCreateTask,
        ):
            MockApp.db_request_restart.set = Mock(name="db_request_restart.set")
            MockApp.db_restart.wait = AsyncMock(name="db_restart.wait")
            MockApp.db_available.wait = Mock(name="db_available.wait")
            MockApp.db_failure.wait = Mock(name="db_failure.wait")
            MockAsyncioWait.return_value = ([mock_done_task], [mock_pending_task])
            MockAsyncioCreateTask.side_effect = [mock_avail_task, mock_fail_task]

            result = await ConfigSetup._wait_for_db()

            MockApp.db_request_restart.set.assert_called_once_with()
            MockApp.db_restart.wait.assert_awaited_once_with()
            self.assertEqual(
                MockAsyncioCreateTask.call_args_list,
                [
                    call(MockApp.db_available.wait(), name=WAIT_AVAILABLE_TASK),
                    call(MockApp.db_failure.wait(), name=WAIT_FAILURE_TASK),
                ],
            )
            MockAsyncioWait.assert_awaited_once_with(
                [mock_avail_task, mock_fail_task], return_when=asyncio.FIRST_COMPLETED
            )
            mock_pending_task.cancel.assert_called_once_with()
            mock_pending_task.get_name.assert_not_called()
            mock_done_task.get_name.assert_called_once_with()
            mock_done_task.cancel.assert_not_called()
            if available:
                self.assertTrue(result)
            else:
                self.assertFalse(result)

    async def test_101_wait_for_db_available(self):
        await self._100_wait_for_db()

    async def test_102_wait_for_db_available(self):
        await self._100_wait_for_db(available=False)

    def test_201_write_db_cfg_file(self):
        mock_setup_cfg = {"mock": {"setup": "cfg"}}
        mock_db_file = "mock.db.file.name"
        mock_db_cfg = {"Mock": "mick", "Mack": "muck"}
        mock_dump = "mock of dumped cfg"
        with (
            patch("core.configuration.setup_config.get_config_item") as mock_get_cfg,
            patch(
                "core.configuration.setup_config.open",
                mock_open(read_data="mock-found-file"),
            ) as mock_file_open,
            patch("json.dumps") as mock_json_dumps,
        ):
            mock_get_cfg.side_effect = [mock_db_file, mock_db_cfg]
            mock_json_dumps.return_value = mock_dump

            result = ConfigSetup._write_db_cfg_file(mock_setup_cfg)

            self.assertEqual(
                mock_get_cfg.call_args_list,
                [
                    call(mock_setup_cfg, SetupConfigKeys.DBCFG_CFG_FILE),
                    call(mock_setup_cfg, SetupConfigKeys.CFG_DBCFG),
                ],
            )
            mock_file_open.assert_called_once_with(file=mock_db_file, mode="w")
            mock_json_dumps.assert_called_once_with({Config.CONFIG_DB: mock_db_cfg})
            mock_file_open().write.assert_called_once_with(mock_dump)
            self.assertEqual(result, mock_db_file)

    def test_202_write_db_cfg_file_invalid_filename(self):
        mock_setup_cfg = {"mock": {"setup": "cfg"}}
        with (
            self.assertRaises(TypeError),
            patch("core.configuration.setup_config.get_config_item") as mock_get_cfg,
        ):
            mock_get_cfg.return_value = 0

            result = ConfigSetup._write_db_cfg_file(mock_setup_cfg)
        mock_get_cfg.assert_called_once_with(
            mock_setup_cfg, SetupConfigKeys.DBCFG_CFG_FILE
        )

    async def _300_create_or_update_global_configuration(self, mock_ids=[]):
        mock_cfg = {"mock": "configuration"}
        mock_ex_cfg = {"Mock": {"existing": "config"}}
        mock_col = "mock_colname"
        captured_args = []
        with (
            patch("core.configuration.setup_config.Configuration") as MockConfiguration,
            patch("core.configuration.setup_config.ColumnName") as MockColumnName,
            patch(
                "core.configuration.setup_config.update_dicts_recursively"
            ) as mock_update_dicts_recursively,
            patch("core.configuration.setup_config.Config") as MockConfig,
            patch(
                "core.configuration.setup_config.SetupConfigValues"
            ) as MockSetupConfigValues,
        ):
            MockConfig.CONFIG_APP = "MockApp"
            MockConfig.CONFIG_USR_MODE = "MockApp/usrmode"
            MockSetupConfigValues.SINGLE_USER = "mock-single"
            MockConfiguration.get_matching_ids = AsyncMock(return_value=mock_ids)
            MockColumnName.return_value = mock_col
            mock_cfg_object = Mock()
            mock_cfg_object.fetch = AsyncMock(return_value=mock_cfg_object)
            mock_cfg_object.store = AsyncMock()
            mock_cfg_object.configuration = copy.copy(mock_ex_cfg)
            MockConfiguration.return_value = mock_cfg_object

            def side_effect(target, source):
                captured_args.append((copy.deepcopy(target), source))
                target.clear()
                target.update(
                    {"mock": f"updated_{mock_update_dicts_recursively.call_count}"}
                )

            mock_update_dicts_recursively.side_effect = side_effect
            await ConfigSetup._create_or_update_global_configuration(mock_cfg)

        MockColumnName.assert_called_once_with("user_id")
        MockConfiguration.get_matching_ids.assert_awaited_once_with({mock_col: None})
        if len(mock_ids) == 0:
            self.assertEqual(mock_update_dicts_recursively.call_count, 1)
            MockConfiguration.assert_called_once_with(
                configuration={"mock": "updated_1"}
            )
            mock_cfg_object.fetch.assert_not_called()
            mock_cfg_object.fetch.assert_not_awaited()
        else:
            MockConfiguration.assert_called_once_with(id=mock_ids[0])
            mock_cfg_object.fetch.assert_awaited_once_with()
            self.assertEqual(mock_update_dicts_recursively.call_count, 2)
            self.assertEqual(
                captured_args[1],
                (
                    mock_ex_cfg,
                    {"mock": "updated_1"},
                ),
                msg="second call of update_dicts_recursively with wrong arguments",
            )
        self.assertEqual(
            captured_args[0],
            ({"MockApp": {"MockApp/usrmode": "mock-single"}}, mock_cfg),
            msg="first call of update_dicts_recursively with wrong arguments",
        )
        mock_cfg_object.store.assert_awaited_once_with()

    async def test_301_create_or_update_global_configuration_no_ids(self):
        await self._300_create_or_update_global_configuration([])

    async def test_302_create_or_update_global_configuration_one_id(self):
        await self._300_create_or_update_global_configuration([9])

    async def test_303_create_or_update_global_configuration_more_ids(self):
        mock_cfg = {"mock": "configuration"}
        mock_col = "mock_colname"
        with (
            patch("core.configuration.setup_config.Configuration") as MockConfiguration,
            patch("core.configuration.setup_config.ColumnName") as MockColumnName,
            self.assertRaises(ConfigurationError),
        ):
            MockConfiguration.get_matching_ids = AsyncMock(return_value=[1, 2])
            MockColumnName.return_value = mock_col

            await ConfigSetup._create_or_update_global_configuration(mock_cfg)

        MockColumnName.assert_called_once_with("user_id")
        MockConfiguration.get_matching_ids.assert_awaited_once_with({mock_col: None})

    async def _400_create_or_update_initial_user(self, mock_ids=None):
        class MockRole(enum.Flag):
            ADMIN = enum.auto()
            R2 = enum.auto()
            R3 = enum.auto()
            EXISTING = enum.auto()

        single_user = mock_ids is None
        if single_user:
            mock_ids = [1]
        mock_cfg = {"mock": "configuration"}
        mock_adm_usr = {"name": "mock-name", "password": "1234"}
        mock_col = "mock_colname"
        mock_ex_role = MockRole.EXISTING
        with (
            patch("core.configuration.setup_config.App") as MockApp,
            patch("core.configuration.setup_config.get_config_item") as mock_get_cfg,
            patch("core.configuration.setup_config.User") as MockUser,
            patch("core.configuration.setup_config.ColumnName") as MockColumnName,
            patch("core.configuration.setup_config.UserRole") as MockUserRole,
            patch("core.configuration.setup_config.SINGLE_USER_NAME", "<mock_single>"),
        ):
            MockApp.status = (
                Status.STATUS_SINGLE_USER if single_user else Status.STATUS_MULTI_USER
            )
            mock_get_cfg.return_value = mock_adm_usr
            MockUser.get_matching_ids = AsyncMock(return_value=mock_ids)
            MockColumnName.return_value = mock_col
            mock_usr_object = Mock()
            mock_usr_object.fetch = AsyncMock(return_value=mock_usr_object)
            mock_usr_object.store = AsyncMock()
            mock_usr_object.role = mock_ex_role
            mock_usr_object.password = mock_adm_usr["password"]
            MockUser.return_value = mock_usr_object
            MockUserRole.ADMIN = MockRole.ADMIN

            await ConfigSetup._create_or_update_initial_user(mock_cfg)

        if single_user:
            mock_get_cfg.assert_not_called()
        else:
            mock_get_cfg.assert_called_once_with(mock_cfg, SetupConfigKeys.ADM_USER)
        MockUser.get_matching_ids.assert_awaited_once_with(
            {mock_col: "<mock_single>" if single_user else mock_adm_usr["name"]}
        )
        if len(mock_ids) == 0:
            mock_usr_object.fetch.assert_not_awaited()
            MockUser.assert_called_once_with(
                name=mock_adm_usr["name"],
                password=mock_adm_usr["password"],
                role=MockRole.ADMIN,
            )
        elif len(mock_ids) == 1:
            MockUser.assert_called_once_with(id=mock_ids[0])
            mock_usr_object.fetch.assert_awaited_once_with()
            self.assertIn(MockRole.ADMIN, mock_usr_object.role)
            self.assertIn(MockRole.EXISTING, mock_usr_object.role)
            self.assertEqual(mock_usr_object.role, MockRole.ADMIN | MockRole.EXISTING)
        if single_user:
            self.assertIsNone(mock_usr_object.password)
        else:
            self.assertEqual(mock_usr_object.password, mock_adm_usr["password"])
        mock_usr_object.store.assert_awaited_once_with()

    async def test_401_create_or_update_initial_user_no_ids(self):
        await self._400_create_or_update_initial_user([])

    async def test_402_create_or_update_initial_user_one_id(self):
        await self._400_create_or_update_initial_user([47])

    async def test_403_create_or_update_initial_user_single(self):

        await self._400_create_or_update_initial_user()

    async def test_404_create_or_update_initial_user_invalid_cfg(self):
        mock_cfg = {"mock": "configuration"}
        with (
            patch("core.configuration.setup_config.App") as MockApp,
            patch("core.configuration.setup_config.get_config_item") as mock_get_cfg,
            patch("core.configuration.setup_config.User") as MockUser,
            self.assertRaises(TypeError) as mock_type_error,
        ):
            MockApp.status = Status.STATUS_MULTI_USER
            mock_get_cfg.return_value = 99
            MockUser.get_matching_ids = AsyncMock(return_value=[])
            await ConfigSetup._create_or_update_initial_user(mock_cfg)
        mock_get_cfg.assert_called_once_with(mock_cfg, SetupConfigKeys.ADM_USER)
        MockUser.get_matching_ids.assert_not_awaited()
        self.assertEqual(
            str(mock_type_error.exception),
            "initial user must be dict not <class 'int'>",
        )

    async def test_405_create_or_update_initial_user_more_ids(self):
        mock_cfg = {"mock": "configuration"}
        mock_adm_usr = {"name": "mock-name", "password": "1234"}
        mock_col = "mock_colname"
        with (
            patch("core.configuration.setup_config.App") as MockApp,
            patch("core.configuration.setup_config.get_config_item") as mock_get_cfg,
            patch("core.configuration.setup_config.User") as MockUser,
            patch("core.configuration.setup_config.ColumnName") as MockColumnName,
            self.assertRaises(DataError) as mock_data_error,
        ):
            MockApp.status = Status.STATUS_MULTI_USER
            mock_get_cfg.return_value = mock_adm_usr
            MockUser.get_matching_ids = AsyncMock(return_value=[22, 33])
            MockColumnName.return_value = mock_col

            await ConfigSetup._create_or_update_initial_user(mock_cfg)

        mock_get_cfg.assert_called_once_with(mock_cfg, SetupConfigKeys.ADM_USER)
        MockUser.get_matching_ids.assert_awaited_once_with(
            {mock_col: mock_adm_usr["name"]}
        )
        self.assertEqual(str(mock_data_error.exception), "Multiple users in DB")

    async def _500_setup_configuration(self, db_available=True):
        mock_cfg = {"mock": "configuration"}
        mock_path = Mock()
        mock_path_parent = "mockpath.parent"
        mock_path_name = "mockpath.name"
        mock_path.parent = mock_path_parent
        mock_path.name = mock_path_name
        mock_filename = "mock.file.path"
        mock_app_cfg = {"Mock": "App Config"}
        mocked_configuration = {Config.CONFIG_APP: mock_app_cfg}
        ConfigSetup._write_db_cfg_file = Mock(return_value=mock_filename)
        ConfigSetup._wait_for_db = AsyncMock(return_value=db_available)
        ConfigSetup._create_or_update_global_configuration = AsyncMock()
        ConfigSetup._create_or_update_initial_user = AsyncMock()
        with (
            patch("core.configuration.setup_config.get_config_item") as mock_get_cfg,
            patch("core.configuration.setup_config.DBConfig") as MockDBCfg,
            patch("core.configuration.setup_config.Path") as MockPath,
        ):
            MockPath.return_value = mock_path
            MockDBCfg.read_db_config_file = Mock()
            mock_get_cfg.return_value = mock_app_cfg
            # SetupConfigValues.MULTI_USER if multi else "other",

            await ConfigSetup.setup_configuration(mock_cfg)

        ConfigSetup._write_db_cfg_file.assert_called_once_with(setup_cfg=mock_cfg)
        self.assertEqual(MockPath.call_args_list, [call(mock_filename)] * 2)
        MockDBCfg.read_db_config_file.assert_called_once_with(
            [mock_path_parent], mock_path_name
        )
        mock_get_cfg.assert_called_once_with(mock_cfg, SetupConfigKeys.CFG_APP)
        ConfigSetup._wait_for_db.assert_awaited_once_with()
        if db_available:
            ConfigSetup._create_or_update_global_configuration.assert_awaited_once_with(
                app_configuration=mocked_configuration
            )
        else:
            ConfigSetup._create_or_update_global_configuration.assert_not_called()
        ConfigSetup._create_or_update_initial_user.assert_awaited_once_with(
            setup_cfg=mock_cfg
        )

    async def test_501_setup_configuration_db(self):
        await self._500_setup_configuration()

    async def test_502_setup_configuration_noDB(self):
        await self._500_setup_configuration(db_available=False)
