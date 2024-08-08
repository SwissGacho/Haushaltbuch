""" Test suite for DB related configuration. """

import sys
import asyncio

import unittest
from unittest.mock import Mock, patch, mock_open, ANY, DEFAULT

from core.configuration.db_config import DBConfig
from core.base_objects import Config
from core.const import APPNAME

MOCK_HOME = "mock-home"
MOCK_CWD = "mock-cwd"
MOCK_PRNT = "mockparent_of:"
MOCK_JOINED = "/joined/"


class MockPath:
    def __init__(self, p, p2="") -> None:
        self.path = str(p)
        if p2:
            self.path += "/+/" + str(p2)
        self.parent = MOCK_PRNT + str(p)

    def __str__(self) -> str:
        return self.path

    def joinpath(self, s):
        return self.path + MOCK_JOINED + str(s)

    @classmethod
    def home(cls):
        return MOCK_HOME

    @classmethod
    def cwd(cls):
        return MOCK_CWD

    is_absolute = Mock(name="is_absolute")


class TestDBConfig(unittest.TestCase):

    def setUp(self) -> None:
        MockPath.is_absolute.reset_mock()
        DBConfig._cfg_searchpath = None
        DBConfig._db_locations = None
        DBConfig.db_configuration = None
        return super().setUp()

    def _200_create_cfg_searchpaths(self, win=True, mock_loc="mock_location"):
        def mock_getenv(env):
            return "MOCK_" + env

        def fail_getenv(_):
            self.fail("No getenv expected for Linux.")

        with (
            patch("core.configuration.db_config.App") as MockApp,
            patch("core.configuration.db_config.Path", new=MockPath),
            patch("platform.system") as mock_platf_sys,
            patch("os.getenv") as mock_os_getenv,
        ):
            mock_cfg_obj = Mock(name="config_object")
            MockApp.config_object = Mock(return_value=mock_cfg_obj)
            mock_cfg_obj.app_location = mock_loc
            mock_platf_sys.return_value = "Windows" if win else "Linux"
            mock_os_getenv.side_effect = mock_getenv if win else fail_getenv

            DBConfig._create_cfg_searchpaths()

            print(f"{DBConfig._cfg_searchpath=}")

            expct_srch = (
                [
                    MOCK_PRNT + mock_loc,
                    "MOCK_PROGRAMDATA" + MOCK_JOINED + APPNAME,
                    "MOCK_LOCALAPPDATA" + MOCK_JOINED + APPNAME,
                    MOCK_HOME,
                    MOCK_CWD,
                ]
                if win
                else [
                    MOCK_PRNT + mock_loc,
                    "/etc" + MOCK_JOINED + APPNAME,
                    "/opt" + MOCK_JOINED + APPNAME,
                    MOCK_HOME,
                    MOCK_CWD,
                ]
            )
            self.assertEqual(DBConfig._cfg_searchpath, expct_srch)
            self.assertEqual(DBConfig._db_locations, expct_srch[1:3])

    def test_201_create_cfg_searchpaths_win(self):
        self._200_create_cfg_searchpaths(win=True)

    def test_202_create_cfg_searchpaths_win_no_loc(self):
        self._200_create_cfg_searchpaths(win=True, mock_loc="")

    def test_203_create_cfg_searchpaths_linx(self):
        self._200_create_cfg_searchpaths(win=False)

    def test_204_create_cfg_searchpaths_linx_no_loc(self):
        self._200_create_cfg_searchpaths(win=False, mock_loc="")

    def test_301_cfg_searchpath(self):
        mock_paths = ["Mock-Paths"]
        DBConfig._cfg_searchpath = mock_paths
        with patch(
            "core.configuration.db_config.DBConfig._create_cfg_searchpaths"
        ) as mock_crt_paths:
            result = DBConfig.cfg_searchpath()
        mock_crt_paths.assert_not_called()
        self.assertEqual(result, mock_paths)

    def test_302_cfg_searchpath_no_paths(self):
        mock_paths = None
        DBConfig._cfg_searchpath = mock_paths
        with patch(
            "core.configuration.db_config.DBConfig._create_cfg_searchpaths"
        ) as mock_crt_paths:
            result = DBConfig.cfg_searchpath()
        mock_crt_paths.assert_called_once_with()
        self.assertEqual(result, [])

    def test_401_db_locations(self):
        mock_paths = ["Mock-Paths"]
        DBConfig._db_locations = mock_paths
        with patch(
            "core.configuration.db_config.DBConfig._create_cfg_searchpaths"
        ) as mock_crt_paths:
            result = DBConfig.db_locations()
        mock_crt_paths.assert_not_called()
        self.assertEqual(result, mock_paths)

    def test_402_db_locations_no_paths(self):
        mock_paths = None
        DBConfig._db_locations = mock_paths
        with patch(
            "core.configuration.db_config.DBConfig._create_cfg_searchpaths"
        ) as mock_crt_paths:
            result = DBConfig.db_locations()
        mock_crt_paths.assert_called_once_with()
        self.assertEqual(result, [])

    def test_501_set_db_configuration(self):
        mock_config = {"Mick": "Mack"}
        DBConfig.set_db_configuration(mock_config)
        self.assertEqual(DBConfig.db_configuration, mock_config)

    def _600_read_db_config_file(
        self,
        cfg_s_pth=[MockPath("mopa1"), MockPath("mopa2")],
        cls_cfg_s_pth=[MockPath("moclspa1"), MockPath("moclspa2")],
        db_cfg_fn="",
        cmd_fn="",
        not_found_count=1,
    ):
        mock_cfg_file = "Mock-CONFIG_DBCFG_FILE"
        mock_cfg_from_file = {"mock": "CFG from file"}
        with (
            patch(
                "core.configuration.db_config.DBConfig.cfg_searchpath"
            ) as mock_srch_path,
            patch("core.configuration.db_config.App") as MockApp,
            patch("core.configuration.db_config.Path", new=MockPath),
            patch(
                "core.configuration.db_config.open",
                mock_open(read_data="mock-found-file"),
            ) as mock_file_open,
            patch("json.load") as mock_json_load,
        ):
            mock_srch_path.return_value = cls_cfg_s_pth
            MockApp.get_config_item = Mock(return_value=cmd_fn)
            db_cfg_abs = (db_cfg_fn or cmd_fn or "")[-3:] == "abs"
            MockPath.is_absolute.return_value = db_cfg_abs
            path_len = 1 if db_cfg_abs else len(cfg_s_pth or cls_cfg_s_pth)
            side_effect = [FileNotFoundError] * not_found_count
            side_effect.append(DEFAULT)
            mock_file_open.side_effect = side_effect
            mock_json_load.return_value = mock_cfg_from_file

            DBConfig.read_db_config_file(
                cfg_searchpath=cfg_s_pth, dbcfg_filename=db_cfg_fn
            )

            if not cfg_s_pth:
                mock_srch_path.assert_called_once_with()
            else:
                mock_srch_path.assert_not_called()
            MockApp.get_config_item.assert_called_once_with(
                Config.CONFIG_DBCFG_FILE, ""
            )
            MockPath.is_absolute.assert_called_once_with()
            self.assertEqual(
                mock_file_open.call_count, min(path_len, not_found_count + 1)
            )
            mock_file_open.assert_called_with(ANY, encoding="utf-8")
            par1 = mock_file_open.call_args.args[0]
            self.assertTrue(isinstance(par1, MockPath))
            if path_len > not_found_count:
                mock_json_load.assert_called_once_with(mock_file_open.return_value)
                self.assertEqual(DBConfig.db_configuration, mock_cfg_from_file)
            else:
                mock_json_load.assert_not_called()
                self.assertIsNone(
                    DBConfig.db_configuration,
                    msg="Configuration should not be set",
                )

    def test_601_read_db_config_file(self):
        self._600_read_db_config_file()

    def test_602_read_db_config_file_serchpath(self):
        self._600_read_db_config_file(
            cfg_s_pth=[
                MockPath("mopa1"),
                MockPath("mopa2"),
                MockPath("mopa3"),
                MockPath("mopa4"),
            ],
            not_found_count=2,
        )

    def test_603_read_db_config_file_class_searchpath(self):
        self._600_read_db_config_file(
            cfg_s_pth=None,
            cls_cfg_s_pth=[
                MockPath("mopa1"),
                MockPath("mopa2"),
                MockPath("mopa3"),
                MockPath("mopa4"),
            ],
            not_found_count=3,
        )

    def test_604_read_db_config_file_not_found(self):
        self._600_read_db_config_file(
            not_found_count=5,
        )

    def test_605_read_db_config_file_cfgfile(self):
        self._600_read_db_config_file(
            db_cfg_fn="mock_filename",
            not_found_count=1,
        )

    def test_606_read_db_config_file_abs_cfgfile(self):
        self._600_read_db_config_file(
            db_cfg_fn="/absolute/mock_filename.abs",
            not_found_count=0,
        )

    def test_607_read_db_config_file_cmdln_file(self):
        self._600_read_db_config_file(
            cmd_fn="mock_filename_from_cmdline",
            not_found_count=1,
        )

    def test_608_read_db_config_file_abs_cmdln_file(self):
        self._600_read_db_config_file(
            cmd_fn="/absolute/mock_filename_from_cmdline.abs",
            not_found_count=0,
        )

    def test_609_read_db_config_file_invalid_cmdln_file(self):
        with self.assertRaises(TypeError):
            self._600_read_db_config_file(
                cmd_fn=None,
                not_found_count=1,
            )
