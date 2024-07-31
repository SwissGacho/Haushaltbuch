""" Test suite for commandline related configuration """

from pathlib import Path
from argparse import ArgumentTypeError
import unittest
from unittest.mock import Mock, patch, call, ANY

from core.const import APPNAME, APPDESC
import core.configuration.cmd_line


class TestCmdLine(unittest.TestCase):

    def test_101_parse_dict(self):
        mock_arg = "lvl1.lvl2=val"
        mock_cfg = {"lvl1": {"lvl2": "val"}}
        result = core.configuration.cmd_line._parse_dict(mock_arg)
        self.assertEqual(result, mock_cfg)

    def test_102_parse_dict_no_value(self):
        mock_arg = "lvl1.lvl2"
        with self.assertRaises(ArgumentTypeError):
            core.configuration.cmd_line._parse_dict(mock_arg)

    def test_201_update_dicts_recursively(self):
        mock_tgt = {
            "lvl1a": "valt1",
            "lvl1b": {
                "lvl2a": "valt2",
                "lvl2b": {"lvl3": "val3"},
            },
            "lvl1c": "oldt1",
        }
        mock_src = {"lvl1c": "new1", "lvl1b": {"lvl2c": "valt2c"}}
        expct = {
            "lvl1a": "valt1",
            "lvl1b": {"lvl2a": "valt2", "lvl2b": {"lvl3": "val3"}, "lvl2c": "valt2c"},
            "lvl1c": "new1",
        }
        core.configuration.cmd_line._update_dicts_recursively(
            target=mock_tgt, source=mock_src
        )
        self.assertEqual(mock_tgt, expct)

    def test_202_update_dicts_recursively_no_dict(self):
        with self.assertRaises(TypeError):
            core.configuration.cmd_line._update_dicts_recursively(
                target="val", source={}
            )
        with self.assertRaises(TypeError):
            core.configuration.cmd_line._update_dicts_recursively(
                target={}, source=None
            )
        # no exception:
        core.configuration.cmd_line._update_dicts_recursively(target={}, source={})

    def _300_parse_commandline(self, args=[]):
        with (
            patch("argparse.ArgumentParser") as MockParser,
            patch("core.configuration.cmd_line._parse_dict") as mock_parse_dict,
            patch(
                "core.configuration.cmd_line._update_dicts_recursively"
            ) as mock_update_dicts,
        ):
            mock_parsed = Mock
            mock_parsed.dbcfg_file = "mock-path"
            mock_parsed.cfg = args
            mock_parser = Mock(name="parser")
            mock_parser.add_argument = Mock()
            mock_parser.parse_args = Mock(return_value=mock_parsed)
            MockParser.return_value = mock_parser
            mock_res_dbcfg = {"mockKey": "mock-path"}
            mock_cfg_from_args = ["res" + a for a in args]
            mock_update_dicts.side_effect = mock_cfg_from_args

            result = core.configuration.cmd_line.parse_commandline("mockKey")

        MockParser.assert_called_once_with(prog=APPNAME, description=APPDESC)
        call1 = call(
            "-d",
            "--db-configuration-file",
            dest="dbcfg_file",
            type=Path,
            default=ANY,
            help=ANY,
        )
        call2 = call(
            "cfg",
            nargs="*",
            type=mock_parse_dict,
            help=ANY,
        )
        self.assertEqual(mock_parser.add_argument.call_args_list, [call1, call2])
        mock_parser.parse_args.assert_called_once_with()
        self.assertEqual(
            mock_update_dicts.call_args_list, [call(mock_res_dbcfg, a) for a in args]
        )
        self.assertEqual(result, mock_res_dbcfg)

    def test_301_parse_commandline_no_args(self):
        self._300_parse_commandline()

    def test_302_parse_commandline_with_args(self):
        self._300_parse_commandline(["mock-arg1", "mock-arg2"])
