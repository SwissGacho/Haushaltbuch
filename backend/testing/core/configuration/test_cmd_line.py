"""Test suite for commandline related configuration"""

# pyright: reportPrivateUsage=false

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
        parse_dict = getattr(core.configuration.cmd_line, "_parse_dict")
        result = parse_dict(mock_arg)
        self.assertEqual(result, mock_cfg)

    def test_102_parse_dict_no_value(self):
        mock_arg = "lvl1.lvl2"
        parse_dict = getattr(core.configuration.cmd_line, "_parse_dict")
        with self.assertRaises(ArgumentTypeError):
            parse_dict(mock_arg)

    def _300_parse_commandline(self, args=None):
        if args is None:
            args = []

        with (
            patch("argparse.ArgumentParser") as MockParser,
            patch("core.configuration.cmd_line._parse_dict") as mock_parse_dict,
            patch(
                "core.configuration.cmd_line.update_dicts_recursively"
            ) as mock_update_dicts,
            patch(
                "core.configuration.cmd_line._is_test_runner_context"
            ) as mock_testrun,
        ):
            mock_testrun.return_value = True
            mock_parsed = Mock()
            mock_parsed.dbcfg_file = "mock-path"
            mock_parsed.cfg = args
            mock_parser = Mock(name="parser")
            mock_parser.add_argument = Mock()
            mock_parser.parse_known_args = Mock(return_value=(mock_parsed, ["unknown"]))
            MockParser.return_value = mock_parser
            mock_res_dbcfg = {"mockKey": "mock-path"}
            parsed_cfgs = [{"mock": arg} for arg in args if "=" in arg]
            mock_parse_dict.side_effect = parsed_cfgs

            result = core.configuration.cmd_line.CommandLine.parse_commandline(
                "mockKey"
            )

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
            type=str,
            help=ANY,
        )
        self.assertEqual(mock_parser.add_argument.call_args_list, [call1, call2])
        mock_parser.parse_known_args.assert_called_once_with()
        self.assertEqual(
            mock_parse_dict.call_args_list,
            [call(arg) for arg in args if "=" in arg],
        )
        self.assertEqual(
            mock_update_dicts.call_args_list,
            [call(mock_res_dbcfg, cfg) for cfg in parsed_cfgs],
        )
        self.assertEqual(result, mock_res_dbcfg)

    def test_301_parse_commandline_no_args(self):
        self._300_parse_commandline()

    def test_302_parse_commandline_with_args(self):
        self._300_parse_commandline(["mock.arg1=foo", "invalid", "mock.arg2=bar"])

    def test_303_parse_commandline_rejects_unknown_non_test_context(self):
        with (
            patch("argparse.ArgumentParser") as MockParser,
            patch(
                "core.configuration.cmd_line._is_test_runner_context"
            ) as mock_testrun,
        ):
            mock_testrun.return_value = False
            mock_parsed = Mock()
            mock_parsed.dbcfg_file = "mock-path"
            mock_parsed.cfg = []
            mock_parser = Mock(name="parser")
            mock_parser.add_argument = Mock()
            mock_parser.parse_known_args = Mock(return_value=(mock_parsed, ["--weird"]))
            mock_parser.error = Mock(side_effect=SystemExit(2))
            MockParser.return_value = mock_parser

            with self.assertRaises(SystemExit):
                core.configuration.cmd_line.CommandLine.parse_commandline("mockKey")

            mock_parser.error.assert_called_once_with("unrecognized arguments: --weird")

    def test_304_parse_commandline_allows_unknown_test_context(self):
        with (
            patch("argparse.ArgumentParser") as MockParser,
            patch(
                "core.configuration.cmd_line._is_test_runner_context"
            ) as mock_testrun,
        ):
            mock_testrun.return_value = True
            mock_parsed = Mock()
            mock_parsed.dbcfg_file = "mock-path"
            mock_parsed.cfg = []
            mock_parser = Mock(name="parser")
            mock_parser.add_argument = Mock()
            mock_parser.parse_known_args = Mock(return_value=(mock_parsed, ["--weird"]))
            mock_parser.error = Mock()
            MockParser.return_value = mock_parser

            result = core.configuration.cmd_line.CommandLine.parse_commandline(
                "mockKey"
            )

            mock_parser.error.assert_not_called()
            self.assertEqual(result, {"mockKey": "mock-path"})
