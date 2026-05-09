"""
Parser for the commandline.
"""

import argparse
import os
import sys

from pathlib import Path
from typing import Any


from core.util_base import update_dicts_recursively
from core.const import APPDESC, APPNAME, DBCFG_FILE_NAME

# from core.base_objects import ConfigDict


def _parse_dict(arg: str):
    # LOG.debug(f"parse_dict({arg})")
    elements = arg.split("=")
    if len(elements) != 2:
        raise argparse.ArgumentTypeError(
            f"Argument should contain exactly one '=': {arg}"
        )
    keys = elements[0].split(".")
    result = {keys[-1]: elements[1]}
    while keys := keys[:-1]:
        result = {keys[-1]: result}
    # LOG.debug(f"parse_dict()->{result=}")
    return result


def _is_test_runner_context() -> bool:
    "Return True when invoked from a known test runner context."
    if "PYTEST_CURRENT_TEST" in os.environ:
        return True

    argv_text = " ".join(sys.argv).lower()
    if "pytest" in argv_text or "unittest" in argv_text:
        return True

    return "pytest" in sys.modules


class CommandLine:
    "Parse the commandline for configuration overrides"

    parsed_commandline = {}

    @classmethod
    def parse_commandline(cls, dbcfg_file_key: str):
        "Parse the commandline for configuration overrides"
        parser = argparse.ArgumentParser(prog=APPNAME, description=APPDESC)
        parser.add_argument(
            "-d",
            "--db-configuration-file",
            dest="dbcfg_file",
            type=Path,
            default=Path(DBCFG_FILE_NAME),
            help="Name of the database configuration file (default: %(default)s)",
        )
        parser.add_argument(
            "cfg",
            nargs="*",
            type=str,
            help="Configuration items: key[.key]*=value",
        )
        args, unknown = parser.parse_known_args()
        if unknown and not _is_test_runner_context():
            parser.error(f"unrecognized arguments: {' '.join(unknown)}")

        cls.parsed_commandline = {dbcfg_file_key: args.dbcfg_file}
        for arg in args.cfg:
            if "=" not in arg:
                continue
            cfg = _parse_dict(arg)
            update_dicts_recursively(cls.parsed_commandline, cfg)
        return cls.parsed_commandline

    @classmethod
    def get_commandline_config(cls) -> dict[str, Any]:
        return cls.parsed_commandline
