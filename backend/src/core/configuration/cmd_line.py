""" Parser for the commandline. """

import argparse

from pathlib import Path
from core.app_logging import getLogger

LOG = getLogger(__name__)
from core.const import APPDESC, APPNAME, DBCFG_FILE_NAME
from core.base_objects import ConfigDict


def _parse_dict(arg: str) -> ConfigDict:
    # LOG.debug(f"parse_dict({arg})")
    elements = arg.split("=")
    if len(elements) != 2:
        raise argparse.ArgumentTypeError(
            f"Argument should contain exactly one '=': {arg}"
        )
    keys = elements[0].split(".")
    result: ConfigDict = {keys[-1]: elements[1]}
    while keys := keys[:-1]:
        result = {keys[-1]: result}
    # LOG.debug(f"parse_dict()->{result=}")
    return result


def _update_dicts_recursively(target: ConfigDict, source: ConfigDict):
    if not (isinstance(target, dict) and isinstance(source, dict)):
        raise TypeError("Configurations must be mappings.")
    for key, value in source.items():
        if isinstance(tgt_dict := target.get(key), dict) and isinstance(value, dict):
            _update_dicts_recursively(tgt_dict, value)
        else:
            target[key] = value


def parse_commandline(dbcfg_file_key: str) -> ConfigDict:
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
        "cfg", nargs="*", type=_parse_dict, help="Configuration items: key[.key]*=value"
    )
    args = parser.parse_args()
    result = {dbcfg_file_key: args.dbcfg_file}
    for cfg in args.cfg:
        _update_dicts_recursively(result, cfg)
    return result
