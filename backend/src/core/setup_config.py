""" Helper functions for the configuration of the setup procedure """

import os
import platform
from pathlib import Path
import argparse

from core.app_logging import getLogger, logExit

LOG = getLogger(__name__)

from core.const import APPNAME, APPDESC, DBCFG_FILE_NAME


def cfg_searchpaths(app_location: str) -> tuple[list[Path], list[Path]]:
    "Possible locations for DB-configuration and SQLite-DB-file"

    def _append(l: list[str], p: str | Path):
        s = str(p)
        if s not in l:
            l.append(s)

    def _getenv(e: str) -> Path:
        p = os.getenv(e)
        if not p:
            raise EnvironmentError
        return Path(p).joinpath(APPNAME)

    cfg_searchpath = []
    db_locations = []
    _append(cfg_searchpath, Path(app_location).parent)
    match platform.system():
        case "Windows":
            _append(cfg_searchpath, _getenv("PROGRAMDATA"))
            _append(cfg_searchpath, _getenv("LOCALAPPDATA"))
            _append(db_locations, _getenv("PROGRAMDATA"))
            _append(db_locations, _getenv("LOCALAPPDATA"))
        case "Linux":
            _append(cfg_searchpath, Path("/etc").joinpath(APPNAME))
            _append(cfg_searchpath, Path("/opt").joinpath(APPNAME))
            _append(db_locations, Path("/etc").joinpath(APPNAME))
            _append(db_locations, Path("/opt").joinpath(APPNAME))
    _append(cfg_searchpath, Path.home())
    _append(cfg_searchpath, Path.cwd())
    return cfg_searchpath, db_locations


def _parse_dict(arg: str) -> dict:
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


def _update_dicts(target: dict, source: dict):
    for key, value in source.items():
        if isinstance(target.get(key), dict) and isinstance(value, dict):
            _update_dicts(target.get(key), value)
        else:
            target[key] = value


def parse_commandline(dbcfg_file_key: str) -> dict:
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
    parser.add_argument("cfg", nargs="*", type=_parse_dict)
    args = parser.parse_args()
    result = {dbcfg_file_key: args.dbcfg_file}
    for cfg in args.cfg:
        _update_dicts(result, cfg)
    return result


logExit(LOG)
