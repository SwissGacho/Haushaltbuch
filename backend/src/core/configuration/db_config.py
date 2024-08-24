""" Handle DB configuration """

import os
import platform
import json
from pathlib import Path
from typing import Optional
from asyncio import Lock

from core.const import APPNAME
from core.app import App
from core.base_objects import BaseObject, Config
from core.app_logging import getLogger

LOG = getLogger(__name__)


class DBConfig(BaseObject):
    "Handling of the DB configuration"
    _cfg_searchpath: Optional[list[str]] = None
    _db_locations: Optional[list[str]] = None
    db_configuration: Optional[dict] = None
    db_config_lock = Lock()

    @classmethod
    def _create_cfg_searchpaths(cls):
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

        cls._cfg_searchpath = []
        cls._db_locations = []
        _append(
            cls._cfg_searchpath, Path(App.config_object().app_location or "").parent
        )
        match platform.system():
            case "Windows":
                _append(cls._cfg_searchpath, _getenv("PROGRAMDATA"))
                _append(cls._cfg_searchpath, _getenv("LOCALAPPDATA"))
                _append(cls._db_locations, _getenv("PROGRAMDATA"))
                _append(cls._db_locations, _getenv("LOCALAPPDATA"))
            case "Linux":
                _append(cls._cfg_searchpath, Path("/etc").joinpath(APPNAME))
                _append(cls._cfg_searchpath, Path("/opt").joinpath(APPNAME))
                _append(cls._db_locations, Path("/etc").joinpath(APPNAME))
                _append(cls._db_locations, Path("/opt").joinpath(APPNAME))
        _append(cls._cfg_searchpath, Path.home())
        _append(cls._cfg_searchpath, Path.cwd())

    @classmethod
    def cfg_searchpath(cls) -> list[str]:
        "Searchpath for DB configuration file"
        if not cls._cfg_searchpath:
            cls._create_cfg_searchpaths()
        return cls._cfg_searchpath or []

    @classmethod
    def db_locations(cls) -> list[str]:
        "Suggested locations for DB file"
        if not cls._db_locations:
            cls._create_cfg_searchpaths()
        return cls._db_locations or []

    @classmethod
    def set_db_configuration(cls, config: dict):
        "Change the DBconfiguration"
        cls.db_configuration = config

    @classmethod
    def read_db_config_file(
        cls, cfg_searchpath: Optional[list[Path]] = None, dbcfg_filename: str = ""
    ):
        "Determine DB configuration from DB config file or commandline"
        # LOG.debug(f"DBConfig.read_db_config_file({cfg_searchpath=}, {dbcfg_filename=})")
        searchpath = cfg_searchpath or cls.cfg_searchpath() or []
        cmdline_dbcfg_filename = App.get_config_item(Config.CONFIG_DBCFG_FILE, "")
        if not isinstance(cmdline_dbcfg_filename, (Path, str)):
            raise TypeError("DB configuration filename from commandline")
        dbcfg_file = Path(dbcfg_filename or cmdline_dbcfg_filename)
        # LOG.debug(f"DBConfig.read_db_config_file: {dbcfg_file=}")
        try:
            for filename in (
                [dbcfg_file]
                if dbcfg_file.is_absolute()
                else [Path(path, dbcfg_file) for path in searchpath]
            ):
                # LOG.debug(f"Searching file: {str(filename)}")
                try:
                    with open(filename, encoding="utf-8") as cfg_file:
                        db_config_from_cfg_file = json.load(cfg_file)
                    # LOG.debug(f"Found DB configuration: {db_config_from_cfg_file}")
                    cls.db_configuration = db_config_from_cfg_file
                    return
                except FileNotFoundError:
                    continue
            LOG.info(f"configuration file {dbcfg_file} not found.")
        except json.JSONDecodeError as exc:
            LOG.warning(f"Unable to decode configuration from {dbcfg_file}: {exc}")
        except (IsADirectoryError, NotADirectoryError, PermissionError, OSError) as exc:
            LOG.warning(f"Unable to read configuration from {dbcfg_file}: {exc}")
