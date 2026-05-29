"""Handle DB configuration"""

import os
import platform
import json
import pprint
from pathlib import Path
from typing import Optional
from asyncio import Lock

from core.app_logging import getLogger, log_exit, redact, VERBOSE_DEBUG

LOG = getLogger(__name__)

from core.const import APPNAME
from core.app import App
from core.base_objects import BaseObject, Config

# import transient_data  # pylint: disable=unused-import


class FileConfig(BaseObject):
    "Handling of the DB configuration"

    _cfg_searchpath: Optional[list[str]] = None
    _db_locations: Optional[list[str]] = None
    db_config_file_path: Optional[Path] = None
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
    def file_config_file(cls) -> Optional[Path]:
        "The path of the DB configuration file, if found"
        return cls.db_config_file_path

    @classmethod
    def read_file_config_file(
        cls, cfg_searchpath: Optional[list[Path]] = None, dbcfg_filename: str = ""
    ) -> Optional[dict]:
        "Determine DB configuration from DB config file or commandline"
        LOG.debug(
            f"FileConfig.read_file_config_file({cfg_searchpath=}, {dbcfg_filename=})"
        )
        searchpath = cfg_searchpath or cls.cfg_searchpath() or []
        cmdline_dbcfg_filename = App.get_config_item(Config.CONFIG_DBCFG_FILE, "")
        if not isinstance(cmdline_dbcfg_filename, (Path, str)):
            raise TypeError("DB configuration filename from commandline")
        dbcfg_file = Path(dbcfg_filename or cmdline_dbcfg_filename)
        LOG.debug(f"FileConfig.read_file_config_file: {dbcfg_file=}")
        try:
            for filename in (
                [dbcfg_file]
                if dbcfg_file.is_absolute()
                else [Path(path, dbcfg_file) for path in searchpath]
            ):
                LOG.log(VERBOSE_DEBUG, f"Searching file: {str(filename)}")
                try:
                    with open(filename, encoding="utf-8") as cfg_file:
                        cfg_from_cfg_file = json.load(cfg_file)
                    cls.db_config_file_path = filename
                    LOG.debug(f"DB configuration file found: {cls.db_config_file_path}")
                    if LOG.isEnabledFor(VERBOSE_DEBUG):
                        for line in pprint.pformat(
                            redact(cfg_from_cfg_file), indent=4, width=120, compact=True
                        ).splitlines():
                            LOG.log(VERBOSE_DEBUG, f" - {line}")
                    return cfg_from_cfg_file
                except FileNotFoundError:
                    continue
            LOG.info(f"configuration file {dbcfg_file} not found.")
        except json.JSONDecodeError as exc:
            LOG.warning(f"Unable to decode configuration from {dbcfg_file}: {exc}")
        except (IsADirectoryError, NotADirectoryError, PermissionError, OSError) as exc:
            LOG.warning(f"Unable to read configuration from {dbcfg_file}: {exc}")

    @classmethod
    def write_file_config_file(cls, config: dict) -> bool:
        "Write the given configuration to the DB configuration file"
        if not cls.db_config_file_path:
            LOG.error("No DB configuration file path set. Cannot write configuration.")
            return False
        try:
            with open(cls.db_config_file_path, "w", encoding="utf-8") as cfg_file:
                json.dump(config, cfg_file, indent=4)
            LOG.debug(f"DB configuration written to {cls.db_config_file_path}")
            return True
        except (IsADirectoryError, NotADirectoryError, PermissionError, OSError) as exc:
            LOG.warning(
                f"Unable to write configuration to {cls.db_config_file_path}: {exc}"
            )
            return False


log_exit(LOG)
