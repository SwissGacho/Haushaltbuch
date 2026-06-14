"""Handle File configuration"""

import os
import platform
import json
import pprint
from pathlib import Path
from typing import Optional
from asyncio import Lock

from core.app_logging import (
    getLogger,
    log_exit,
    redact,
    DEBUG,
    VERBOSE_DEBUG,
    pprint_lines,
)

LOG = getLogger(__name__)

from core.const import APPNAME
from core.app import App
from core.base_objects import BaseObject, Config

# import transient_data  # pylint: disable=unused-import


class FileConfig(BaseObject):
    "Handling of the configuration read from the config file."

    _cfg_searchpath: Optional[list[str]] = None
    _db_locations: Optional[list[str]] = None
    file_config_file_path: Optional[Path] = None
    db_config_lock = Lock()

    @classmethod
    def _create_cfg_searchpaths(cls):
        "Possible locations for file-configuration and SQLite-DB-file"

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
        "Searchpath for file configuration file"
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
        "The path of the file configuration file, if found"
        return cls.file_config_file_path

    @classmethod
    def read_file_config_file(
        cls, cfg_searchpath: Optional[list[Path]] = None, filecfg_filename: str = ""
    ) -> Optional[dict]:
        "Determine configuration from config file or commandline"
        LOG.debug(
            f"FileConfig.read_file_config_file({cfg_searchpath=}, {filecfg_filename=})"
        )
        searchpath = cfg_searchpath or cls.cfg_searchpath() or []
        cmdline_filecfg_filename = App.get_config_item(Config.CONFIG_FILECFG_FILE, "")
        if not isinstance(cmdline_filecfg_filename, (Path, str)):
            raise TypeError("Invalid file configuration filename from commandline")
        filecfg_file = Path(filecfg_filename or cmdline_filecfg_filename)
        cls.file_config_file_path = None
        if LOG.isEnabledFor(DEBUG):
            LOG.debug(
                f"FileConfig.read_file_config_file: {filecfg_file=} "
                f"({'absolute' if filecfg_file.is_absolute() else 'relative'}) "
                f"{len(searchpath)=}"
            )
        if LOG.isEnabledFor(VERBOSE_DEBUG):
            for pth in searchpath:
                LOG.log(VERBOSE_DEBUG, f" - {pth}")
        for filename in (
            [filecfg_file]
            if filecfg_file.is_absolute()
            else ([Path(path, filecfg_file) for path in searchpath])
        ):
            LOG.log(VERBOSE_DEBUG, f"Searching file: {str(filename)}")
            try:
                with open(filename, encoding="utf-8") as cfg_file:
                    cfg_from_cfg_file = json.load(cfg_file)
            except FileNotFoundError:
                continue
            except (
                IsADirectoryError,
                NotADirectoryError,
                PermissionError,
                OSError,
            ) as exc:
                LOG.error(f"Unable to read configuration from {filecfg_file}: {exc}")
                return None
            except json.JSONDecodeError as exc:
                LOG.error(f"Unable to decode configuration from {filecfg_file}: {exc}")
                return None
            cls.file_config_file_path = filename
            LOG.debug(f"File configuration file found: {cls.file_config_file_path}")
            if LOG.isEnabledFor(VERBOSE_DEBUG):
                for line in pprint_lines(cfg_from_cfg_file):
                    LOG.log(VERBOSE_DEBUG, f" - {line}")
            return cfg_from_cfg_file
        LOG.info(f"configuration file {filecfg_file} not found.")

    @classmethod
    def write_file_config_file(cls, config: dict) -> bool:
        "Write the given configuration to the file configuration file"
        if not cls.file_config_file_path:
            LOG.error(
                "No file configuration file path set. Cannot write configuration."
            )
            return False
        try:
            with open(cls.file_config_file_path, "w", encoding="utf-8") as cfg_file:
                json.dump(config, cfg_file, indent=4)
            LOG.debug(f"File configuration written to {cls.file_config_file_path}")
            return True
        except (IsADirectoryError, NotADirectoryError, PermissionError, OSError) as exc:
            LOG.warning(
                f"Unable to write configuration to {cls.file_config_file_path}: {exc}"
            )
            return False


log_exit(LOG)
