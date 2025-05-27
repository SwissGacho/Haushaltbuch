"""Common utilities"""

import sys
from typing import Callable, Union
import importlib.metadata
from packaging.version import Version


from core.base_objects import ConfigDict
from persistance.bo_descriptors import BODict


class EnvironmentError(Exception):
    """Custom exception for environment requirement errors."""

    pass


def check_environment():
    """Check if the environment meets the requirements for running the application."""

    def check_lib_version(lib: str, required: Version, raise_error: bool = True):
        """Check if the installed library version meets the required version."""
        try:
            installed_version = Version(importlib.metadata.version(lib))
        except importlib.metadata.PackageNotFoundError:
            raise EnvironmentError(f"The '{lib}' package is not installed.")

        if installed_version < required:
            if raise_error:
                raise EnvironmentError(
                    f"'{lib}' version {required} or higher required, "
                    f"but found: {installed_version}"
                )
            return installed_version, False
        return installed_version, True

    # Python-Version
    required_python_version = (3, 12)
    if sys.version_info < required_python_version:
        raise EnvironmentError(
            f"Python {required_python_version[0]}.{required_python_version[1]} or higher required, "
            f"but found: {sys.version_info.major}.{sys.version_info.minor}"
        )

    check_lib_version("websockets", Version("15.0.1"))


class _classproperty:
    "Property on class level (only getter implemented)"

    def __init__(self, fget: Callable) -> None:
        self.fget = fget

    def __get__(self, owner_self, owner_cls=None):
        return self.fget(owner_cls)


def get_config_item(cfg: dict, key: str):
    "Extract a value from a dict of dicts using 'key' as '/' separated path."
    if not cfg:
        return None
    sub_cfg = cfg
    for key_part in key.split("/"):
        if not isinstance(sub_cfg, dict):
            return None
        sub_cfg = sub_cfg.get(key_part, {})
    return sub_cfg


def update_dicts_recursively(
    target: Union[ConfigDict, BODict], source: Union[ConfigDict, BODict]
):
    "Merge source into target"
    if not (isinstance(target, dict) and isinstance(source, dict)):
        raise TypeError("Configurations must be mappings.")
    for key, value in source.items():
        if isinstance(tgt_dict := target.get(key), dict) and isinstance(value, dict):
            update_dicts_recursively(tgt_dict, value)
        else:
            target[key] = value
