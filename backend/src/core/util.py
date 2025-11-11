"""Common utilities"""

import sys
from typing import Callable, Type, TypeVar, Generic, overload, Union, Optional, Any
import importlib.metadata
from packaging.version import Version

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from core.base_objects import ConfigDict
from business_objects.bo_descriptors import BODict


class EnvironmentError(Exception):
    """Custom exception for environment requirement errors."""


def check_environment():
    """Check if the environment meets the requirements for running the application."""

    def check_lib_version(lib: str, required: Version, raise_error: bool = True):
        """Check if the installed library version meets the required version."""
        try:
            installed_version = Version(importlib.metadata.version(lib))
        except importlib.metadata.PackageNotFoundError as exc:
            if raise_error:
                raise EnvironmentError(
                    f"The '{lib}' package is not installed."
                ) from exc
            return None

        if installed_version < required:
            raise EnvironmentError(
                f"'{lib}' version {required} or higher required, "
                f"but found: {installed_version}"
            )
        return installed_version

    # Python-Version
    required_python_version = (3, 12)
    if sys.version_info < required_python_version:
        raise EnvironmentError(
            f"Python {required_python_version[0]}.{required_python_version[1]} or higher required, "
            f"but found: {sys.version_info.major}.{sys.version_info.minor}"
        )

    check_lib_version("websockets", Version("15.0.1"))
    check_lib_version("aiosqlite", Version("0.21.0"), raise_error=False)
    check_lib_version("aiomysql", Version("0.2.0"), raise_error=False)


_T = TypeVar("_T")


class _classproperty(Generic[_T]):
    "Property on class level (only getter implemented)"

    def __init__(self, fget: Callable[[Type[Any]], _T]) -> None:
        self.fget = fget
        self.fset: Optional[Callable[[Type[Any], _T], None]] = None

    @overload
    def __get__(self, instance: None, owner_cls: Type[Any]) -> _T: ...

    @overload
    def __get__(self, instance: object, owner_cls: Type[Any]) -> _T: ...

    def __get__(self, instance, owner_cls: Optional[Type[Any]] = None) -> _T:
        if owner_cls is None:
            owner_cls = type(instance)
        return self.fget(owner_cls)

    def __set__(self, instance, value: _T) -> None:
        if self.fset is None:
            raise AttributeError("can't set attribute")
        # In case of class access, owner_self is the class itself,
        # In case of instance access, owner_self is the instance
        owner_cls = instance if isinstance(instance, type) else type(instance)
        self.fset(owner_cls, value)

    def setter(self, fset: Callable[[Any, _T], None]) -> "_classproperty[_T]":
        """Decorator to define a setter for the classproperty"""
        self.fset = fset
        return self

    def __class_getitem__(cls, item):  # for generic compatibility
        return cls


def get_config_item(cfg: dict | None, key: str):
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
    target: Optional[Union[ConfigDict, BODict]], source: Union[ConfigDict, BODict]
):
    "Merge source into target"
    if target is None:
        return
    if not (isinstance(target, dict) and isinstance(source, dict)):
        raise TypeError("Configurations must be mappings.")
    for key, value in source.items():
        if isinstance(tgt_dict := target.get(key), dict) and isinstance(value, dict):
            update_dicts_recursively(tgt_dict, value)
        else:
            target[key] = value


log_exit(LOG)
