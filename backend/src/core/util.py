""" Common utilities """

from typing import Callable

from core.base_objects import ConfigDict


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


class _classproperty:
    "Property on class level (only getter implemented)"

    def __init__(self, fget: Callable) -> None:
        self.fget = fget

    def __get__(self, owner_self, owner_cls=None):
        return self.fget(owner_cls)


def update_dicts_recursively(target: ConfigDict, source: ConfigDict):
    "Merge source into target"
    if not (isinstance(target, dict) and isinstance(source, dict)):
        raise TypeError("Configurations must be mappings.")
    for key, value in source.items():
        if isinstance(tgt_dict := target.get(key), dict) and isinstance(value, dict):
            update_dicts_recursively(tgt_dict, value)
        else:
            target[key] = value
