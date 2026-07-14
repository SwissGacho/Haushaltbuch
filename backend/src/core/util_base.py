"""
Utility functions without import of other app modules.
"""

from typing import Any


def update_dicts_recursively(
    target: dict[str, Any] | None,
    source: dict[str, Any],
    source_overrides_target: bool = True,
):
    """Merge source into target.
    If source_overrides_target is True, source values will override target values,
    otherwise only missing keys in target will be updated."""
    if target is None:
        return
    if not (isinstance(target, dict) and isinstance(source, dict)):
        raise TypeError("Configurations must be mappings.")
    for key, value in source.items():
        if isinstance(tgt_dict := target.get(key), dict) and isinstance(value, dict):
            update_dicts_recursively(tgt_dict, value, source_overrides_target)
        else:
            if source_overrides_target or key not in target:
                target[key] = value.copy() if isinstance(value, dict) else value


def get_config_item(cfg: dict[str, Any] | None, key: str, default: Any = None):
    "Extract a value from a dict of dicts using 'key' as '/' separated path."
    if not cfg:
        return default
    if not key:
        return cfg
    current = cfg
    for part in key.split("/"):
        if not isinstance(current, dict):
            return default
        if part not in current:
            return default
        current = current[part]

    return current
