"""
Utility functions without import of other app modules.
"""


def update_dicts_recursively(
    target: dict | None, source: dict, source_overrides_target: bool = True
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
                target[key] = value


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
