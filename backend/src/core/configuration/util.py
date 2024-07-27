""" Configuration utilities """


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
