""" Data classes package
    On import this pakage import all data classes it contains.
    Any classes having a name starting with '_' are ignored.
"""

import importlib
import pathlib

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)


def import_business_objects():
    base_path = pathlib.Path(__file__).parent
    rel_paths = [p.relative_to(base_path.parent) for p in base_path.rglob("*.py")]
    modules = [
        ".".join(m)
        for m in [p.with_suffix("").parts for p in rel_paths if p.name[0] != "_"]
    ]
    for mod in modules:
        module = importlib.import_module(name=mod)
        for module_class in [
            cls
            for n, cls in module.__dict__.items()
            if isinstance(cls, type) and cls.__module__ == mod
        ]:
            if hasattr(module_class, "register_persistant_class"):
                module_class.register_persistant_class()


import_business_objects()
log_exit(LOG)
