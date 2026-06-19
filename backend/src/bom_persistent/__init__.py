"""Data classes package
On import this package imports all data classes it contains.
Any classes having a name starting with '_' are ignored.
"""

import importlib
import pathlib

from core.app_logging import getLogger, log_exit, pprint_lines, VERBOSE_DEBUG

LOG = getLogger(__name__)


def import_business_objects():
    "Import all business object modules to register their classes"
    base_path = pathlib.Path(__file__).parent
    rel_paths = [p.relative_to(base_path.parent) for p in base_path.rglob("*.py")]
    modules = [
        ".".join(m)
        for m in [p.with_suffix("").parts for p in rel_paths if p.name[0] != "_"]
    ]
    bos: set[type] = set()
    for mod in modules:
        module = importlib.import_module(name=mod)
        for module_class in [
            cls
            for n, cls in module.__dict__.items()
            if isinstance(cls, type) and cls.__module__ == mod
        ]:
            if hasattr(module_class, "register_bo_class"):
                bos.add(module_class)
                module_class.register_bo_class()
    if LOG.isEnabledFor(VERBOSE_DEBUG):
        for line in [
            f"Registered {len(bos)} persistent business object classes:"
        ] + pprint_lines(
            {
                cls.__name__: (
                    [c.__name__ for c in cls.specialists]
                    if hasattr(cls, "specialists")
                    else set()
                )
                for cls in bos
            }
        ):
            LOG.log(VERBOSE_DEBUG, line)


import_business_objects()
log_exit(LOG)
