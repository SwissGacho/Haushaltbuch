""" Messages package
    On import this pakage import all Message classes it contains.
    Any classes having a name starting with '_' are ignored.
"""

import os
import glob
import importlib
import inspect


def split_path(path: str) -> list:
    "split path elements into a list"
    if path:
        split = split_path(os.path.dirname(path))
        split.append(os.path.basename(path))
        return split
    return []


base_path = os.path.dirname(__file__)
base_package = os.path.basename(base_path)
py_paths = glob.glob(
    f"{base_package}/**/*.py", root_dir=os.path.dirname(base_path), recursive=True
)
modules = [
    ".".join(m) for m in [split_path(p[:-3]) for p in py_paths] if m[-1:][0][0] != "_"
]
for mod in modules:
    module = importlib.import_module(name=mod)
