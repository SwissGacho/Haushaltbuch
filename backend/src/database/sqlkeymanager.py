from hmac import new
from typing import Any, TypedDict
import re


class SQL_Dict(TypedDict):
    """Dictionary for an SQL query and its parameters."""

    query: str
    params: dict[str, Any]


class SQLKeyManager:

    def __init__(self):
        super().__init__()
        self._initialize_registry()
        self.params = {}

    def _initialize_registry(self):
        self._last_key = 0
        self._keys: set[str] = set()
        self._aliases: dict[str, set[str]] = {}

    def _generate_key(self) -> str:
        """Generate a new unique id for a parameter"""
        self._last_key += 1
        return str(self._last_key)

    def _register_key(self, key="param") -> str:
        """Register a new key for a parameter,
        substitute and return new key if key already exists"""
        if not hasattr(self, "_keys"):
            self._initialize_registry()
        new_key = key
        while new_key in self._keys or new_key == "":
            new_key = key + self._generate_key()
        self._keys |= {new_key}
        return new_key

    def _create_param(self, proposed_key: str, value) -> str:
        if self.params.get(proposed_key) == value:
            return proposed_key
        for key in self._aliases.get(proposed_key, set()):
            if self.params[key] == value:
                return key
        final_key = self._register_key(proposed_key)
        self.params[final_key] = value
        self._aliases[proposed_key] = self._aliases.get(proposed_key, set()) | {
            final_key
        }
        self._aliases[proposed_key].add(final_key)
        return final_key

    def merge_params(self, query: str, params: dict[str, Any]) -> str:
        for key, value in params.items():
            if not key in query:
                continue
            final_key = self._create_param(key, value)
            query = re.sub(rf":{key}\b", f":{final_key}", query)
        return query
