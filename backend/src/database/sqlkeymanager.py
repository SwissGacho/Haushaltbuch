class SQLKeyManager:

    def __init__(self):
        super().__init__()
        self._last_key = 0
        self._keys: set[str] = set()

    def _generate_key(self):
        """Generate a new unique id for a parameter"""
        self._last_key += 1
        return str(self._last_key)

    def register_key(self, key="param"):
        if key in self._keys or key == "":
            key += self._generate_key()
        self._keys |= {key}
        return key
