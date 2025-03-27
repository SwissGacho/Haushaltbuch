class SQLKeyManager:
    _last_key = 0

    def __init__(self):
        super().__init__()
        self._initialize_registry()

    def _generate_key(self):
        """Generate a new unique id for a parameter"""
        SQLKeyManager._last_key += 1
        return str(SQLKeyManager._last_key)

    def register_key(self, key="param"):
        """Register a new key for a parameter,
        substitute and return new key if key already exists"""
        if not hasattr(self, "_keys"):
            self._initialize_registry()
        if key in self._keys or key == "":
            key += self._generate_key()
        self._keys |= {key}
        return key

    def _initialize_registry(self):
        # self._last_key = 0
        self._keys: set[str] = set()
