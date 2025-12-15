"""Base class for WebSocket connections"""


class WSConnectionBase:
    "Base class for WebSocket connections"

    def _register_connection(self, key: str | None = None) -> None:
        raise NotImplementedError()

    def _register_message_sender(self, sender):
        raise NotImplementedError()

    def _unregister_connection(self):
        raise NotImplementedError()

    def _unregister_message_sender(self, sender):
        raise NotImplementedError()

    @property
    def connection_id(self):
        "get connection identifier"
        raise NotImplementedError()

    @property
    def session(self):
        "get/set session associated with connection"
        raise NotImplementedError()

    @session.setter
    def session(self, ses):
        raise NotImplementedError()

    async def _send(self, payload):
        raise NotImplementedError()

    async def send_message(self, message, status=False):
        "send a message to the client"
        raise NotImplementedError()

    async def send_message_to_component(self, comp, msg):
        "send a message to a specific component"
        raise NotImplementedError()

    async def start_connection(self):
        "say hello and expect Login"
        raise NotImplementedError()

    async def abort_connection(self, reason: str | None = None, token=None):
        "say goodbye"
        raise NotImplementedError()

    def connection_closed(self):
        "call when connection has been closed"
        raise NotImplementedError()

    async def handle_message(self, message):
        "accept a message from the client and trigger according actions"
        raise NotImplementedError()
