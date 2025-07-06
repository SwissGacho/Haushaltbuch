"""Mix-In class for objects that can send messages to a websocket connection."""

from core.app_logging import getLogger

LOG = getLogger(__name__)


class WSMessageSender:
    """Mix-In class for objects that can send messages to a websocket connection. When the connection is closed, the objects are notified and can clean up their resources."""

    def __init__(self, connection, *args, **kwargs):
        LOG.debug(
            f"+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
        )
        LOG.debug(f"WSMessageSender.__init__({connection=}, {args=}, {kwargs=})")
        self._connection = connection
        self._connection._register_message_sender(self)

    def cleanup(self):
        """Override this to release any resources."""
        pass

    def handle_connection_closed(self):
        """Called when the connection is closed. Override to clean up resources."""
        self.cleanup()

    def close(self):
        """Called by the owner (WebSocket) to clean up this sender."""
        self._connection._unregister_message_sender(self)
        self.cleanup()

    async def send_message(self, message):
        LOG.debug(f"WSMessageSender.send_message({message=})")
        LOG.debug(f"{self._connection=}")
        await self._connection.send_message(message)
