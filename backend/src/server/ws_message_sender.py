"""Mix-In class for objects that can send messages to a websocket connection."""

from core.app_logging import getLogger, Logger, log_exit

LOG: Logger = getLogger(__name__)


class WSMessageSender:
    """Mix-In class for objects that can send messages to a websocket connection."""

    def __init__(self, connection, *args, **kwargs):
        LOG.debug(f"WSMessageSender.__init__({connection=}, {args=}, {kwargs=})")
        self._connection = connection
        self._connection._register_message_sender(self)

    def cleanup(self):
        """Override this to release any resources."""
        raise NotImplementedError(
            "WSMessageSender.cleanup() must be overridden in the subclass"
        )

    def handle_connection_closed(self):
        """The connection calls this to notify this object that the connection is closed."""
        self.cleanup()

    async def send_message(self, message):
        """Send a message to the websocket connection."""
        LOG.debug(f"WSMessageSender.send_message({message=})")
        LOG.debug(f"     {self._connection=}")
        await self._connection.send_message(message)


log_exit(LOG)
