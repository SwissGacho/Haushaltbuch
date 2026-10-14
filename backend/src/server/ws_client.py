"""A WebSocket client represents a browser session.
Multiple ws-connections can originate from the same browser session."""

from typing import Union

from core.app_logging import (
    getLogger,
    log_exit,
    Logger,
    redact,
    VERBOSE_DEBUG,
    redact_str,
)

LOG: Logger = getLogger(__name__)

from core.exceptions import ClientNotFoundError
from server.ws_token import WSToken


class WSClient:
    """Container for a WebSocket client"""

    clients: dict[str, "WSClient"] = {}

    def __init__(self, client_token: str | None = None):
        self.user = None
        self.client_token: str | None = client_token
        self.websockets: list = []

    @classmethod
    def get_client(cls, client_token: str) -> Union["WSClient", None]:
        """This method returns a WSClient instance associated
            with the given client_token if it is valid.
        None is returned if the client_token is not valid.
        A ClientNotFoundError exception is raised if no existing
            client is found for the given valid client_token.
        """

        if not WSToken.check_token(client_token):
            if client_token in cls.clients:
                cls.clients.pop(client_token)
            return None
        if client_token in cls.clients:
            return cls.clients[client_token]
        raise ClientNotFoundError(
            "No existing client found for the given valid client_token"
        )


log_exit(LOG)
