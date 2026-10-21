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
    pprint_lines,
)

LOG: Logger = getLogger(__name__)

from core.exceptions import ClientNotFoundError
from server.ws_token import WSToken
from bom_persistent.management.user import GenericUser


class WSClient:
    """Container for a WebSocket client"""

    clients: dict[str, "WSClient"] = {}

    def __init__(self, client_token: str):
        self._user: GenericUser | None = None
        self.client_token: str = client_token
        self._authorized: bool = False
        self.websockets: list = []
        WSClient.clients[client_token] = self
        LOG.debug(f"Initialized WSClient with client_token: {redact_str(client_token)}")
        LOG.log(VERBOSE_DEBUG, "WSClient.clients:")
        for line in pprint_lines(
            {
                redact_str(k): v.user.display_name if v.user else "<no user>"
                for k, v in WSClient.clients.items()
            }
        ):
            LOG.log(VERBOSE_DEBUG, f"  {line}")

    @property
    def user(self) -> GenericUser | None:
        return self._user

    @user.setter
    def user(self, value: GenericUser | None) -> None:
        LOG.log(
            VERBOSE_DEBUG,
            "Setting user"
            f" from: {self._user.display_name if self._user else '<no user>'}"
            f" to: {value.display_name if value else '<no user>'}",
        )
        self._user = value

    @property
    def authorized(self) -> bool:
        return self._authorized

    @authorized.setter
    def authorized(self, value: bool) -> None:
        LOG.log(
            VERBOSE_DEBUG, f"Setting authorized from: {self._authorized} to: {value}"
        )
        self._authorized = value

    @classmethod
    def get_client(cls, client_token: str) -> Union["WSClient", None]:
        """This method returns a WSClient instance associated
            with the given client_token if it is valid.
        None is returned if the client_token is not valid.
        A ClientNotFoundError exception is raised if no existing
            client is found for the given valid client_token.
        """

        if not WSToken.check_token(client_token):
            # client token is not valid, remove it from the clients dictionary if it exists
            if client_token in cls.clients:
                cls.clients.pop(client_token)
            return None
        if client_token in cls.clients:
            return cls.clients[client_token]
        raise ClientNotFoundError(
            "No existing client found for the given valid client_token"
        )


log_exit(LOG)
