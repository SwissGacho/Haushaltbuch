"""Manager for user sessions.
A session is created by a WS connection without session token.
"""

from typing import Self, Optional
from bom_persistent.management.user import GenericUser

from core.app_logging import (
    ContextLogger,
    get_context_logger,
    getLogger,
    log_exit,
    Logger,
    pprint_lines,
    redact,
    redact_str,
    DEBUG,
    VERBOSE_DEBUG,
)

LOG: Logger = getLogger(__name__)

from core.app import App
from core.base_objects import Config
from core.exceptions import TokenExpiredError
from server.ws_connection_base import SessionBase, WSConnectionBase
from server.ws_token import WSToken

# from server.ws_connection import WS_Connection


class Session(SessionBase):
    "a user session with limited lifetime"

    _all_sessions: list[Self] = []
    _next_session_nbr = 0
    session_clients: dict[str, GenericUser] = {}

    def __init__(
        self,
        user: GenericUser,
        conn_token: Optional[WSToken],
        connection: WSConnectionBase | None,
        client_token: str | None = None,
    ) -> None:
        local_LOG = self.local_logger(connection)
        local_LOG.debug(
            f"creating new session (user={user.name},  client-token': {redact_str(client_token)})"
        )
        Session._all_sessions.append(self)
        self._session_nbr = Session._next_session_nbr
        Session._next_session_nbr += 1
        self.connections = [connection] if connection else []
        hours = App.get_config_item(Config.CONFIG_APP_SESSION_TIMEOUT, default=2)
        inactive_seconds_timeout = int(
            (hours if isinstance(hours, (int, float)) else 2) * 60 * 60
        )  # default: 2 hours
        self.token = WSToken(inactive_seconds_timeout=inactive_seconds_timeout)
        self._user: GenericUser = user
        self._tokens: set[WSToken] = {conn_token} if conn_token else set()
        if client_token:
            if client_token not in Session.session_clients:
                local_LOG.debug(
                    f"detected new client with client-token': {redact_str(client_token)}"
                )
                Session.session_clients[client_token] = user
            if local_LOG.isEnabledFor(VERBOSE_DEBUG):
                local_LOG.log(VERBOSE_DEBUG, "current clients:")
                for line in pprint_lines(
                    {redact_str(k): str(v) for k, v in Session.session_clients.items()}
                ):
                    local_LOG.log(VERBOSE_DEBUG, f"  {line}")
        else:
            local_LOG.debug("new session without client-token provided")

    @property
    def session_id(self):
        "get session identifier"
        return f"ses #{self._session_nbr}"

    @classmethod
    def local_logger(
        cls, connection: WSConnectionBase | None = None
    ) -> ContextLogger | Logger:
        return (
            get_context_logger(LOG, **connection.connection_context)
            if connection
            else LOG
        )

    @classmethod
    def cleanup_expired_client_tokens(cls) -> None:
        """Remove client-token mappings whose tokens are no longer valid."""
        for client_token in tuple(cls.session_clients):
            if not WSToken.check_token(client_token):
                del cls.session_clients[client_token]

    @classmethod
    def get_session_from_token(
        cls,
        ses_token: str | None,
        conn_token: str | None,
        client_token: str | None = None,
        session_user: GenericUser | None = None,
        connection: WSConnectionBase | None = None,
    ):
        "find session by session or any connection token"
        local_LOG = cls.local_logger(connection)
        if ses_token and not WSToken.check_token(ses_token):
            raise TokenExpiredError("Session expired.")
        if conn_token and not WSToken.check_token(conn_token):
            raise TokenExpiredError("Previous connection expired.")
        if client_token and not WSToken.check_token(client_token):
            client_token = None
        if ses_token or conn_token:
            for ses in cls._all_sessions:
                if (ses.token == ses_token or conn_token in ses.conn_tokens) and (
                    session_user is None or ses.user == session_user
                ):
                    local_LOG.debug(
                        f"got session by {'session'if ses.token == ses_token else 'connection'} token"
                    )
                    return ses
        if not client_token:
            local_LOG.debug("no session found for given tokens")
            return None
        if (
            client_token in cls.session_clients
            and cls.session_clients[client_token]
            and (
                session_user is None
                or session_user == cls.session_clients[client_token]
            )
        ):
            return cls(
                user=cls.session_clients[client_token],
                conn_token=None,
                connection=connection,
                client_token=client_token,
            )
        return None

    @property
    def conn_tokens(self) -> set[str]:
        "get all connection tokens of session"
        return {token.token for token in self._tokens}

    @property
    def user(self) -> GenericUser:
        "get user associated with session"
        return self._user

    def add_connection(self, connection) -> int:
        "add a connection to session"
        if connection not in self.connections:
            self.connections.append(connection)
        return self.connections.index(connection)

    def add_token(self, token: str):
        "add a connection token to session"
        if token:
            self._tokens.add(WSToken(token))

    def __str__(self) -> str:
        return f"Session[#{self._session_nbr}]"

    def __repr__(self) -> str:
        return (
            f"<Session[#{self._session_nbr}](user={self.user},"
            f"token={self.token},conn_token={self._tokens})>"
        )


log_exit(LOG)
