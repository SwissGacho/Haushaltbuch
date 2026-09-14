"""Manager for user sessions.
A session is created by a WS connection without session token.
"""

from typing import Self, Optional
from bom_persistent.management.user import User

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
from server.ws_connection_base import SessionBase
from server.ws_token import WSToken

# from server.ws_connection import WS_Connection


class Session(SessionBase):
    "a user session with limited lifetime"

    _all_sessions: list[Self] = []
    _next_session_nbr = 0
    _client_sessions: dict[str, list[Self]] = {}

    def __init__(
        self,
        user: User,
        conn_token: Optional[WSToken],
        connection,  #: "WS_Connection"
        client_token: str | None = None,
    ) -> None:
        local_LOG = self.local_logger(connection)
        local_LOG.debug(
            f"creating new session (user={user.name},  client-token': {redact_str(client_token)})"
        )
        Session._all_sessions.append(self)
        self._session_nbr = Session._next_session_nbr
        Session._next_session_nbr += 1
        self.connections = [connection]
        inactive_seconds_timeout = (
            (App.get_config_item(Config.CONFIG_APP_SESSION_TIMEOUT) or 2) * 60 * 60
        )  # default: 2 hours
        self.token = WSToken(inactive_seconds_timeout=inactive_seconds_timeout)
        self._user: User = user
        self._tokens: set[WSToken] = {conn_token} if conn_token else set()
        if client_token:
            if client_token not in Session._client_sessions:
                local_LOG.debug(
                    f"detected new client with client-token': {redact_str(client_token)}"
                )
                Session._client_sessions[client_token] = []
            Session._client_sessions[client_token].append(self)
            local_LOG.debug(
                f"added session for client-token': {redact_str(client_token)}"
            )
            if local_LOG.isEnabledFor(VERBOSE_DEBUG):
                local_LOG.log(VERBOSE_DEBUG, "current client sessions:")
                for line in pprint_lines(
                    (
                        {
                            redact_str(k): [str(s) for s in v]
                            for k, v in Session._client_sessions.items()
                        }
                    )
                ):
                    local_LOG.log(VERBOSE_DEBUG, f"  {line}")
        else:
            local_LOG.debug("new session without client-token provided")

    @property
    def session_id(self):
        "get session identifier"
        return f"ses #{self._session_nbr}"

    @classmethod
    def local_logger(self, connection=None) -> ContextLogger | Logger:
        return (
            get_context_logger(LOG, **connection.connection_context)
            if connection
            else LOG
        )

    @classmethod
    def get_session_from_token(
        cls,
        ses_token: str | None,
        conn_token: str | None,
        client_token: str | None = None,
        session_user: User | None = None,
        connection=None,
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
            client_token in cls._client_sessions
            and cls._client_sessions[client_token]
            and (
                session_user is None
                or session_user == cls._client_sessions[client_token][0].user
            )
        ):
            return cls(
                user=cls._client_sessions[client_token][0].user,
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
    def user(self) -> User:
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
