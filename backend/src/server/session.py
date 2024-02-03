""" Manager for user sessions.
    A session is created by a WS connection without session token.
"""

from server.ws_token import WSToken
from core.app_logging import getLogger

LOG = getLogger(__name__)


class Session:
    "a user session with limited lifetime"

    _all_sessions = []

    def __init__(self, user, conn_token, connection) -> None:
        Session._all_sessions.append(self)
        self._session_nbr = len(Session._all_sessions)
        self.connections = [connection]
        self.token = WSToken()
        self.user = user
        self._tokens = {conn_token} if conn_token else set()
        self.LOG = getLogger(f"{Session.__module__}({self.session_id})")
        self.LOG.debug(f"created session for user {user}")

    @property
    def session_id(self):
        return f"ses #{self._session_nbr}"

    @classmethod
    def get_session_from_token(
        cls, ses_token: str, conn_token: str, session_user=None, connection=None
    ):
        "find session by session or any connection token"
        local_LOG = (
            getLogger(f"{Session.__module__}({connection.connection_id})")
            if connection
            else LOG
        )
        for ses in cls._all_sessions:
            if (ses.token == ses_token or conn_token in ses._tokens) and (
                session_user is None or ses.user == session_user
            ):
                ses.LOG.debug(
                    f"got session by {'session'if ses.token == ses_token else 'connection'} token"
                )
                return ses

    def add_connection(self, connection):
        if not connection in self.connections:
            self.connections.append(connection)
        return self.connections.index(connection)

    def add_token(self, token: str):
        "add a connection token to session"
        if token:
            self._tokens.add(token)

    def __repr__(self) -> str:
        return f"<Session[#{self._session_nbr}](user={self.user},token={self.token},conn_token={self._tokens})>"


# LOG.debug("module imported")
