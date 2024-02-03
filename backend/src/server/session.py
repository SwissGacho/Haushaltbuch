""" Manager for user sessions.
    A session is created by a WS connection without session token.
"""

from server.ws_token import WSToken
from core.app_logging import getLogger

LOG = getLogger(__name__)


class Session:
    "a user session with limited lifetime"

    _all_sessions = []

    def __init__(self, user, conn_token) -> None:
        Session._all_sessions.append(self)
        self._session_nbr = len(Session._all_sessions)
        self.token = WSToken()
        self.user = user
        self._tokens = {conn_token} if conn_token else set()

    @classmethod
    def get_session_from_token(cls, ses_token: str, conn_token: str, session_user=None):
        "find session by session or any connection token"
        for ses in cls._all_sessions:
            if (ses.token == ses_token or conn_token in ses._tokens) and (
                session_user is None or ses.user == session_user
            ):
                return ses

    def add_token(self, token: str):
        "add a connection token to session"
        if token:
            self._tokens.add(token)

    def __repr__(self) -> str:
        return (
            f"<Session(user={self.user},token={self.token},conn_token={self._tokens})>"
        )


# LOG.debug("module imported")
