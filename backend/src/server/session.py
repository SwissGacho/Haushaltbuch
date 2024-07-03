""" Manager for user sessions.
    A session is created by a WS connection without session token.
"""

from data.management.user import User
from server.ws_token import WSToken
from core.app_logging import getLogger
from database.sqlexpression import ColumnName

LOG = getLogger(__name__)


class Session:
    "a user session with limited lifetime"

    _all_sessions = []

    def __init__(self, username, conn_token, connection) -> None:
        Session._all_sessions.append(self)
        self._session_nbr = len(Session._all_sessions)
        self.LOG = getLogger(f"{Session.__module__}({self.session_id})")
        self.connections = [connection]
        self.token = WSToken()
        self.username = username
        self.user: User = None
        self._tokens = {conn_token} if conn_token else set()

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

    async def get_user_obj(self) -> User:
        "get Business Object for user logging in"
        username = self.username
        self.LOG.debug(f"get_user_obj for {username}")
        matching_users = await User.get_matching_ids({ColumnName("name"): username})
        matching_count = len(matching_users)
        self.LOG.debug(matching_count)
        if matching_count > 1:
            raise ValueError(f"multiple users with name '{username}' found")
        if matching_count < 1:
            raise PermissionError(f"User '{username} not found.")
        row_match = await matching_users.fetchone()
        user = User(id=row_match["id"])
        self.user = user
        return user

    def __repr__(self) -> str:
        return (
            f"<Session[#{self._session_nbr}](user={self.username},"
            f"token={self.token},conn_token={self._tokens})>"
        )


# LOG.debug("module imported")
