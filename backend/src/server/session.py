""" Manager for user sessions.
    A session is created by a WS connection without session token.
"""

from asyncio import get_event_loop
from data.management.user import User
from server.ws_token import WSToken
from core.app_logging import getLogger

LOG = getLogger(__name__)


class Session:
    "a user session with limited lifetime"

    _all_sessions = []

    def __init__(self, username, conn_token, connection) -> None:
        Session._all_sessions.append(self)
        self._session_nbr = len(Session._all_sessions)
        self.connections = [connection]
        self.token = WSToken()
        user = self.get_user_obj(username)
        self.user = user
        self._tokens = {conn_token} if conn_token else set()
        self.LOG = getLogger(f"{Session.__module__}({self.session_id})")
        self.LOG.debug(f"created session for user {username}")

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

    async def get_user_obj(self, username)->User:
        "get Business Object for user logging in"
        self.LOG.debug(f"get_user_obj for {username}")
        print("get_user_obj")
        matchingUsers = await(User.get_matching_ids({"name": username}))
        matching_count = matchingUsers.rowcount
        if matching_count > 1:
            raise ValueError(f"multiple users with name '{username}' found")
        if matching_count == 1:
            rowMatch = matchingUsers.fetchone()
            user = User(id=rowMatch[0][0])
            return user
        if matching_count == 0:
            totalUsers = User.count_rows()
            if totalUsers > 0:
                raise ValueError(f"user '{username}' not found")
            user = User(name=username)
            user.store()
            return user

    def __repr__(self) -> str:
        return f"<Session[#{self._session_nbr}](user={self.user.name},token={self.token},conn_token={self._tokens})>"


# LOG.debug("module imported")
