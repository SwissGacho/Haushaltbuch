""" Login related messages
"""

from server.ws_token import WSToken
from server.session import Session
from messages.message import Message, MessageType, MessageAttribute
from core.app_logging import getLogger
from core.status import Status
from core.app import App
from data.management.user import User
from database.sqlexpression import ColumnName

LOG = getLogger(__name__)


class HelloMessage(Message):
    "provide connection token to client"

    def __init__(self, token: WSToken, status: str = None) -> None:
        super().__init__(msg_type=MessageType.WS_TYPE_HELLO, token=token, status=status)


class LoginMessage(Message):
    "incoming login message"

    @classmethod
    def message_type(cls):
        return MessageType.WS_TYPE_LOGIN

    async def handle_message(self, connection):
        "handle login message"
        LOG = getLogger(  # pylint: disable=invalid-name,redefined-outer-name
            f"{LoginMessage.__module__})"
        )
        # pylint: disable=comparison-with-callable
        user = self.message.get(
            MessageAttribute.WS_ATTR_USER,
            ("<single user>" if App.status == Status.STATUS_SINGLE_USER else None),
        )
        token = self.message.get(MessageAttribute.WS_ATTR_TOKEN)
        try:
            session = Session.get_session_from_token(
                ses_token=self.message.get(MessageAttribute.WS_ATTR_SES_TOKEN),
                conn_token=self.message.get(MessageAttribute.WS_ATTR_PREV_TOKEN),
            ) or (Session(user, token, connection) if user else None)
            if not session:
                raise PermissionError(f"Failed to create session for user '{user}'")
            await session.get_user_obj()
            connection.session = session
            LOG = getLogger(  # pylint: disable=invalid-name
                f"{LoginMessage.__module__}({connection.connection_id})"
            )
            await connection.send_message(
                WelcomeMessage(token=token, ses_token=session.token)
            )
            LOG.debug("login successful")
        except PermissionError:
            await connection.abort_connection(reason="Access denied")
        except ValueError as exc:
            raise RuntimeError("Login Failure.") from exc


class WelcomeMessage(Message):
    "provide session token after successful login"

    def __init__(
        self, token: WSToken, ses_token: WSToken = None, status: str = None
    ) -> None:
        super().__init__(
            msg_type=MessageType.WS_TYPE_WELCOME, token=token, status=status
        )
        self.message |= {MessageAttribute.WS_ATTR_SES_TOKEN: ses_token}


class ByeMessage(Message):
    "provide info why the connection will now be closed"

    def __init__(
        self, token: WSToken = None, reason: str = "Error", status: str = None
    ) -> None:
        super().__init__(msg_type=MessageType.WS_TYPE_BYE, token=token, status=status)
        self.message |= {MessageAttribute.WS_ATTR_REASON: reason}


# LOG.debug("module imported")
