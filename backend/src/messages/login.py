""" Login related messages
"""

from server.ws_token import WSToken
from server.session import Session
from messages.message import Message, MessageType, MessageAttribute
from core.app_logging import getLogger

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
        LOG.debug(f"handle {self=} {self.message=}")
        user = self.message.get(MessageAttribute.WS_ATTR_USER)
        token = self.message.get(MessageAttribute.WS_ATTR_TOKEN)
        connection._session = (
            Session.get_session_from_token(
                ses_token=self.message.get(MessageAttribute.WS_ATTR_SES_TOKEN),
                conn_token=self.message.get(MessageAttribute.WS_ATTR_PREV_TOKEN),
            )
            or Session(user, token)
            if user
            else None
        )
        if session:
            connection._session = session
            await connection.send_message(
                WelcomeMessage(token=token, ses_token=connection._session.token)
            )
        else:
            await connection.abort_connection(reason="Access denied")


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
