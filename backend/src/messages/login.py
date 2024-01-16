""" Login related messages
"""

from server.ws_token import WSToken
from server.session import Session
from messages.message import Message, MessageType, MessageAttribute


class HelloMessage(Message):
    "provide connection token to client"

    def __init__(self, token: WSToken, status: str = None) -> None:
        super().__init__(msg_type=MessageType.WS_TYPE_HELLO, token=token, status=status)


class LoginMessage(Message):
    "incoming login message"

    @classmethod
    def message_type(cls):
        return MessageType.WS_TYPE_LOGIN.value

    async def handle_message(self, connection):
        "handle login message"
        print(f"handle {self=} {self.message=}")
        user = self.message.get(MessageAttribute.WS_ATTR_USER.value)
        token = self.message.get(MessageAttribute.WS_ATTR_TOKEN.value)
        connection._session = (
            Session.get_session_from_token(
                ses_token=self.message.get(MessageAttribute.WS_ATTR_SES_TOKEN),
                conn_token=self.message.get(MessageAttribute.WS_ATTR_PREV_TOKEN),
            )
            or Session(user, token)
            if user
            else None
        )
        await connection.send_message(
            WelcomeMessage(token=token, ses_token=connection._session.token)
        )


class WelcomeMessage(Message):
    "provide session token after successful login"

    def __init__(
        self, token: WSToken, ses_token: WSToken = None, status: str = None
    ) -> None:
        super().__init__(
            msg_type=MessageType.WS_TYPE_WELCOME, token=token, status=status
        )
        self.message |= {MessageAttribute.WS_ATTR_SES_TOKEN: ses_token}
