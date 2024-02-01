""" Handle a websocket connection """

from core.app import App
from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from messages.login import HelloMessage, ByeMessage
from core.app_logging import getLogger

LOG = getLogger(__name__)


class ConnectionClosed(Exception):
    pass


class WS_Connection:
    """Websocket connection created by the client"""

    def __init__(self, websocket) -> None:
        self._socket = websocket
        self._session = None
        self._token = WSToken()

    async def _send(self, payload):
        await self._socket.send(payload)
        # LOG.debug(f"sent message: {payload}")

    async def send_message(self, message: Message, status=False):
        "Send a message to the client"
        if status:
            message.add({MessageAttribute.WS_ATTR_STATUS: App.status})
        await self._send(message.serialize())

    async def start_connection(self):
        "say hello and expect Login"
        await self.send_message(HelloMessage(token=self._token, status=App.status))
        try:
            msg = Message(json_message=await self._socket.recv())
            if msg.message_type() == MessageType.WS_TYPE_LOGIN:
                await self.handle_message(msg)
            else:
                await self.abort_connection("Login expected")
        except ConnectionClosed:
            raise
        except Exception:
            LOG.error("Login failed.")
            return False
        else:
            return True

    async def abort_connection(self, reason: str = None, token=None, status=False):
        "say goodbye"
        await self.send_message(ByeMessage(token=token, reason=reason, status=status))
        raise ConnectionClosed

    async def handle_message(self, message: Message):
        "accept a message from the client and trigger according actions"
        # LOG.debug(f"handle {message=} {message.message=} {message.token=}")
        if message.token != self._token:
            LOG.warning(f"Received invalid token {message.token}")
            await self.abort_connection(reason="Invalid Token", token=message.token)
        else:
            await message.handle_message(self)
