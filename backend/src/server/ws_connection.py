""" Handle a websocket connection """

from core.exceptions import ConnectionClosed
from core.app import App
from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from messages.login import HelloMessage, ByeMessage
from core.app_logging import getLogger

LOG = getLogger(__name__)


class WS_Connection:
    """Websocket connection created by the client"""

    def __init__(self, websocket, sock_nbr) -> None:
        self._socket = websocket
        self._session = None
        self._conn_nbr = sock_nbr
        self._token = WSToken()
        self.LOG = getLogger(f"{WS_Connection.__module__}({self.connection_id})")

    @property
    def connection_id(self):
        if self._session:
            return f"{self._session.session_id},conn #{self._conn_nbr}"
        else:
            return str(self._conn_nbr)

    @property
    def session(self):
        return self._session

    @session.setter
    def session(self, ses):
        self._session = ses
        self._conn_nbr = ses.add_connection(self)
        self.LOG = getLogger(f"{WS_Connection.__module__}({self.connection_id})")

    async def _send(self, payload):
        await self._socket.send(payload)
        # self.LOG.debug(f"sent message: {payload}")

    async def send_message(self, message: Message, status=False):
        "Send a message to the client"
        if status:
            message.add({MessageAttribute.WS_ATTR_STATUS: App.status})
        await self._send(message.serialize())

    async def start_connection(self):
        "say hello and expect Login"
        self.LOG.debug("start login handshake, say hello")
        await self.send_message(HelloMessage(token=self._token, status=App.status))
        try:
            json_message = await self._socket.recv()
            self.LOG.debug(f"reply is {json_message}")
            msg = Message(json_message=json_message)
            if msg.message_type() == MessageType.WS_TYPE_LOGIN:
                await self.handle_message(msg)
            else:
                await self.abort_connection("Login expected")
        except ConnectionClosed:
            raise
        except Exception as exc:
            self.LOG.error(f"Login failed ({exc})")
            return False
        else:
            self.LOG.debug("connection started")
            return True

    async def abort_connection(self, reason: str = None, token=None, status=False):
        "say goodbye"
        await self.send_message(ByeMessage(token=token, reason=reason, status=status))
        raise ConnectionClosed

    async def handle_message(self, message: Message):
        "accept a message from the client and trigger according actions"
        # self.LOG.debug(f"handle {message=} {message.message=} {message.token=}")
        if message.token == self._token:
            self.LOG.debug(f"Received login")
            await message.handle_message(self)
        else:
            self.LOG.warning(f"Received invalid token {message.token}")
            await self.abort_connection(reason="Invalid Token", token=message.token)
