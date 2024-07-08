""" Handle a websocket connection """

import websockets

import core.exceptions
from core.app import App
from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from messages.login import HelloMessage, ByeMessage, LoginMessage
from messages.admin import EchoMessage
from messages.setup import FetchSetupMessage, StoreSetupMessage
from core.app_logging import getLogger

LOG = getLogger(__name__)


class WS_Connection:
    """Websocket connection created by the client"""

    connections: dict[str, "WS_Connection"] = {}

    def __init__(self, websocket, sock_nbr) -> None:
        self._socket = websocket
        self._session = None
        self._conn_nbr = sock_nbr
        self._comp = None
        self._token = WSToken()
        self.LOG = getLogger(  # pylint: disable=invalid-name
            f"{WS_Connection.__module__}({self.connection_id})"
        )
        self._register_connection()

    def _register_connection(self, key: str = None) -> None:
        if key:
            self._comp = key
        # remove existing entry
        self._unregister_connection()
        # self.LOG.debug(
        #     f"WS_Connection._register_connection({key=}): adding '{key or self.connection_id}'"
        # )
        WS_Connection.connections |= {(key or self.connection_id): self}
        # self.LOG.debug(f"{WS_Connection.connections=}")

    def _unregister_connection(self):
        for key in [k for k, v in WS_Connection.connections.items() if v is self]:
            # self.LOG.debug(
            #     f"WS_Connection._unregister_connection(): del connection {key}"
            # )
            del WS_Connection.connections[key]

    @property
    def connection_id(self):
        "identifying string (for logging)"
        myself = self._comp or str(self._conn_nbr)
        if self._session:
            return f"{self._session.session_id},conn {myself}"
        else:
            return myself

    @property
    def session(self):
        "session this connection is contained in"
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
        "Send a message to the client using current connection"
        if status:
            message.add({MessageAttribute.WS_ATTR_STATUS: App.status})
        await self._send(message.serialize())

    async def send_message_to_component(self, comp, msg):
        """
        Send a message to component(s) on a specified connection or
        list of connections (if 'comp=="*" send to all component connections).
        """
        self.LOG.debug(f"Send {msg} to {comp}")
        if comp == "*":
            conns = WS_Connection.connections.values()
        else:
            conns = [WS_Connection.connections.get(comp, self)]
        for conn in conns:
            await conn._send(msg)

    async def start_connection(self):
        "say hello and expect Login"
        self.LOG.debug("start login handshake, say hello")
        await self.send_message(HelloMessage(token=self._token, status=App.status))
        try:
            while json_message := await self._socket.recv():
                self.LOG.debug(
                    f"WS_Connection.start_connection(): reply to hello is {json_message}"
                )
                msg = Message(json_message=json_message)
                if isinstance(msg, LoginMessage):
                    self._register_connection(
                        msg.message.get(MessageAttribute.WS_ATTR_COMPONENT)
                    )
                    await self.handle_message(msg)
                    break
                if isinstance(msg, (FetchSetupMessage, StoreSetupMessage, EchoMessage)):
                    await self.handle_message(msg)
                    continue
                self.LOG.error(
                    f"WS_Connection.start_connection(): unhandled {msg.__class__.__name__}"
                )
                await self.abort_connection("Login expected")
        except websockets.exceptions.ConnectionClosed as exc:
            self.LOG.debug(f"Connection closed by socket: {exc}")
            return False
        except core.exceptions.ConnectionClosed as exc:
            self.LOG.debug(f"Connection closed by handler: {exc}")
            return False
        except Exception as exc:
            self.LOG.error(f"Start connection failed in handler ({exc})")
            raise
            return False
        else:
            self.LOG.debug("connection started")
            return True

    async def abort_connection(self, reason: str = None, token=None, status=False):
        "say goodbye"
        await self.send_message(ByeMessage(token=token, reason=reason, status=status))
        raise core.exceptions.ConnectionClosed(f"Connection aborted ({reason})")

    def connection_closed(self):
        "call when connection has been closed"
        self._unregister_connection()

    async def handle_message(self, message: Message):
        "accept a message from the client and trigger according actions"
        # self.LOG.debug(f"handle {message=} {message.message=} {message.token=}")
        if message.token == self._token:
            # self.LOG.debug(f"Received message")
            await message.handle_message(self)
        else:
            self.LOG.warning(f"Received invalid token {message.token}")
            await self.abort_connection(reason="Invalid Token", token=message.token)


# LOG.debug("module imported")
