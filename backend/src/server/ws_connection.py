"""Handle a websocket connection"""

import websockets
import json
import pprint

from core.app_logging import (
    get_context_logger,
    getLogger,
    Logger,
    log_exit,
    DEBUG,
    VERBOSE_DEBUG,
    redact,
)

LOG: Logger = getLogger(__name__)

import core.exceptions
from core.app import App
from core.exceptions import WSConnectionError
from messages.message import Message, MessageAttribute
from messages.login import HelloMessage, ByeMessage, LoginMessage
from messages.admin import EchoMessage
from messages.setup import FetchSetupMessage, StoreSetupMessage
from server.ws_connection_base import WSConnectionBase
from server.ws_message_sender import WSMessageSender
from server.ws_token import WSToken


class WSConnection(WSConnectionBase):
    """Websocket connection created by the client"""

    connections: dict[str, "WSConnection"] = {}

    def __init__(self, websocket, sock_nbr) -> None:
        self._socket = websocket
        self._socket_nbr = sock_nbr
        self._session = None
        self._conn_nbr = sock_nbr
        self._comp = None
        self.is_primary = False
        self._token = WSToken()
        self.subscribers: list[WSMessageSender] = []
        self.conn_logger = get_context_logger(LOG, **self.connection_context)
        self._register_connection()

    def _register_connection(self, key: str | None = None) -> None:
        if key:
            self._comp = key
        # remove existing entry
        self._unregister_connection()
        # self.conn_logger.debug(f"WS_Connection._register_connection({key=}): adding '{key or self._socket_nbr}'")
        WSConnection.connections |= {(key or self._socket_nbr): self}
        # self.conn_logger.debug(f"{WSConnection.connections=}")

    def register_message_sender(self, sender: WSMessageSender):
        "register a message sender to this connection"
        self.subscribers.append(sender)
        # self.conn_logger.debug(f"Registered {sender} as message sender")

    def _unregister_connection(self):
        for key in [k for k, v in WSConnection.connections.items() if v is self]:
            # self.conn_logger.debug(
            #     f"WS_Connection._unregister_connection(): del connection {key}"
            # )
            del WSConnection.connections[key]

    def unregister_message_sender(self, sender: WSMessageSender):
        "unregister a message sender from this connection"
        try:
            self.subscribers.remove(sender)
            # self.conn_logger.debug(f"Unregistered {sender} as message sender")
        except ValueError:
            self.conn_logger.warning(f"Sender {sender} not found in subscribers list")

    def unregister_other_senders(self, sender_to_keep: WSMessageSender):
        """Unregister all message senders except the specified one from this connection."""
        for sender in list(self.subscribers):
            if sender is not sender_to_keep:
                sender.release_subscriptions()

    @property
    def connection_context(self):
        "identifying string (for logging)"
        descr = {"socket": self._socket_nbr}
        if self._session:
            descr["session"] = self._session.session_id
        if self._comp:
            descr["comp"] = self._comp
        return descr

    @property
    def session(self):
        "session this connection is contained in"
        return self._session

    @session.setter
    def session(self, ses):
        self._session = ses
        self._conn_nbr = ses.add_connection(self)
        self.conn_logger = get_context_logger(LOG, **self.connection_context)

    async def _send(self, payload):
        await self._socket.send(payload)
        if self.conn_logger.isEnabledFor(VERBOSE_DEBUG):
            self.conn_logger.debug("WSConnection._send(): sent message:")
            try:
                if isinstance(payload, (bytes, bytearray)):
                    debug_payload = json.loads(payload.decode())
                elif isinstance(payload, str):
                    debug_payload = json.loads(payload)
                else:
                    debug_payload = payload
                debug_output = pprint.pformat(
                    redact(debug_payload), indent=4, width=120, compact=True
                )
            except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
                debug_output = str(redact(payload))
            for line in debug_output.splitlines():
                LOG.log(VERBOSE_DEBUG, f"    {line}")
        elif self.conn_logger.isEnabledFor(DEBUG):
            debug_payload = redact(payload)
            if len(str(debug_payload)) > 80:
                debug_payload = f"{str(debug_payload)[:80]}... (total {len(str(debug_payload))} chars)"
            self.conn_logger.debug(
                f"WSConnection._send(): sent message: {debug_payload}"
            )

    async def send_message(self, message: Message, status=False):
        "Send a message to the client using current connection"
        # LOG.debug(f"WS_Connection.send_message({message.__class__.__name__}, {status=})")
        if status:
            message.add({MessageAttribute.WS_ATTR_STATUS: App.status})
        if not message.get_str(MessageAttribute.WS_ATTR_TOKEN):
            message.add({MessageAttribute.WS_ATTR_TOKEN: self._token})
        await self._send(await message.serialize())

    async def send_message_to_component(self, comp, msg):
        """
        Send a message to component(s) on a specified connection or
        list of connections (if 'comp=="*" send to all component connections).
        """
        self.conn_logger.debug(f"Send {msg} to {comp}")
        if comp == "*":
            conns = WSConnection.connections.values()
        else:
            conns = [WSConnection.connections.get(comp, self)]
        for conn in conns:
            await conn._send(msg)  # pylint: disable=protected-access

    async def start_connection(self):
        "say hello and expect Login"
        # self.conn_logger.debug("start login handshake, say hello")
        await self.send_message(HelloMessage(token=self._token, status=App.status))
        try:
            while json_message := await self._socket.recv():
                if self.conn_logger.isEnabledFor(VERBOSE_DEBUG):
                    # self.conn_logger.debug(f"sent message: {payload}")
                    self.conn_logger.debug(
                        "WS_Connection.start_connection(): reply to hello is:"
                    )
                    try:
                        debug_message = pprint.pformat(
                            redact(json_message),
                            width=120,
                            compact=True,
                        )
                    except Exception:
                        try:
                            debug_message = str(redact(json_message))
                        except Exception:
                            debug_message = json_message
                    for line in debug_message.splitlines():
                        LOG.log(VERBOSE_DEBUG, f"    {line}")
                elif self.conn_logger.isEnabledFor(DEBUG):
                    debug_message = redact(json_message)
                    if len(str(debug_message)) > 80:
                        debug_message = f"{str(debug_message)[:80]}... (total {len(str(debug_message))} chars)"
                    self.conn_logger.debug(
                        f"WS_Connection.start_connection(): reply to hello is: {debug_message}"
                    )
                msg = Message(json_message=json_message)
                if isinstance(msg, LoginMessage):
                    self._register_connection(
                        msg.message.get(MessageAttribute.WS_ATTR_COMPONENT)
                    )
                    self.is_primary = msg.message.get(
                        MessageAttribute.WS_ATTR_IS_PRIMARY, False
                    )
                    await self.handle_message(msg)
                    break
                if isinstance(msg, (FetchSetupMessage, StoreSetupMessage, EchoMessage)):
                    await self.handle_message(msg)
                    continue
                self.conn_logger.error(
                    f"WS_Connection.start_connection(): unhandled {msg.__class__.__name__}"
                )
                await self.abort_connection("Login expected")
        except websockets.exceptions.ConnectionClosed as exc:
            self.conn_logger.debug(f"Connection closed by socket: {exc}")
            return False
        except core.exceptions.WSConnectionClosed as exc:
            self.conn_logger.debug(f"Connection closed by handler: {exc}")
            return False
        except Exception as exc:
            self.conn_logger.error(f"Start connection failed in handler ({exc})")
            raise WSConnectionError("Connection start failed") from exc
        else:
            # self.conn_logger.debug("connection started")
            return True

    async def abort_connection(
        self, reason: str | None = None, token: WSToken | None = None
    ):
        "say goodbye"
        await self.send_message(ByeMessage(token=token, reason=reason))
        raise core.exceptions.WSConnectionClosed(f"Connection aborted ({reason})")

    def connection_closed(self):
        "call when connection has been closed"
        # LOG.debug(f"WS_Connection.connection_closed({self._socket_nbr=})")
        # LOG.debug(f"Current subscribers: {self.subscribers=}")
        for sender in self.subscribers:
            sender.handle_connection_closed()
        self._unregister_connection()

    async def handle_message(self, message: Message):
        "accept a message from the client and trigger according actions"
        # self.conn_logger.debug(f"handle {message=} {message.message=} {message.token=}")
        if message.token == self._token:
            # self.conn_logger.debug(f"Received message")
            await message.handle_message(self)
        else:
            self.conn_logger.warning(f"Received invalid token {message.token}")
            await self.abort_connection(reason="Invalid Token", token=message.token)


log_exit(LOG)
