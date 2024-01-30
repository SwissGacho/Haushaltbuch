# WS server

# from datetime import datetime
import asyncio
from contextlib import asynccontextmanager
import websockets
import socket
from secrets import token_hex as token
import logging

# logging.basicConfig()

from core.app import App, WEBSOCKET_PORT

# from server.session import Session
from server.ws_token import WSToken
from messages.message import Message, MessageType, MessageAttribute
from messages.login import HelloMessage, ByeMessage
from core.app_logging import getLogger

LOG = getLogger(__name__)


class WSProtocol(websockets.WebSocketServerProtocol):
    async def process_request(self, path, headers):
        LOG.debug(f"Request, path: {path} ")
        LOG.debug(f"  Headers: {headers} ")
        # self.cookies = {}
        # # Loop over all Cookie headers
        # for value in headers.get_all("Cookie"):
        #     # split header value by ';' to get each cookie, the split
        #     # cookie by '=' to get name and content of cookie and
        #     # collect these in a dict
        #     self.cookies.update(
        #         {
        #             e[0]: e[1]
        #             for e in [
        #                 v.strip().split("=") for v in value.split(";") if len(v) > 0
        #             ]
        #         }
        #     )
        # # LOG.debug(f"Cookies: {self.cookies} ")
        # gacho_cookie = json.loads(self.cookies.get("gacho", "{}"))
        # user = gacho_cookie.get("user", None)


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


class WS_Handler:
    async def handler(self, websocket, path):
        "Handle a ws connection"
        # LOG.debug("connection opened")
        connection = WS_Connection(websocket)
        try:
            if await connection.start_connection():
                async for ws_message in websocket:
                    LOG.debug(f"Client posted: {ws_message=}")
                    try:
                        message = Message(json_message=ws_message)
                    except TypeError:
                        LOG.warning(
                            "message handler failed to create Message object"
                            + f"from json: {ws_message}"
                        )
                        raise
                    await connection.handle_message(message=message)
        except Exception as exc:
            LOG.error(f"Connection aborted by exception {exc}")
        finally:
            LOG.debug("Connection closed.")


@asynccontextmanager
async def get_websocket():
    ws_handler = WS_Handler()
    hostname = socket.gethostname()
    ws_server = await websockets.serve(
        ws_handler.handler,
        ["localhost", hostname],
        WEBSOCKET_PORT,
        # create_protocol=WSProtocol
    )
    if not ws_server.is_serving():
        LOG.error("Failed to start WS server")
    try:
        yield ws_server
    finally:
        ws_server.close()


# LOG.debug("module imported")
