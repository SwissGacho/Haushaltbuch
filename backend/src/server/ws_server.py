# WS server

# from datetime import datetime
import asyncio
from contextlib import asynccontextmanager
import websockets
import socket
from secrets import token_hex as token
import logging

# logging.basicConfig()

from core.app import app, WEBSOCKET_PORT

# from server.session import Session
from server.ws_token import WSToken
from messages.message import Message
from messages.login import HelloMessage
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
            message.add({WS_ATTR_STATUS: app.status})
        await self._send(message.serialize())

    async def start_connection(self):
        "say hello"
        await self.send_message(HelloMessage(token=self._token, status=app.status))

    async def handle_message(self, message: Message):
        "accept a message from the client and trigger according actions"
        # LOG.debug(f"handle {message=} {message.message=} {message.token=}")
        if message.token != self._token:
            LOG.warning(f"Received invalid token {message.token}")
        else:
            await message.handle_message(self)


class WS_Handler:
    def __init__(self):
        pass

    async def handler(self, websocket, path):
        "Handle a ws connection"
        # LOG.debug("connection opened")
        connection = WS_Connection(websocket)
        try:
            await connection.start_connection()
            async for ws_message in websocket:
                LOG.debug(f"Client posted: {ws_message=}")
                message = Message(json_message=ws_message)
                await connection.handle_message(message=message)
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
