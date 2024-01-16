# WS server

# from datetime import datetime
import asyncio
from contextlib import asynccontextmanager
import websockets
import socket
from secrets import token_hex as token
import logging

# logging.basicConfig()

from core.const import *
from core.status import app

# from server.session import Session
from server.ws_token import WSToken
from messages.message import Message
from messages.login import HelloMessage


class WSProtocol(websockets.WebSocketServerProtocol):
    async def process_request(self, path, headers):
        print(f"Request, path: {path} ")
        print(f"  Headers: {headers} ")
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
        # # print(f"Cookies: {self.cookies} ")
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
        # print(f"sent message: {payload}")

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
        # print(f"handle {message=} {message.message=} {message.token=}")
        if message.token != self._token:
            print(f"Received invalid token {message.token}")
        else:
            await message.handle_message(self)


class WS_Handler:
    def __init__(self):
        pass

    async def handler(self, websocket, path):
        "Handle a ws connection"
        # print("connection opened")
        connection = WS_Connection(websocket)
        try:
            await connection.start_connection()
            async for ws_message in websocket:
                print(f"Client posted: {ws_message=}")
                message = Message(json_message=ws_message)
                await connection.handle_message(message=message)
        finally:
            print("Connection closed.")


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
    if ws_server.is_serving():
        print(
            f"Websocket server listening on addresses: {' , '.join( [sock.getsockname()[0] for sock in ws_server.sockets])}"
        )
    else:
        print("Failed to start WS server")
    try:
        yield ws_server
    finally:
        print("Closing WS server")
        ws_server.close()
