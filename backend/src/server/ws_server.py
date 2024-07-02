""" Setup a websocket server and handle connection call """

from contextlib import asynccontextmanager
import websockets
import socket

from core.app import WEBSOCKET_PORT

from server.ws_connection import WS_Connection
from messages.message import Message
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


class WS_Handler:
    counter = 0

    async def handler(self, websocket, path):
        "Handle a ws connection"
        sock_nbr = WS_Handler.counter
        WS_Handler.counter += 1
        self.LOG = getLogger(f"{WS_Handler.__module__}(sock #{sock_nbr})")
        self.LOG.debug("connection opened")
        connection = WS_Connection(websocket, sock_nbr=f"sock #{sock_nbr}")
        try:
            if await connection.start_connection():
                self.LOG = getLogger(
                    f"{WS_Handler.__module__}({connection.connection_id})"
                )
                self.LOG.debug(f"connection started from socket connection #{sock_nbr}")
                async for ws_message in websocket:
                    self.LOG.debug(f"Client posted: {ws_message=}")
                    try:
                        message = Message(json_message=ws_message)
                    except TypeError:
                        self.LOG.warning(  # pylint: disable=logging-not-lazy
                            "message handler failed to create Message object"
                            f"from json: {ws_message}"
                        )
                        raise
                    await connection.handle_message(message=message)
        except Exception as exc:
            self.LOG.error(f"Connection aborted by exception {exc}")
            raise
        finally:
            self.LOG.debug("Connection ended.")
            connection.connection_closed()


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
