"""Setup a websocket server and handle connection call"""

from contextlib import asynccontextmanager
import websockets.asyncio.server as websockets
import socket

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from core.const import WEBSOCKET_PORT
from server.ws_connection import WS_Connection
from messages.message import Message


class WSHandler:
    "Container for Websocket handler"

    counter = 0

    async def handler(self, websocket):
        "Handle a ws connection"
        sock_nbr = WSHandler.counter
        WSHandler.counter += 1
        local_LOG = getLogger(  # pylint: disable=invalid-name
            f"{WSHandler.__module__}(sock #{sock_nbr})"
        )
        local_LOG.debug("connection opened")
        connection = WS_Connection(websocket, sock_nbr=f"sock #{sock_nbr}")
        try:
            if await connection.start_connection():
                local_LOG = getLogger(  # pylint: disable=invalid-name
                    f"{WSHandler.__module__}({connection.connection_id})"
                )
                local_LOG.debug(
                    f"connection started from socket connection #{sock_nbr}"
                )
                async for ws_message in websocket:
                    local_LOG.debug(f"Client posted: {ws_message=}")
                    try:
                        message = Message(json_message=ws_message)
                    except TypeError:
                        local_LOG.warning(  # pylint: disable=logging-not-lazy
                            "message handler failed to create Message object"
                            f"from json: {ws_message}"
                        )
                        raise
                    await connection.handle_message(message=message)
        except Exception as exc:
            local_LOG.error(f"Connection aborted by exception {exc}")
            raise
        finally:
            local_LOG.debug("Connection ended.")
            connection.connection_closed()


@asynccontextmanager
async def get_websocket():
    "Context manager for Websockets"
    ws_handler = WSHandler()
    hostname = socket.gethostname()
    ws_server = await websockets.serve(
        ws_handler.handler,
        ["localhost", hostname],
        WEBSOCKET_PORT,
    )
    if not ws_server.is_serving():
        LOG.error("Failed to start WS server")
    try:
        yield ws_server
    finally:
        ws_server.close()


log_exit(LOG)
