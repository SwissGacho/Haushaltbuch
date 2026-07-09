"""Setup a websocket server and handle connection call"""

import os
import json
import socket
import websockets
import websockets.asyncio.server as websockets_server
from contextlib import asynccontextmanager

from core.app_logging import (
    get_context_logger,
    getLogger,
    log_exit,
    Logger,
    redact,
    WARNING,
    DEBUG,
    VERBOSE_DEBUG,
    pprint_lines,
    redact_truncate,
)

LOG: Logger = getLogger(__name__)

from core.const import WEBSOCKET_PORT
from server.ws_connection import WSConnection
from messages.message import Message


class WSHandler:
    "Container for Websocket handler"

    counter = 0

    async def handler(self, websocket):
        "Handle a ws connection"
        sock_nbr = WSHandler.counter
        WSHandler.counter += 1
        context_log = get_context_logger(LOG, socket=f"sock #{sock_nbr}")
        context_log.debug("connection opened")
        connection = WSConnection(websocket, sock_nbr=f"sock #{sock_nbr}")
        try:
            if await connection.start_connection():
                context_log = get_context_logger(LOG, **connection.connection_context)
                context_log.debug("Connection started.")
                async for ws_message in websocket:
                    if context_log.isEnabledFor(DEBUG):
                        context_log.debug(
                            "WSHandler.handler(): client posted: "
                            f"{redact_truncate(ws_message,max_length=50)}"
                        )
                        if context_log.isEnabledFor(VERBOSE_DEBUG):
                            try:
                                msg = json.loads(ws_message)
                            except Exception:
                                msg = ws_message
                            for line in pprint_lines(msg):
                                LOG.log(VERBOSE_DEBUG, f"     {line}")
                    try:
                        message = Message(json_message=ws_message)
                    except TypeError:
                        if context_log.isEnabledFor(WARNING):
                            context_log.warning(
                                "message handler failed to create Message object "
                                f"from json: {redact(ws_message)}"
                            )
                        raise
                    await connection.handle_message(message=message)
        except websockets.exceptions.ConnectionClosed as exc:
            context_log.debug(f"Connection closed by peer: {exc}")
        except Exception as exc:
            context_log.error(f"Connection aborted by exception {exc}")
            raise
        finally:
            context_log.debug("Connection ended.")
            connection.connection_closed()


@asynccontextmanager
async def get_websocket():
    "Context manager for Websockets"
    ws_handler = WSHandler()
    localhost = [socket.gethostname(), "localhost"]
    bind_address = os.getenv("WS_BIND_ADDRESS") or localhost
    LOG.info(f"Starting WebSocket server on {bind_address}:{WEBSOCKET_PORT}")
    ws_server = await websockets_server.serve(
        handler=ws_handler.handler,
        host=bind_address,  # type: ignore[arg-type]
        port=WEBSOCKET_PORT,
    )
    if not ws_server.is_serving():
        LOG.error("Failed to start WS server")
    try:
        yield ws_server
    finally:
        ws_server.close()


log_exit(LOG)
