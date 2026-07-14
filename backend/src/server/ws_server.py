"""Setup a websocket server and handle connection call"""

import os
import json
import pprint
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
)

LOG: Logger = getLogger(__name__)

from core.const import WEBSOCKET_PORT
from server.ws_connection import WSConnection
from messages.message import Message
from messages.admin import LogMessage


class WSHandler:
    "Container for Websocket handler"

    counter = 0

    async def handler(self, websocket):
        "Handle a ws connection"
        sock_nbr = WSHandler.counter
        WSHandler.counter += 1
        local_LOG = get_context_logger(LOG, socket=f"sock #{sock_nbr}")
        local_LOG.debug("connection opened")
        connection = WSConnection(websocket, sock_nbr=f"sock #{sock_nbr}")
        try:
            if await connection.start_connection():
                local_LOG = get_context_logger(LOG, **connection.connection_context)
                local_LOG.debug("Connection started.")
                async for ws_message in websocket:
                    if local_LOG.isEnabledFor(VERBOSE_DEBUG):
                        local_LOG.debug("WSHandler.handler(): client posted:")
                        try:
                            debug_message = pprint.pformat(
                                redact(json.loads(ws_message)),
                                indent=4,
                                width=120,
                                compact=True,
                            )
                        except json.JSONDecodeError:
                            debug_message = redact(ws_message)
                        for line in debug_message.splitlines():
                            LOG.log(VERBOSE_DEBUG, f"    {line}")
                    elif local_LOG.isEnabledFor(DEBUG):
                        debug_message = redact(ws_message)
                        if len(str(debug_message)) > 80:
                            debug_message = (
                                f"{str(debug_message)[:80]}... "
                                + f"(total {len(str(debug_message))} chars)"
                            )
                        local_LOG.debug(
                            f"WSHandler.handler(): client posted: {debug_message}"
                        )
                    try:
                        message = Message(json_message=ws_message)
                    except TypeError:
                        if local_LOG.isEnabledFor(WARNING):
                            local_LOG.warning(
                                "message handler failed to create Message object "
                                f"from json: {redact(ws_message)}"
                            )
                        raise
                    await connection.handle_message(
                        message=message,
                        check_ses_token=not isinstance(message, LogMessage),
                    )
        except websockets.exceptions.ConnectionClosed as exc:
            local_LOG.debug(f"Connection closed by peer: {exc}")
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
