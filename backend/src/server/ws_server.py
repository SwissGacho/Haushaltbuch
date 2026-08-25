"""Setup a websocket server and handle connection call"""

import os
import re
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
from core.app import App
from core.configuration.config import Config
from server.ws_connection import WSConnection
from messages.message import Message
from messages.admin import LogMessage


class WSHandler:
    "Container for Websocket handler"

    counter = 0

    def get_auth_user(self, headers) -> str | None:
        "Get headers from websocket request"
        auth_header_name = App.get_config_item(
            Config.CONFIG_APP_AUTH_HEADER, default=""
        )
        auth_user_pattern = App.get_config_item(
            Config.CONFIG_APP_AUTH_USER_PATTERN, default=""
        )
        if not (
            isinstance(auth_header_name, str) and isinstance(auth_user_pattern, str)
        ):
            LOG.error(
                f"Invalid configuration for auth_header or auth_user_pattern: "
                f"auth_header={auth_header_name}; auth_user_pattern={auth_user_pattern}"
            )
            return None
        auth_user = None
        if LOG.isEnabledFor(DEBUG):
            LOG.log(VERBOSE_DEBUG, "WSHandler.get_auth_user(): request headers:")
            for header, value in headers.raw_items():
                LOG.log(VERBOSE_DEBUG, f"  {header:<40}: {value}")
        if auth_header_name:
            auth_header = headers.get(auth_header_name)
            if auth_user_pattern:
                try:
                    match = re.search(auth_user_pattern, auth_header)
                    auth_user = match.group(1) if match else None
                except re.error as e:
                    LOG.error(f"Invalid regex pattern: {auth_user_pattern}; error: {e}")
            else:
                auth_user = auth_header
            LOG.debug(f"WSHandler.get_auth_user(): authenticated user: '{auth_user}' ")
            LOG.log(
                VERBOSE_DEBUG,
                f"(from {auth_header_name}"
                f"{(' using pattern ' + auth_user_pattern) if auth_user_pattern else ''})",
            )
        return auth_user

    async def handler(self, websocket):
        "Handle a ws connection"
        sock_nbr = WSHandler.counter
        WSHandler.counter += 1
        context_log = get_context_logger(LOG, socket=f"sock #{sock_nbr}")
        context_log.debug("connection opened")
        auth_user = self.get_auth_user(websocket.request.headers)
        connection = WSConnection(websocket, sock_nbr=f"sock #{sock_nbr}")
        try:
            if await connection.start_connection(authenticated_user=auth_user):
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
                    await connection.handle_message(
                        message=message,
                        check_ses_token=not isinstance(message, LogMessage),
                    )
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
