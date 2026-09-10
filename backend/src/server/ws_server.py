"""Setup a websocket server and handle connection call"""

import os
import re
import json
import socket
from uuid import UUID
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
from server.ws_token import WSToken
from messages.message import Message
from messages.admin import LogMessage

LOGIN_SUBPROTOCOL = "mp-login"
CLIENT_TOKEN_COOKIE_NAME = "client-token"


class WSHandler:
    "Container for Websocket handler"

    counter = 0
    sockets: dict[UUID, dict[str, any]] = {}

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
        if auth_header_name:
            auth_header = headers.get(auth_header_name)
            if not auth_header:
                return None
            if auth_user_pattern:
                try:
                    match = re.search(auth_user_pattern, auth_header)
                    auth_user = match.group(1) if match else None
                except (re.error, TypeError, IndexError) as e:
                    LOG.error(
                        f"Auth user extraction failed: pattern={auth_user_pattern}; error: {e}"
                    )
            else:
                auth_user = auth_header
            LOG.debug(f"WSHandler.get_auth_user(): authenticated user: '{auth_user}' ")
            LOG.log(
                VERBOSE_DEBUG,
                f"(from {auth_header_name}"
                f"{(' using pattern ' + auth_user_pattern) if auth_user_pattern else ''})",
            )
        return auth_user

    def process_response(self, websocket, request, response):
        "Acknowledge the login subprotocol and issue a connection token cookie for it"
        WSHandler.sockets[websocket.id] = {}
        if LOG.isEnabledFor(DEBUG):
            LOG.log(VERBOSE_DEBUG, "WSHandler.get_auth_user(): request headers:")
            items = (
                request.headers.raw_items()
                if hasattr(request.headers, "raw_items")
                else request.headers.items()
            )
            header_values: dict[str, list[str]] = {}
            for header, value in items:
                if header.lower() == "cookie":
                    cookies = dict(
                        crumb.strip().split("=", 1)
                        for crumb in value.split(";")
                        if "=" in crumb
                    )
                    value = "; ".join(f"{k}={v}" for k, v in redact(cookies).items())
                header_values.setdefault(header, []).append(value)
            for line in pprint_lines(
                {k: v[0] if len(v) == 1 else v for k, v in header_values.items()}
            ):
                LOG.log(VERBOSE_DEBUG, f"  {line}")
        requested_protocols = [
            protocol.strip()
            for protocol in request.headers.get("Sec-WebSocket-Protocol", "").split(",")
        ]
        if requested_protocols:
            LOG.debug(
                f"WSHandler.process_response(): requested protocols: {requested_protocols}"
            )
        if LOGIN_SUBPROTOCOL not in requested_protocols:
            return None
        response.headers["Sec-WebSocket-Protocol"] = LOGIN_SUBPROTOCOL
        request_cookies: dict[str, str] = dict(
            crumb.strip().split("=", 1)
            for crumb in request.headers.get("Cookie", "").split(";")
            if "=" in crumb
        )
        if CLIENT_TOKEN_COOKIE_NAME in request_cookies:
            client_token = request_cookies[CLIENT_TOKEN_COOKIE_NAME]
            LOG.debug(
                f"WSHandler.process_response(): found {redact({CLIENT_TOKEN_COOKIE_NAME: client_token})}"
            )
            WSHandler.sockets[websocket.id]["client_token_valid"] = WSToken.check_token(
                client_token
            )
            if WSHandler.sockets[websocket.id]["client_token_valid"]:
                WSHandler.sockets[websocket.id]["client_token"] = WSToken(client_token)
            return None
        client_token = WSToken(inactive_seconds_timeout=None)
        LOG.debug(
            f"WSHandler.process_response(): issuing {redact({CLIENT_TOKEN_COOKIE_NAME: client_token})} for login"
        )
        response.headers["Set-Cookie"] = (
            f"{CLIENT_TOKEN_COOKIE_NAME}={client_token}; Path=/; HttpOnly; SameSite=Strict"
        )
        WSHandler.sockets[websocket.id]["client_token"] = client_token
        WSHandler.sockets[websocket.id]["client_token_valid"] = False
        return None

    async def handler(self, websocket):
        "Handle a ws connection"
        sock_nbr = WSHandler.counter
        WSHandler.counter += 1
        context_log = get_context_logger(LOG, socket=f"sock #{sock_nbr}")
        context_log.debug("connection opened")
        auth_user = self.get_auth_user(websocket.request.headers)
        connection = WSConnection(websocket, sock_nbr=f"sock #{sock_nbr}")
        try:
            if await connection.start_connection(
                authenticated_user=auth_user,
                clienttoken=WSHandler.sockets.get(websocket.id, {}).get("client_token"),
                client_token_valid=WSHandler.sockets.get(websocket.id, {}).get(
                    "client_token_valid", False
                ),
            ):
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
        process_response=ws_handler.process_response,
    )
    if not ws_server.is_serving():
        LOG.error("Failed to start WS server")
    try:
        yield ws_server
    finally:
        ws_server.close()


log_exit(LOG)
