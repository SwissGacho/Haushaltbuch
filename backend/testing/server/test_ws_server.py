"""Test suite for websocket server"""

import unittest
from unittest.mock import Mock, MagicMock, AsyncMock, patch

from server.ws_server import (
    CLIENT_TOKEN_COOKIE_NAME,
    LOGIN_SUBPROTOCOL,
    WSHandler,
)

# class Test_000_WS_Server(unittest.IsolatedAsyncioTestCase):
#     async def test_001_get_websocket(self):


class Test_200_WSHandler(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        WSHandler.sockets.clear()

    def test_200_process_response_logs_redacted_repeated_headers(self):
        handler = WSHandler()
        websocket = Mock(id="ws-1")
        headers = Mock()
        headers.raw_items.return_value = [
            ("X-Api-Key", "sensitive-value"),
            ("X-Trace-Id", "first"),
            ("X-Trace-Id", "second"),
        ]
        headers.get.side_effect = lambda name, default="": {
            "Sec-WebSocket-Protocol": "",
            "Cookie": "",
        }.get(name, default)
        request = Mock(headers=headers)
        response = Mock(headers={})

        with (patch("server.ws_server.LOG") as mock_log,):
            mock_log.isEnabledFor.return_value = True
            self.assertIsNone(handler.process_response(websocket, request, response))

        logged_headers = [
            call.args[1]
            for call in mock_log.log.call_args_list
            if call.args and call.args[0] == 5
        ]
        header_log = next(
            log_line for log_line in logged_headers if "X-Api-Key" in log_line
        )
        self.assertIn("  {'X-Api-Key': ", header_log)
        self.assertNotIn("sensitive-value", header_log)
        self.assertIn("'X-Trace-Id': ['first', 'second']", header_log)

    def test_201_process_response_ignores_non_login_connection(self):
        handler = WSHandler()
        websocket = Mock(id="ws-1")
        request = Mock()
        request.headers.get.side_effect = lambda name, default="": {
            "Sec-WebSocket-Protocol": "other-protocol",
            "Cookie": "",
        }.get(name, default)
        response = Mock(headers={})

        handler.process_response(websocket, request, response)

        self.assertEqual(response.headers, {})
        self.assertEqual(WSHandler.sockets[websocket.id], {})

    def test_202_process_response_issues_client_token_for_login_connection(self):
        handler = WSHandler()
        websocket = Mock(id="ws-1")
        request = Mock()
        request.headers.get.side_effect = lambda name, default="": {
            "Sec-WebSocket-Protocol": LOGIN_SUBPROTOCOL,
            "Cookie": "",
        }.get(name, default)
        response = Mock(headers={})

        with patch(
            "server.ws_server.WSToken", return_value="issued-token"
        ) as mock_token:
            handler.process_response(websocket, request, response)

        mock_token.assert_called_once_with(inactive_seconds_timeout=None)
        self.assertEqual(
            WSHandler.sockets[websocket.id],
            {"client_token": "issued-token", "client_token_valid": False},
        )
        self.assertEqual(response.headers["Sec-WebSocket-Protocol"], LOGIN_SUBPROTOCOL)
        self.assertEqual(
            response.headers["Set-Cookie"],
            f"{CLIENT_TOKEN_COOKIE_NAME}=issued-token; Path=/; HttpOnly; SameSite=Strict",
        )

    def test_203_process_response_reuses_existing_client_token_cookie(self):
        handler = WSHandler()
        websocket = Mock(id="ws-1")
        request = Mock()
        request.headers.get.side_effect = lambda name, default="": {
            "Sec-WebSocket-Protocol": LOGIN_SUBPROTOCOL,
            "Cookie": f"{CLIENT_TOKEN_COOKIE_NAME}=existing-token",
        }.get(name, default)
        response = Mock(headers={})

        with patch("server.ws_server.WSToken") as mock_token:
            mock_token.check_token.return_value = True
            mock_token.return_value = "existing-token"
            handler.process_response(websocket, request, response)

        mock_token.check_token.assert_called_once_with("existing-token")
        mock_token.assert_called_once_with("existing-token")
        self.assertEqual(
            WSHandler.sockets[websocket.id],
            {"client_token": "existing-token", "client_token_valid": True},
        )
        self.assertEqual(response.headers["Sec-WebSocket-Protocol"], LOGIN_SUBPROTOCOL)
        self.assertNotIn("Set-Cookie", response.headers)

    async def _200_handle_messages(
        self,
        start_conn=True,
        messages=None,
        mock_header="",
        mock_pattern="",
        mock_user="mock_auth_value",
        mock_auth_user=None,
    ):
        if messages is None:
            messages = []
        handler = WSHandler()
        mock_connection = Mock(name="WSConnection")
        mock_connection.start_connection = AsyncMock(return_value=start_conn)
        mock_connection.handle_message = AsyncMock()
        mock_connection.connection_context = {
            "comp": "mock_component",
            "socket": "mock_socket",
        }
        mock_socket = MagicMock()
        mock_socket.id = "ws-1"
        mock_socket.__aiter__.return_value = messages
        mock_socket.request.headers = {
            "mock_auth_header": mock_user,
        }
        mock_path = Mock()
        no_messages = len(messages) if start_conn else 0

        # =================================== test ==========================
        with (
            patch("server.ws_server.WSConnection", return_value=mock_connection),
            patch("server.ws_server.Message") as Mock_Msg,
            patch(
                "server.ws_server.App.get_config_item",
                side_effect=[mock_header, mock_pattern],
            ),
        ):
            await handler.handler(websocket=mock_socket)
        # =================================== test ==========================

        if not mock_header:
            mock_connection.start_connection.assert_awaited_once_with(
                authenticated_user=None,
                clienttoken=None,
                client_token_valid=False,
            )
        else:
            mock_connection.start_connection.assert_awaited_once_with(
                authenticated_user=mock_auth_user or mock_user,
                clienttoken=None,
                client_token_valid=False,
            )
        self.assertEqual(Mock_Msg.call_count, no_messages, "number of Messages created")
        self.assertEqual(
            mock_connection.handle_message.await_count,
            no_messages,
            "number of Messages handled",
        )

    async def test_201_ws_handler_failed_login(self):
        messages = [
            {"type": "mocktype", "text": "mocktext 1"},
            {"type": "mocktype", "text": "mocktext 2"},
        ]
        await self._200_handle_messages(messages=messages, start_conn=False)

    async def test_202_ws_handler_normal_login_no_user(self):
        messages = [
            {"type": "mocktype", "text": "mocktext 1"},
            {"type": "mocktype", "text": "mocktext 2"},
        ]
        await self._200_handle_messages(messages=messages)

    async def test_203_ws_handler_normal_login_with_user_direct(self):
        messages = [
            {"type": "mocktype", "text": "mocktext 1"},
            {"type": "mocktype", "text": "mocktext 2"},
        ]
        await self._200_handle_messages(
            messages=messages, mock_header="mock_auth_header", mock_pattern=""
        )

    async def test_204_ws_handler_normal_login_with_user_pattern(self):
        messages = [
            {"type": "mocktype", "text": "mocktext 1"},
            {"type": "mocktype", "text": "mocktext 2"},
        ]
        await self._200_handle_messages(
            messages=messages,
            mock_header="mock_auth_header",
            mock_pattern=r"CN=\s*(\w+)\s*(,|$)",
            mock_user="O=org,CN=   mock_name ,C=CH",
            mock_auth_user="mock_name",
        )
