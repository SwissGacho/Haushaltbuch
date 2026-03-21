"""Test suite for websocket server"""

import unittest
from unittest.mock import Mock, MagicMock, AsyncMock, patch

from server.ws_server import WSHandler


# class Test_000_WS_Server(unittest.IsolatedAsyncioTestCase):
#     async def test_001_get_websocket(self):


class Test_200_WSHandler(unittest.IsolatedAsyncioTestCase):
    async def _200_handle_messages(self, start_conn=True, messages=[]):
        handler = WSHandler()
        mock_connection = Mock()
        mock_connection.start_connection = AsyncMock(return_value=start_conn)
        mock_connection.handle_message = AsyncMock()
        mock_socket = MagicMock()
        mock_socket.__aiter__.return_value = messages
        mock_path = Mock
        no_messages = len(messages) if start_conn else 0

        # =================================== test ==========================
        with (
            patch("server.ws_server.WSConnection", return_value=mock_connection),
            patch("server.ws_server.Message") as Mock_Msg,
        ):
            await handler.handler(websocket=mock_socket)
        # =================================== test ==========================

        mock_connection.start_connection.assert_awaited_once_with()
        self.assertEqual(Mock_Msg.call_count, no_messages, "number of Messages created")
        self.assertEqual(
            mock_connection.handle_message.await_count,
            no_messages,
            "number of Messages handled",
        )

    async def test_201_ws_handler_normal_login(self):
        messages = [
            {"type": "mocktype", "text": "mocktext 1"},
            {"type": "mocktype", "text": "mocktext 2"},
        ]
        await self._200_handle_messages(messages=messages)

    async def test_202_ws_handler_failed_login(self):
        messages = [
            {"type": "mocktype", "text": "mocktext 1"},
            {"type": "mocktype", "text": "mocktext 2"},
        ]
        await self._200_handle_messages(messages=messages, start_conn=False)
