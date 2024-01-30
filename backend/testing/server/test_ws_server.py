""" Test suite for websocket server """

import unittest
from unittest.mock import Mock, MagicMock, AsyncMock, patch

from server.ws_server import WS_Connection, WS_Handler, ConnectionClosed
from messages.message import MessageType


# class Test_000_WS_Server(unittest.IsolatedAsyncioTestCase):
#     async def test_001_get_websocket(self):


class Test_100_WS_Connection(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.connection = WS_Connection(AsyncMock(name="mockSocket"))
        self.connection.token = "mockToken"
        return super().setUp()

    # async def test_101_start_connection_with_login(self):
    #     self.mockConnection.send_message = MagicMock()
    #     self.mockConnection.abort_connection = MagicMock()
    #     self.mockConnection.handle_message = MagicMock()
    #     mock_login_message = Mock()
    #     mock_login_message.message_type = Mock(return_value=MessageType.WS_TYPE_LOGIN)
    #     MockMessage = Mock(return_value=mock_login_message)
    async def test_102_abort_connection(self):
        self.connection.send_message = AsyncMock(name="send_message")
        mock_bye_msg = Mock(name="ByeMessage")

        with (
            patch("server.ws_server.ByeMessage", mock_bye_msg),
            self.assertRaises(ConnectionClosed),
        ):
            await self.connection.abort_connection(reason="AbortMessage")
        self.connection.send_message.assert_awaited_once_with(mock_bye_msg())


class Test_200_WS_Handler(unittest.IsolatedAsyncioTestCase):
    async def _200_handle_messages(self, start_conn=True, messages=[]):
        handler = WS_Handler()
        mock_connection = Mock()
        mock_connection.start_connection = AsyncMock(return_value=start_conn)
        mock_connection.handle_message = AsyncMock()
        mock_socket = MagicMock()
        mock_socket.__aiter__.return_value = messages
        mock_path = Mock
        no_messages = len(messages) if start_conn else 0
        with (
            patch("server.ws_server.WS_Connection", return_value=mock_connection),
            patch("server.ws_server.Message") as Mock_Msg,
        ):
            await handler.handler(websocket=mock_socket, path=mock_path)
            mock_connection.start_connection.assert_awaited_once_with()
            self.assertEqual(
                Mock_Msg.call_count, no_messages, "number of Messages created"
            )
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
