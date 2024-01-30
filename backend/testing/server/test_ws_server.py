""" Test suite for websocket server """

import unittest
from unittest.mock import Mock, MagicMock, AsyncMock, patch
import inspect

from server.ws_server import WS_Connection, WS_Handler, ConnectionClosed
from messages.message import MessageType, MessageAttribute


# class Test_000_WS_Server(unittest.IsolatedAsyncioTestCase):
#     async def test_001_get_websocket(self):


class Test_100_WS_Connection(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.connection = WS_Connection(AsyncMock(name="mockSocket"))
        self.connection._token = "mockToken"
        self.MockApp = Mock(return_value="MockStatus")
        return super().setUp()

    async def test_101__send(self):
        self.connection._socket.send = AsyncMock(name="websocket.send")
        mock_payload = Mock()
        await self.connection._send(mock_payload)
        self.connection._socket.send.assert_awaited_once_with(mock_payload)

    async def _102_send_message(self, status=None):
        self.connection._send = AsyncMock(name="_send")
        mock_message = Mock(name="message")
        mock_message.add = Mock()
        mock_message.serialize = Mock()
        if status is None:
            arg = {}
        else:
            arg = {"status": status}

        with (patch("server.ws_server.App", self.MockApp),):
            await self.connection.send_message(mock_message, **arg)

        if status:
            mock_message.add.assert_called_once_with(
                {MessageAttribute.WS_ATTR_STATUS: self.MockApp.status}
            )
        else:
            mock_message.add.assert_not_called()
        self.connection._send.assert_awaited_once_with(mock_message.serialize())

    async def test_102a_send_message_without_status(self):
        await self._102_send_message()

    async def test_102b_send_message_with_status(self):
        await self._102_send_message(status=True)

    async def test_102c_send_message_with_status_false(self):
        await self._102_send_message(status=False)

    async def _103_start_connection(self, login_arg=""):
        mock_hello_message = Mock(name="mock_hello_message")
        MockHelloMessage = Mock(name="HelloMessage", return_value=mock_hello_message)
        mock_message = Mock(name="mock_message")
        if inspect.isclass(login_arg):
            if login_arg is ConnectionClosed:
                exp_result = "ConnCloseExc"
            else:
                exp_result = "AnyExc"
            mock_message.message_type = Mock(return_value=MessageType.WS_TYPE_LOGIN)
            MockMessage = Mock(name="LoginMessage", return_value=mock_message)
        elif login_arg == "mockJSONlogin":
            exp_result = "success"
            mock_message.message_type = Mock(return_value=MessageType.WS_TYPE_LOGIN)
            MockMessage = Mock(name="LoginMessage", return_value=mock_message)
        else:
            exp_result = "AbortLogin"
            mock_message.message_type = Mock(return_value="other")
            MockMessage = Mock(name="OtherMessage", return_value=mock_message)

        self.connection._socket = Mock()
        self.connection._socket.recv = AsyncMock(
            name="websocket.recv", return_value=mock_message
        )
        self.connection.send_message = AsyncMock(name="send_message")
        self.connection.abort_connection = AsyncMock(
            name="abort_connection", side_effect=ConnectionClosed
        )
        self.connection.handle_message = AsyncMock(
            name="handle_message",
            side_effect=(
                login_arg if exp_result in ["ConnCloseExc", "AnyExc"] else None
            ),
        )
        with (
            patch("server.ws_server.HelloMessage", MockHelloMessage),
            patch("server.ws_server.App", self.MockApp),
            patch("server.ws_server.Message", MockMessage),
        ):
            if exp_result in ["ConnCloseExc", "AbortLogin"]:
                with self.assertRaises(ConnectionClosed):
                    await self.connection.start_connection()
                result = None
            else:
                result = await self.connection.start_connection()

        self.connection.send_message.assert_awaited_once_with(mock_hello_message)
        self.connection._socket.recv.assert_awaited_once_with()
        MockMessage.assert_called_once_with(json_message=mock_message)
        mock_message.message_type.assert_called_once_with()
        if exp_result == "success":
            self.connection.handle_message.assert_awaited_once_with(mock_message)
            self.connection.abort_connection.assert_not_awaited()
            self.assertTrue(result, "successful login expected")
        elif exp_result == "ConnCloseExc":
            self.connection.handle_message.assert_awaited_once_with(mock_message)
            self.connection.abort_connection.assert_not_awaited()
            self.assertIsNone(
                result, "login expected to be aborted by exception in handle_message"
            )
        elif exp_result == "AbortLogin":
            self.connection.handle_message.assert_not_awaited()
            self.connection.abort_connection.assert_awaited_once_with("Login expected")
            self.assertIsNone(result, "login expected to be aborted")
        else:
            self.connection.handle_message.assert_awaited_once_with(mock_message)
            self.connection.abort_connection.assert_not_awaited()
            self.assertEqual(exp_result, "AnyExc")
            self.assertFalse(result, "login expected to be unsuccessful")

    async def test_103a_start_connection_with_login(self):
        await self._103_start_connection("mockJSONlogin")

    async def test_103b_start_connection_without_login(self):
        await self._103_start_connection("mockJSONnoLogin")

    async def test_103c_start_connection_invalid_login(self):
        await self._103_start_connection(ConnectionClosed)

    async def test_103d_start_connection_exception(self):
        await self._103_start_connection(Exception)

    async def _104_abort_connection(self, args={}):
        mock_bye_message = Mock(name="mock_bye_message")
        MockByeMessage = Mock(name="ByeMessage", return_value=mock_bye_message)

        self.connection.send_message = AsyncMock(name="send_message")
        with (
            patch("server.ws_server.ByeMessage", MockByeMessage),
            self.assertRaises(ConnectionClosed),
        ):
            await self.connection.abort_connection(**args)
        self.connection.send_message.assert_awaited_once_with(mock_bye_message)
        MockByeMessage.assert_called_once_with(
            token=args.get("token"),
            reason=args.get("reason"),
            status=args.get("status", False),
        )

    async def test_104a_abort_connection_no_args(self):
        await self._104_abort_connection()

    async def test_104b_abort_connection_with_reason(self):
        await self._104_abort_connection({"reason": "MockReason"})

    async def test_104c_abort_connection_with_token(self):
        await self._104_abort_connection({"token": "MockToken"})

    async def test_104d_abort_connection_with_all_args(self):
        await self._104_abort_connection(
            {"status": True, "token": "MockToken", "reason": "MockReason"}
        )


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
