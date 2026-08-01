"""Test suite for login messages."""

import unittest
from json import dumps
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from core.exceptions import TokenExpiredError
from messages.login import HelloMessage, LoginMessage, WelcomeMessage, ByeMessage
from messages.message import MessageType, MessageAttribute


class Test_100_LoginMessages(unittest.IsolatedAsyncioTestCase):

    def test_101_login_message_type(self):
        self.assertEqual(LoginMessage.message_type(), MessageType.WS_TYPE_LOGIN)

    async def test_102_handle_message_success_new_session_non_primary(self):
        msg = LoginMessage(
            dumps(
                {
                    MessageAttribute.WS_ATTR_TYPE: MessageType.WS_TYPE_LOGIN,
                    MessageAttribute.WS_ATTR_TOKEN: "conn-token",
                }
            )
        )
        session = Mock(token="ses-token")
        connection = Mock(
            session=None,
            connection_context={"connection": "ws-1"},
            is_primary=False,
        )
        connection.send_message = AsyncMock()
        connection.abort_connection = AsyncMock()

        with (
            patch(
                "messages.login.check_login", AsyncMock(return_value=Mock(name="user"))
            ) as mock_check_login,
            patch(
                "messages.login.Session", Mock(return_value=session)
            ) as mock_session_class,
            patch(
                "messages.login.get_context_logger",
                Mock(return_value=Mock(debug=Mock())),
            ) as mock_ctx_logger,
        ):
            await msg.handle_message(connection)

        mock_check_login.assert_awaited_once_with(msg.message)
        mock_session_class.assert_called_once()
        mock_ctx_logger.assert_called_once()
        self.assertIs(connection.session, session)
        connection.abort_connection.assert_not_awaited()
        connection.send_message.assert_awaited_once()
        sent_message = connection.send_message.await_args.args[0]
        self.assertIsInstance(sent_message, WelcomeMessage)
        self.assertEqual(
            sent_message.message[MessageAttribute.WS_ATTR_TYPE],
            MessageType.WS_TYPE_WELCOME,
        )
        self.assertEqual(
            sent_message.message[MessageAttribute.WS_ATTR_SES_TOKEN], "ses-token"
        )
        self.assertNotIn(MessageAttribute.WS_ATTR_VERSION_INFO, sent_message.message)

    async def test_103_handle_message_success_existing_session_primary(self):
        msg = LoginMessage(
            dumps(
                {
                    MessageAttribute.WS_ATTR_TYPE: MessageType.WS_TYPE_LOGIN,
                    MessageAttribute.WS_ATTR_TOKEN: "conn-token",
                    MessageAttribute.WS_ATTR_SES_TOKEN: "ses-token",
                }
            )
        )
        session = Mock(token="ses-token")
        connection = Mock(
            session=None,
            connection_context={"connection": "ws-1"},
            is_primary=True,
        )
        connection.send_message = AsyncMock()
        connection.abort_connection = AsyncMock()

        mock_session_class = Mock()
        mock_session_class.get_session_from_token = Mock(return_value=session)

        with (
            patch("messages.login.Session", mock_session_class),
            patch("messages.login.check_login", AsyncMock()) as mock_check_login,
            patch(
                "messages.login.App",
                SimpleNamespace(
                    status_object=SimpleNamespace(version={"version": "9.9.9"})
                ),
            ),
            patch(
                "messages.login.get_context_logger",
                Mock(return_value=Mock(debug=Mock())),
            ),
        ):
            await msg.handle_message(connection)

        mock_session_class.get_session_from_token.assert_called_once_with(
            ses_token="ses-token", conn_token=""
        )
        mock_check_login.assert_not_awaited()
        self.assertIs(connection.session, session)
        connection.send_message.assert_awaited_once()
        sent_message = connection.send_message.await_args.args[0]
        self.assertEqual(
            sent_message.message[MessageAttribute.WS_ATTR_VERSION_INFO],
            {"version": "9.9.9"},
        )

    async def test_104_handle_message_permission_error_aborts(self):
        msg = LoginMessage(
            dumps(
                {
                    MessageAttribute.WS_ATTR_TYPE: MessageType.WS_TYPE_LOGIN,
                    MessageAttribute.WS_ATTR_TOKEN: "conn-token",
                }
            )
        )
        connection = Mock(
            session=None,
            connection_context={"connection": "ws-1"},
            is_primary=False,
        )
        connection.send_message = AsyncMock()
        connection.abort_connection = AsyncMock()

        with (
            patch(
                "messages.login.check_login",
                AsyncMock(side_effect=PermissionError("denied")),
            ),
            patch(
                "messages.login.get_context_logger",
                Mock(return_value=Mock(debug=Mock())),
            ),
        ):
            await msg.handle_message(connection)

        connection.abort_connection.assert_awaited_once_with(reason="Access denied")
        connection.send_message.assert_not_awaited()

    async def test_105_handle_message_token_expired_aborts(self):
        msg = LoginMessage(
            dumps(
                {
                    MessageAttribute.WS_ATTR_TYPE: MessageType.WS_TYPE_LOGIN,
                    MessageAttribute.WS_ATTR_TOKEN: "conn-token",
                    MessageAttribute.WS_ATTR_SES_TOKEN: "expired-ses-token",
                }
            )
        )
        connection = Mock(
            session=None,
            connection_context={"connection": "ws-1"},
            is_primary=False,
        )
        connection.send_message = AsyncMock()
        connection.abort_connection = AsyncMock()

        mock_session_class = Mock()
        mock_session_class.get_session_from_token = Mock(
            side_effect=TokenExpiredError()
        )

        with patch("messages.login.Session", mock_session_class):
            await msg.handle_message(connection)

        connection.abort_connection.assert_awaited_once_with(reason="Session expired")
        connection.send_message.assert_not_awaited()

    async def test_106_handle_message_value_error_is_wrapped(self):
        msg = LoginMessage(
            dumps(
                {
                    MessageAttribute.WS_ATTR_TYPE: MessageType.WS_TYPE_LOGIN,
                    MessageAttribute.WS_ATTR_TOKEN: "conn-token",
                }
            )
        )
        connection = Mock(
            session=None,
            connection_context={"connection": "ws-1"},
            is_primary=False,
        )
        connection.send_message = AsyncMock()
        connection.abort_connection = AsyncMock()

        with patch(
            "messages.login.check_login", AsyncMock(side_effect=ValueError("bad login"))
        ):
            with self.assertRaises(RuntimeError):
                await msg.handle_message(connection)

        connection.abort_connection.assert_not_awaited()
        connection.send_message.assert_not_awaited()


class Test_200_OtherMessage(unittest.TestCase):
    def test_201_hello_message(self):
        msg = HelloMessage(token="conn-token", status="ok")
        self.assertEqual(
            msg.message[MessageAttribute.WS_ATTR_TYPE], MessageType.WS_TYPE_HELLO
        )
        self.assertEqual(msg.message[MessageAttribute.WS_ATTR_TOKEN], "conn-token")
        self.assertEqual(msg.message[MessageAttribute.WS_ATTR_STATUS], "ok")

    def test_202_welcome_message_with_and_without_version(self):
        msg_with_version = WelcomeMessage(
            token="conn-token", ses_token="ses-token", version_info={"version": "1.0"}
        )
        self.assertEqual(
            msg_with_version.message[MessageAttribute.WS_ATTR_TYPE],
            MessageType.WS_TYPE_WELCOME,
        )
        self.assertEqual(
            msg_with_version.message[MessageAttribute.WS_ATTR_SES_TOKEN], "ses-token"
        )
        self.assertEqual(
            msg_with_version.message[MessageAttribute.WS_ATTR_VERSION_INFO],
            {"version": "1.0"},
        )

        msg_without_version = WelcomeMessage(token="conn-token", ses_token="ses-token")
        self.assertNotIn(
            MessageAttribute.WS_ATTR_VERSION_INFO, msg_without_version.message
        )

    def test_203_bye_message_defaults_and_custom_reason(self):
        default_msg = ByeMessage()
        self.assertEqual(
            default_msg.message[MessageAttribute.WS_ATTR_TYPE], MessageType.WS_TYPE_BYE
        )
        self.assertEqual(default_msg.message[MessageAttribute.WS_ATTR_REASON], "Error")

        custom_msg = ByeMessage(token="conn-token", reason="Access denied")
        self.assertEqual(
            custom_msg.message[MessageAttribute.WS_ATTR_TOKEN], "conn-token"
        )
        self.assertEqual(
            custom_msg.message[MessageAttribute.WS_ATTR_REASON], "Access denied"
        )
