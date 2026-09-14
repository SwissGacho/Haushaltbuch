"""Test suite for Session"""

import unittest
from types import SimpleNamespace
from typing import cast
from unittest.mock import Mock, patch

from bom_persistent.management.user import User
from core.exceptions import TokenExpiredError
from server.session import Session
from server.ws_token import WSToken


class Test_100_Session(unittest.TestCase):
    def setUp(self):
        self.all_sessions = list(Session._all_sessions)
        self.client_sessions = {
            token: list(sessions)
            for token, sessions in Session._client_sessions.items()
        }
        self.all_tokens = set(WSToken._all_tokens)
        Session._all_sessions.clear()
        Session._client_sessions.clear()
        WSToken._all_tokens.clear()

    def tearDown(self):
        Session._all_sessions.extend(self.all_sessions)
        Session._client_sessions.update(self.client_sessions)
        WSToken._all_tokens.update(self.all_tokens)

    def create_session(
        self, user=None, conn_token=None, connection=None, client_token=None
    ):
        connection = connection or Mock(name="connection")
        connection.connection_context = {"connection": "mock-connection"}
        with patch("server.session.App.get_config_item", return_value=2):
            return Session(
                user=cast(User, user or SimpleNamespace(name="alice")),
                conn_token=conn_token,
                connection=connection,
                client_token=client_token,
            )

    def test_101_constructor_registers_session_and_creates_token(self):
        user = cast(User, SimpleNamespace(name="alice"))
        connection = Mock(name="connection")

        session = self.create_session(user=user, connection=connection)

        self.assertIn(session, Session._all_sessions)
        self.assertEqual(session.user, user)
        self.assertEqual(session.connections, [connection])
        self.assertIsInstance(session.token, WSToken)
        self.assertEqual(session.conn_tokens, set())

    def test_102_constructor_registers_session_for_client_token(self):
        client_token = "client-token"

        session = self.create_session(client_token=client_token)

        self.assertEqual(Session._client_sessions[client_token], [session])

    def test_103_get_session_from_session_token(self):
        session = self.create_session()

        found = Session.get_session_from_token(
            ses_token=session.token.token, conn_token=""
        )

        self.assertIs(found, session)

    def test_104_get_session_from_connection_token(self):
        connection_token = WSToken(inactive_seconds_timeout=None)
        session = self.create_session(conn_token=connection_token)

        found = Session.get_session_from_token(
            ses_token="", conn_token=connection_token.token
        )

        self.assertIs(found, session)
        self.assertEqual(session.conn_tokens, {connection_token.token})

    def test_105_get_session_from_client_token_creates_session_for_user(self):
        user = cast(User, SimpleNamespace(name="alice"))
        client_token = WSToken(inactive_seconds_timeout=None)
        existing_session = self.create_session(
            user=user, client_token=client_token.token
        )

        connection = Mock(name="new-connection")
        connection.connection_context = {"connection": "ws-2"}
        with patch("server.session.App.get_config_item", return_value=2):
            found = Session.get_session_from_token(
                ses_token="",
                conn_token="",
                client_token=client_token.token,
                session_user=user,
                connection=connection,
            )

        self.assertIsNot(found, existing_session)
        self.assertIsNotNone(found)
        assert found is not None
        self.assertIs(found.user, user)
        self.assertEqual(found.connections, [connection])
        self.assertEqual(
            Session._client_sessions[client_token.token], [existing_session, found]
        )

    def test_106_get_session_from_client_token_rejects_other_user(self):
        client_token = WSToken(inactive_seconds_timeout=None)
        self.create_session(
            user=cast(User, SimpleNamespace(name="alice")),
            client_token=client_token.token,
        )

        found = Session.get_session_from_token(
            ses_token="",
            conn_token="",
            client_token=client_token.token,
            session_user=cast(User, SimpleNamespace(name="bob")),
        )

        self.assertIsNone(found)

    def test_107_get_session_from_expired_session_token_raises(self):
        session = self.create_session()
        session.token.invalidate()

        with self.assertRaises(TokenExpiredError):
            Session.get_session_from_token(ses_token=session.token.token, conn_token="")

    def test_108_get_session_from_invalid_client_token_returns_none(self):
        found = Session.get_session_from_token(
            ses_token="", conn_token="", client_token="unknown-client-token"
        )

        self.assertIsNone(found)

    def test_109_add_connection_and_token(self):
        session = self.create_session()
        connection = Mock(name="second-connection")
        connection_token = WSToken(inactive_seconds_timeout=None)

        self.assertEqual(session.add_connection(connection), 1)
        self.assertEqual(session.add_connection(connection), 1)
        session.add_token(connection_token.token)

        self.assertEqual(session.connections, [session.connections[0], connection])
        self.assertEqual(session.conn_tokens, {connection_token.token})
