""" test suite for the app's main module
"""

import re
import unittest
from unittest.mock import Mock, MagicMock, AsyncMock, patch, call, DEFAULT
import logging

from money_pilot import main
from database.db import get_db
from server.ws_server import get_websocket


def msg_count(regex, msg_list):
    count = 0
    for msg in msg_list:
        if re.match(regex, msg):
            count += 1
    return count


class Test_Main(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        return await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        return await super().asyncTearDown()

    async def test_001_main(self):
        mock_get_websocket = MagicMock(
            return_value=AsyncMock(get_websocket()), name="get_websocket"
        )
        mock_get_db = MagicMock(return_value=AsyncMock(get_db()), name="get_db")
        mock_get_db.return_value.__aenter__ = AsyncMock(side_effect=[None, "DB"])
        MockApp = Mock(name="App")
        MockApp.configuration.get = Mock(name="configuration.get")
        MockApp.db_ready = AsyncMock(name="db_ready")
        MockApp.db_request_restart.wait = AsyncMock(
            name=".db_request_restart.wait", side_effect=[DEFAULT, KeyboardInterrupt]
        )
        MockApp.db_available.set = Mock(name="db_available.set")
        MockApp.db_failure.set = Mock(name="db_failure.set")
        MockApp.db_restart.set = Mock(name="db_restart.set")
        MockApp.db_available.clear = Mock(name="db_available.clear")
        MockApp.db_failure.clear = Mock(name="db_failure.clear")
        MockApp.db_restart.clear = Mock(name="db_restart.clear")
        MockApp.db_request_restart.clear = Mock(name="db_request_restart.clear")
        MockApp.attach_mock(MockApp.db_available.set, "db_available.set")
        MockApp.attach_mock(MockApp.db_failure.set, "db_failure.set")
        MockApp.attach_mock(MockApp.db_restart.set, "db_restart.set")
        MockApp.attach_mock(MockApp.db_available.clear, "db_available.clear")
        MockApp.attach_mock(MockApp.db_failure.clear, "db_failure.clear")
        MockApp.attach_mock(MockApp.db_restart.clear, "db_restart.clear")
        MockApp.attach_mock(
            MockApp.db_request_restart.clear, "db_request_restart.clear"
        )
        with (
            patch("money_pilot.App", MockApp),
            self.assertLogs(level=logging.INFO) as logs,
            patch("money_pilot.get_db", mock_get_db),
            patch("money_pilot.get_websocket", mock_get_websocket),
            self.assertRaises(KeyboardInterrupt, msg="expect KeyboardInterrupt"),
        ):
            await main()

        mock_get_websocket.assert_called_once_with()
        mock_get_websocket.return_value.__aenter__.assert_awaited_once_with()
        self.assertEqual(mock_get_db.call_count, 2, "expect get_db called twice")
        self.assertEqual(
            mock_get_db.return_value.__aenter__.await_count,
            2,
            "expect get_db context manager entered twice",
        )
        self.assertEqual(mock_get_db.return_value.__aenter__.await_args_list, [(), ()])
        MockApp.db_available.set.assert_called_once_with()
        MockApp.db_failure.set.assert_called_once_with()
        self.assertEqual(
            MockApp.db_request_restart.wait.await_count,
            2,
            "expected 'db_request_restart' awaited twice",
        )
        expected_mock_calls = [
            call.db_restart.clear(),
            call.db_failure.set(),
            call.db_request_restart.clear(),
            call.db_available.clear(),
            call.db_failure.clear(),
            call.db_restart.set(),
            call.db_restart.clear(),
            call.db_available.set(),
        ]
        self.assertEqual(MockApp.mock_calls, expected_mock_calls)
        self.assertEqual(len(logs.output), 3)
        self.assertEqual(msg_count("INFO:.*App running", logs.output), 2)
        self.assertEqual(msg_count("INFO:.*DB restarting", logs.output), 1)
