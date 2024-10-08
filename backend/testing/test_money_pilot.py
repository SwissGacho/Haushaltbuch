""" test suite for the app's main module
"""

import unittest
from unittest.mock import MagicMock, AsyncMock, patch
import logging
import asyncio

from money_pilot import main
from core.exceptions import DBRestart
from database.db_manager import get_db
from server.ws_server import get_websocket


class Test_Main(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        return await super().asyncSetUp()

    async def asyncTearDown(self) -> None:
        return await super().asyncTearDown()

    async def test_001_main(self):
        mock_get_db = MagicMock(return_value=AsyncMock(get_db()), name="get_db")
        mock_get_websocket = MagicMock(
            return_value=AsyncMock(get_websocket()), name="get_websocket"
        )
        with (
            patch("money_pilot.App"),
            self.assertLogs(level=logging.DEBUG) as logs,
            patch("money_pilot.get_db", mock_get_db),
            patch("money_pilot.get_websocket", mock_get_websocket),
            patch(
                "asyncio.Future",
                AsyncMock(
                    name="MockFuture", side_effect=[DBRestart, KeyboardInterrupt]
                ),
            ) as future,
            self.assertRaises(KeyboardInterrupt, msg="expect KeyboardInterrupt"),
        ):
            await main()
        self.assertEqual(mock_get_db.call_count, 2, "expect get_db called twice")
        self.assertEqual(
            mock_get_db.return_value.__aenter__.await_count,
            2,
            "expect get_db context manager entered twice",
        )
        self.assertEqual(mock_get_db.return_value.__aenter__.await_args_list, [(), ()])
        mock_get_websocket.assert_called_once_with()
        mock_get_websocket.return_value.__aenter__.assert_awaited_once_with()
        self.assertEqual(future.await_count, 2, "expected Future awaited twice")
        # self.assertEqual(len(logs.output), 4)
