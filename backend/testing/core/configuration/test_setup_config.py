""" Test suite for configuration setup """

import asyncio

import unittest
from unittest.mock import Mock, AsyncMock, patch, call

from core.configuration.setup_config import (
    ConfigSetup,
    WAIT_AVAILABLE_TASK,
    WAIT_FAILURE_TASK,
)


class MockTask:
    def __init__(self) -> None:
        self.cancel = Mock()
        self.get_name = Mock()


class TestConfigSetup(unittest.IsolatedAsyncioTestCase):

    async def _100_wait_for_db(self, available=True):
        mock_done_task = MockTask()
        mock_done_task.get_name.return_value = (
            WAIT_AVAILABLE_TASK if available else WAIT_FAILURE_TASK
        )
        mock_pending_task = MockTask()
        mock_avail_task = "mock-available-task"
        mock_fail_task = "mock-failure-task"
        with (
            patch("core.configuration.setup_config.App") as MockApp,
            patch("asyncio.wait") as MockAsyncioWait,
            patch("asyncio.create_task") as MockAsyncioCreateTask,
        ):
            MockApp.db_request_restart.set = Mock(name="db_request_restart.set")
            MockApp.db_restart.wait = AsyncMock(name="db_restart.wait")
            MockApp.db_available.wait = Mock(name="db_available.wait")
            MockApp.db_failure.wait = Mock(name="db_failure.wait")
            MockAsyncioWait.return_value = ([mock_done_task], [mock_pending_task])
            MockAsyncioCreateTask.side_effect = [mock_avail_task, mock_fail_task]

            result = await ConfigSetup._wait_for_db()

            MockApp.db_request_restart.set.assert_called_once_with()
            MockApp.db_restart.wait.assert_awaited_once_with()
            self.assertEqual(
                MockAsyncioCreateTask.call_args_list,
                [
                    call(MockApp.db_available.wait(), name=WAIT_AVAILABLE_TASK),
                    call(MockApp.db_failure.wait(), name=WAIT_FAILURE_TASK),
                ],
            )
            MockAsyncioWait.assert_awaited_once_with(
                [mock_avail_task, mock_fail_task], return_when=asyncio.FIRST_COMPLETED
            )
            mock_pending_task.cancel.assert_called_once_with()
            mock_pending_task.get_name.assert_not_called()
            mock_done_task.get_name.assert_called_once_with()
            mock_done_task.cancel.assert_not_called()
            if available:
                self.assertTrue(result)
            else:
                self.assertFalse(result)

    async def test_101_wait_for_db_available(self):
        await self._100_wait_for_db()

    async def test_102_wait_for_db_available(self):
        await self._100_wait_for_db(available=False)
