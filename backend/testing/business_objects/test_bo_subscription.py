"""Test suite for business object subscriptions."""

import asyncio
from typing import Optional
import unittest
from unittest.mock import AsyncMock, Mock, patch

import core.exceptions
from business_objects.business_object_base import BOBase
from business_objects.bo_subscription import BOSubscription


class MockBOBase(BOBase):
    @classmethod
    async def count_rows(cls, conditions: Optional[dict] = None) -> int:
        return 0

    @classmethod
    async def get_matching_ids(cls, conditions: Optional[dict] = None) -> list[int]:
        return []

    @classmethod
    async def get_matching_objects(
        cls, conditions: Optional[dict] = None, attributes: list[str] | None = None
    ) -> list[BOBase]:
        return []


class MockConcreteBO(MockBOBase):
    pass


class Test_100__BOSubscription(unittest.IsolatedAsyncioTestCase):
    async def test_101_test_initialization(self):
        con = Mock()
        MockConcreteBO.subscribe_to_instance = Mock(return_value=456)
        con.unregister_other_senders = Mock()
        bo_subscription = BOSubscription(
            bo_type=MockConcreteBO, connection=con, index=1
        )
        con.unregister_other_senders.assert_called_once_with(bo_subscription)
        self.assertEqual(bo_subscription._bo_type, MockConcreteBO)
        MockConcreteBO.subscribe_to_instance.assert_called_once_with(
            bo_subscription._handle_event_
        )

    async def test_102_get_objects(self):
        con = Mock()
        bo_subscription = BOSubscription(
            bo_type=MockConcreteBO, connection=con, index=42
        )
        objects = await bo_subscription._get_objects_()
        self.assertEqual([bo_subscription._obj], objects)

    async def test_103_handle_event(self):
        con = Mock()
        with patch(
            "business_objects.bo_subscription.BOSubscription.notify_subscription_subscribers",
            new_callable=AsyncMock,
        ) as mock_notify:
            bo_subscription = BOSubscription(
                bo_type=MockConcreteBO, connection=con, index=42
            )
            await bo_subscription._handle_event_(MockConcreteBO(bo_id=1))
            mock_notify.assert_awaited_once()

    async def test_104_notify_subscription_subscribers(self):
        con = Mock()
        with patch(
            "business_objects.bo_subscription.BOSubscription.send_message",
            new_callable=AsyncMock,
        ) as mock_send_message:
            bo_subscription = BOSubscription(
                bo_type=MockConcreteBO, connection=con, index=42
            )
            await bo_subscription.notify_subscription_subscribers()
            mock_send_message.assert_awaited_once()

    async def test_105_cleanup(self):
        MockConcreteBO.unsubscribe_from_instance = Mock()
        con = Mock()
        con.unregister_message_sender = Mock()
        bo_subscription = BOSubscription(
            bo_type=MockConcreteBO, connection=con, index=42
        )
        subscription_id = bo_subscription._subscription_id
        bo_subscription.cleanup()
        MockConcreteBO.unsubscribe_from_instance.assert_called_once_with(
            subscription_id
        )
        con.unregister_message_sender.assert_called_once()

    async def test_106_notify_subscribers_on_init_schedules_task(self):
        con = Mock()
        with patch(
            "business_objects.bo_subscription.BOSubscription.notify_subscription_subscribers",
            new_callable=AsyncMock,
        ) as mock_notify:
            BOSubscription(
                bo_type=MockConcreteBO,
                connection=con,
                index=42,
                notify_subscribers_on_init=True,
            )
            await asyncio.sleep(0)
            mock_notify.assert_awaited_once()

    async def test_107_notify_subscribers_on_init_false_does_not_schedule_task(self):
        con = Mock()
        with patch(
            "business_objects.bo_subscription.BOSubscription.notify_subscription_subscribers",
            new_callable=AsyncMock,
        ) as mock_notify:
            BOSubscription(
                bo_type=MockConcreteBO,
                connection=con,
                index=42,
                notify_subscribers_on_init=False,
            )
            await asyncio.sleep(0)
            mock_notify.assert_not_awaited()


class Test_200__handle_notify_task_result(unittest.TestCase):
    def test_201_connection_closed_is_logged_as_debug(self):
        task = Mock()
        task.result.side_effect = core.exceptions.WSConnectionClosed("gone")
        with patch("business_objects.bo_subscription.LOG") as mock_log:
            BOSubscription._handle_notify_task_result(task)
            mock_log.debug.assert_called_once()
            mock_log.exception.assert_not_called()

    def test_202_unexpected_exception_is_logged_as_exception(self):
        task = Mock()
        task.result.side_effect = ValueError("boom")
        with patch("business_objects.bo_subscription.LOG") as mock_log:
            BOSubscription._handle_notify_task_result(task)
            mock_log.exception.assert_called_once()
            mock_log.debug.assert_not_called()

    def test_203_cancelled_error_is_ignored(self):
        task = Mock()
        task.result.side_effect = asyncio.CancelledError()
        with patch("business_objects.bo_subscription.LOG") as mock_log:
            BOSubscription._handle_notify_task_result(task)
            mock_log.debug.assert_not_called()
            mock_log.exception.assert_not_called()

    def test_204_success_logs_nothing(self):
        task = Mock()
        task.result.return_value = None
        with patch("business_objects.bo_subscription.LOG") as mock_log:
            BOSubscription._handle_notify_task_result(task)
            mock_log.debug.assert_not_called()
            mock_log.exception.assert_not_called()
