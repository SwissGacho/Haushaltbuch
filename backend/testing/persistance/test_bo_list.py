"""Test suite for business object list attributes."""

from typing import Optional
import unittest
from unittest import mock
from unittest.mock import AsyncMock, Mock, patch
from business_objects.bo_list import BOSubscription, BOList
from business_objects.business_object_base import BOBase


class MockBOBase(BOBase):

    def __new__(cls, id: int | None = None, *args, **attributes):
        print(f"MockBOBase.__new__({cls=}, {id=}, {args=}, {attributes=})")
        return super().__new__(cls)


class MockConcreteBO(MockBOBase):
    @classmethod
    async def get_matching_ids(cls, conditions: Optional[dict] = None) -> list[int]:
        return []


class MockConnection:
    pass


class Test_100__BOSubscription(unittest.IsolatedAsyncioTestCase):

    async def test101_test_initialization(self):
        con = Mock()
        MockConcreteBO.subscribe_to_instance = Mock(return_value=456)
        con.unregister_other_senders = Mock()
        boSubscription = BOSubscription(bo_type=MockConcreteBO, connection=con, id=1)
        con.unregister_other_senders.assert_called_once_with(boSubscription)
        self.assertEqual(boSubscription._bo_type, MockConcreteBO)
        MockConcreteBO.subscribe_to_instance.assert_called_once_with(
            boSubscription._handle_event_
        )

    async def test102_get_objects(self):
        con = Mock()
        boSubscription = BOSubscription(bo_type=MockConcreteBO, connection=con, id=42)
        objects = await boSubscription._get_objects_()
        self.assertEqual([boSubscription._obj], objects)

    async def test103_handle_event(self):
        con = Mock()
        with patch(
            "business_objects.bo_list.BOSubscription.notify_subscription_subscribers",
            new_callable=AsyncMock,
        ) as mock_notify:
            boSubscription = BOSubscription(
                bo_type=MockConcreteBO, connection=con, id=42
            )
            await boSubscription._handle_event_(boSubscription._obj)
            mock_notify.assert_awaited_once()

    async def test104_notify_subscription_subscribers(self):
        con = Mock()
        with patch(
            "business_objects.bo_list.BOSubscription.send_message",
            new_callable=AsyncMock,
        ) as mock_send_message:
            boSubscription = BOSubscription(
                bo_type=MockConcreteBO, connection=con, id=42
            )
            await boSubscription.notify_subscription_subscribers()
            mock_send_message.assert_awaited_once()

    async def test105_cleanup(self):
        MockConcreteBO.unsubscribe_from_instance = Mock()
        con = Mock()
        con.unregister_message_sender = Mock()
        bo_subscription = BOSubscription(bo_type=MockConcreteBO, connection=con, id=42)
        subscription_id = bo_subscription._subscription_id
        bo_subscription.cleanup()
        MockConcreteBO.unsubscribe_from_instance.assert_called_once_with(
            subscription_id
        )
        con.unregister_message_sender.assert_called_once()

    def setUp(self) -> None:
        self.patcher = patch("business_objects.bo_list.BOBase", MockBOBase)
        # self.conPatcher = patch("server.ws_connection.WS_Connection", MockConnection)
        # self.conPatcher.start()
        self.patcher.start()

    def tearDown(self):
        # self.conPatcher.stop()
        self.patcher.stop()


class Test_200__BOList(unittest.IsolatedAsyncioTestCase):

    async def test201_initialization(self):
        MockConcreteBO.subscribe_to_all_changes = Mock(return_value=456)
        con = Mock()
        boList = BOList(bo_type=MockConcreteBO, connection=con)
        self.assertEqual(boList._bo_type, MockConcreteBO)
        MockConcreteBO.subscribe_to_all_changes.assert_called_once_with(
            boList._handle_event_
        )

    async def test202_notify_subscription_subscribers(self):
        con = Mock()
        with patch(
            "business_objects.bo_list.BOList.send_message", new_callable=AsyncMock
        ) as mock_send_message:
            boList = BOList(bo_type=MockConcreteBO, connection=con)
            await boList.notify_subscription_subscribers()
            mock_send_message.assert_awaited_once()

    async def test203_get_objects(self):
        con = Mock()
        mock_concrete_BO = MockConcreteBO()
        mock_concrete_BO.id = 42
        MockConcreteBO.get_matching_ids = AsyncMock(return_value=[42])
        boList = BOList(bo_type=MockConcreteBO, connection=con)
        objects = await boList._get_objects_()
        for obj in objects:
            self.assertIsInstance(obj, MockConcreteBO)
            self.assertEqual(obj.id, 42)

    async def test_204_cleanup(self):
        MockConcreteBO.unsubscribe_from_all_changes = Mock()
        con = Mock()
        con.unregister_message_sender = Mock()
        boList = BOList(bo_type=MockConcreteBO, connection=con)
        subscription_id = boList._subscription_id
        boList.cleanup()
        MockConcreteBO.unsubscribe_from_all_changes.assert_called_once_with(
            boList._subscription_id
        )
        con.unregister_message_sender.assert_called_once()

    def setUp(self) -> None:
        patcher = patch("business_objects.bo_list.BOBase", MockBOBase)
        patcher.start()
        self.addCleanup(patcher.stop)
        # self.conPatcher = patch("server.ws_connection.WS_Connection", MockConnection)
        # self.conPatcher.start()
