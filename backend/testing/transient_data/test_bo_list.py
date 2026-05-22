"""Test suite for transient BO list objects."""

from typing import Optional
import unittest
from unittest.mock import AsyncMock, Mock, patch

from business_objects.business_object_base import BOBase
from transient_data.bo_list import BOList


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


class Test_200__BOList(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_get_business_object_by_name = Mock(
            name="get_business_object_by_name", return_value=MockConcreteBO
        )
        MockBOBase.get_business_object_by_name = self.mock_get_business_object_by_name
        self.patchers = {
            patch("transient_data.bo_list.BOBase", MockBOBase),
        }
        for patcher in self.patchers:
            patcher.start()

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()

    async def test_201_initialization(self):
        con = Mock()
        bo_list = BOList(index="mock_bo", bo_type=MockConcreteBO, connection=con)
        self.assertEqual(bo_list._bo_type, MockConcreteBO)
        self.assertIsNone(bo_list._conditions)
        self.mock_get_business_object_by_name.assert_called_once_with("mock_bo")

    async def test_202_initialization_rejects_non_string_index(self):
        with self.assertRaises(TypeError):
            BOList(index=123)

    async def test_203_subscribe_to_instance(self):
        con = Mock()
        MockConcreteBO.subscribe_to_all_changes = Mock(return_value=99)
        bo_list = BOList(index="mock_bo", bo_type=MockConcreteBO, connection=con)

        async def callback(_):
            return None

        callback_id = bo_list.subscribe_to_instance(callback)

        self.assertEqual(callback_id, 1)
        self.assertEqual(bo_list._subscription_id, 99)
        MockConcreteBO.subscribe_to_all_changes.assert_called_once_with(callback)

    async def test_204_unsubscribe_from_instance(self):
        con = Mock()
        MockConcreteBO.subscribe_to_all_changes = Mock(return_value=55)
        MockConcreteBO.unsubscribe_from_all_changes = Mock()
        bo_list = BOList(index="mock_bo", bo_type=MockConcreteBO, connection=con)

        async def callback(_):
            return None

        callback_id = bo_list.subscribe_to_instance(callback)
        bo_list.unsubscribe_from_instance(callback_id)

        MockConcreteBO.unsubscribe_from_all_changes.assert_called_once_with(55)
        self.assertIsNone(bo_list._subscription_id)

    async def test_205_unsubscribe_from_instance_without_class_subscription(self):
        con = Mock()
        MockConcreteBO.unsubscribe_from_all_changes = Mock()
        bo_list = BOList(index="mock_bo", bo_type=MockConcreteBO, connection=con)
        bo_list.unsubscribe_from_instance(1)
        MockConcreteBO.unsubscribe_from_all_changes.assert_not_called()

    async def test_206_business_values_as_dict(self):
        con = Mock()
        conditions = {"category": "food"}
        bo_list = BOList(
            index="mock_bo",
            bo_type=MockConcreteBO,
            connection=con,
            conditions=conditions,
        )
        MockConcreteBO.display_name_components = Mock(return_value=["name"])
        MockConcreteBO.get_matching_objects = AsyncMock(
            return_value=[
                Mock(id=1, display_name="First object"),
                Mock(id=2, display_name="Second object"),
            ]
        )

        result = await bo_list.business_values_as_dict()

        self.assertEqual(
            result,
            {
                "objects": [
                    {"id": 1, "display_name": "First object"},
                    {"id": 2, "display_name": "Second object"},
                ]
            },
        )
        MockConcreteBO.display_name_components.assert_called_once_with()
        MockConcreteBO.get_matching_objects.assert_awaited_once_with(
            attributes=["name"], conditions=conditions
        )
