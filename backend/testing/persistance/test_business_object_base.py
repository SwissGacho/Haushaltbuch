"""Test suite for Business Objects Base"""

import datetime
import unittest
from unittest.mock import Mock, patch

from business_objects.business_object_base import AttributeDescription, BOBase
from business_objects.business_attribute_base import BaseFlag
from business_objects.bo_descriptors import (
    AttributeAccessLevel,
    AttributeType,
    BOFlag,
    BOStr,
    BOList,
    BORelation,
    BOColumnConstraint,
    BOBaseBase,
)


MOCK_TAB1 = "mock_table"
MOCK_TAB2 = "mockbo2s"


class MockFlag(BaseFlag):
    OPTION_A = 1
    OPTION_B = 2


class MockBO1(BOBase):
    _table = MOCK_TAB1


class MockBO2(BOBase):
    mock_attr1 = BOStr()
    mock_attr2 = BORelation(MockBO1)
    mock_attr3 = BOList()
    mock_attr4 = BOFlag(MockFlag)

    def __init__(
        self,
        bo_id=None,
        mock_attr1="mock attribute 1",
        mock_attr2=None,
        mock_attr3=[],
        mock_attr4=None,
    ) -> None:
        super().__init__(bo_id=bo_id)
        self.mock_attr1 = mock_attr1
        self.mock_attr2 = mock_attr2
        self.mock_attr3 = mock_attr3
        self.mock_attr4 = mock_attr4


class MockBO3(MockBO2):
    mock_attr5 = BOStr()

    def __init__():
        super().__init__()
        self.mock_attr5 = "mock attribute 5"


mock_attr_desc = [
    AttributeDescription(
        name="id",
        data_type=int,
        constraint=BOColumnConstraint.BOC_PK_INC,
        constraint_values={},
        attribute_type=AttributeType.ATYPE_INT,
        access_level=AttributeAccessLevel.AAL_READ_ONLY,
    ),
    AttributeDescription(
        name="last_updated",
        data_type=datetime.datetime,
        constraint=BOColumnConstraint.BOC_DEFAULT_CURR
        | BOColumnConstraint.BOC_ON_UPDATE_CURR,
        constraint_values={},
        attribute_type=AttributeType.ATYPE_DATETIME,
        access_level=AttributeAccessLevel.AAL_READ_ONLY,
    ),
    AttributeDescription(
        name="mock_attr1",
        data_type=str,
        constraint=BOColumnConstraint.BOC_NONE,
        constraint_values={},
        attribute_type=AttributeType.ATYPE_STR,
        access_level=AttributeAccessLevel.AAL_READ_WRITE,
    ),
    AttributeDescription(
        name="mock_attr2",
        data_type=BOBaseBase,
        constraint=BOColumnConstraint.BOC_FK,
        constraint_values={"relation": MockBO1},
        attribute_type=AttributeType.ATYPE_RELATION,
        access_level=AttributeAccessLevel.AAL_READ_WRITE,
    ),
    AttributeDescription(
        name="mock_attr3",
        data_type=list,
        constraint=BOColumnConstraint.BOC_NONE,
        constraint_values={},
        attribute_type=AttributeType.ATYPE_LIST,
        access_level=AttributeAccessLevel.AAL_READ_WRITE,
    ),
    AttributeDescription(
        name="mock_attr4",
        data_type=BaseFlag,
        constraint=BOColumnConstraint.BOC_NONE,
        constraint_values={"flag_type": MockFlag},
        attribute_type=AttributeType.ATYPE_FLAG,
        access_level=AttributeAccessLevel.AAL_READ_WRITE,
    ),
    AttributeDescription(
        name="mock_attr5",
        data_type=str,
        constraint=BOColumnConstraint.BOC_NONE,
        constraint_values={},
        attribute_type=AttributeType.ATYPE_STR,
        access_level=AttributeAccessLevel.AAL_READ_WRITE,
    ),
]

mock_bo3_as_dict = {a.name: a.data_type for a in mock_attr_desc}
mock_bo3_business_as_dict = {
    a.name: a.data_type
    for a in mock_attr_desc
    if a.access_level != AttributeAccessLevel.AAL_WRITE_ONLY
}


class Test_100_BOBase_classmethods(unittest.IsolatedAsyncioTestCase):

    def test_101_register_instance(self):
        bo_instance = MockBO1()
        bo_instance.id = 1
        MockBO1.register_instance(bo_instance)
        self.assertEqual(bo_instance, MockBO1._loaded_instances[1])

    def test_102_add_attribute(self):
        class MockBO102(BOBase):
            pass

        MockBO102.add_attribute(
            attribute_name="new_attr",
            data_type=str,
            constraint_flag=BOColumnConstraint.BOC_NONE,
            attribute_type=AttributeType.ATYPE_STR,
            access_level=AttributeAccessLevel.AAL_READ_WRITE,
        )
        for attr in MockBO102._attributes[MockBO102.__name__]:
            if attr.name == "new_attr":
                self.assertEqual(attr.data_type, str)
                self.assertEqual(attr.constraint, BOColumnConstraint.BOC_NONE)
                self.assertEqual(attr.attribute_type, AttributeType.ATYPE_STR)
                self.assertEqual(attr.access_level, AttributeAccessLevel.AAL_READ_WRITE)
            else:
                self.assertNotEqual(attr.name, "new_attr")

    def test_103_register_persistant_class(self):
        self.assertNotIn("mockbo2", BOBase._business_objects)
        MockBO2.register_persistant_class()
        self.assertIn("mockbo2", BOBase._business_objects)

    def test_104_all_business_objects(self):
        bos = MockBO2._business_objects
        self.assertEqual(bos, MockBO2.all_business_objects)

    def test_105_get_business_object_by_name(self):
        MockBO2.register_persistant_class()
        bo_class = BOBase.get_business_object_by_name("mockbo2")
        self.assertEqual(bo_class, MockBO2)
        with self.assertRaises(ValueError):
            BOBase.get_business_object_by_name("non_existent_bo")

    def test_105_table(self):
        self.assertEqual(MockBO2.table, MOCK_TAB2)
        self.assertEqual(MockBO1.table, MOCK_TAB1)

    def test_106_attributes_as_dict(self):
        self.assertEqual(
            MockBO3.attributes_as_dict().keys(),
            mock_bo3_as_dict.keys(),
        )

    def test_107_business_attributes_as_dict(self):
        self.assertEqual(
            MockBO3.business_attributes_as_dict().keys(),
            mock_bo3_business_as_dict.keys(),
        )

    def test_108_attribute_descriptions(self):
        bo3_attr_desc = MockBO3.attribute_descriptions()
        self.assertEqual(bo3_attr_desc, mock_attr_desc)

    def test_109_primary_key(self):
        self.assertEqual(MockBO2.primary_key(), "id")

    def test_110_references(self):
        refs = MockBO2.references()
        self.assertEqual(refs, [MockBO1])

    def test_111_subscribe_to_creation(self):
        callback = Mock()
        subscriber_id = MockBO2.subscribe_to_creation(callback)
        self.assertEqual(
            MockBO2._creation_subscribers[subscriber_id],
            callback,
        )

    def test_112_unsubscribe_from_creation(self):
        callback = Mock()
        subscriber_id = MockBO2.subscribe_to_creation(callback)
        MockBO2.unsubscribe_from_creation(subscriber_id)
        self.assertNotIn(subscriber_id, MockBO2._creation_subscribers)
        second_unsubscribe = MockBO2.unsubscribe_from_creation(subscriber_id)
        self.assertIsNone(second_unsubscribe)

    def test_113_subscribe_to_all_changes(self):
        callback = Mock()
        subscriber_id = MockBO2.subscribe_to_all_changes(callback)
        self.assertEqual(
            MockBO2._change_subscribers[subscriber_id],
            callback,
        )

    def test_114_unsubscribe_from_all_changes(self):
        callback = Mock()
        subscriber_id = MockBO2.subscribe_to_all_changes(callback)
        MockBO2.unsubscribe_from_all_changes(subscriber_id)
        self.assertNotIn(subscriber_id, MockBO2._change_subscribers)
        second_unsubscribe = MockBO2.unsubscribe_from_all_changes(subscriber_id)
        self.assertIsNone(second_unsubscribe)

    def test_115_subscribe_to_instance(self):
        bo_instance = MockBO2()
        bo_instance.id = 1
        callback = Mock()
        subscriber_id = bo_instance.subscribe_to_instance(callback)
        self.assertEqual(
            bo_instance._instance_subscribers[subscriber_id],
            callback,
        )

    def test_116_unsubscribe_from_instance(self):
        bo_instance = MockBO2()
        bo_instance.id = 1
        callback = Mock()
        subscriber_id = bo_instance.subscribe_to_instance(callback)
        bo_instance.unsubscribe_from_instance(subscriber_id)
        self.assertNotIn(subscriber_id, bo_instance._instance_subscribers)
        second_unsubscribe = bo_instance.unsubscribe_from_instance(subscriber_id)
        self.assertIsNone(second_unsubscribe)

    async def test_117_business_values_as_dict(self):
        bo_instance = MockBO2(
            bo_id=1,
            mock_attr1="test attr 1",
            mock_attr2=None,
            mock_attr3=[],
            mock_attr4=MockFlag.OPTION_A,
        )
        expected_dict = {
            "id": 1,
            "last_updated": None,
            "mock_attr1": "test attr 1",
            "mock_attr2": None,
            "mock_attr3": [],
            "mock_attr4": MockFlag.OPTION_A,
        }
        self.assertEqual(await bo_instance.business_values_as_dict(), expected_dict)
        bo_instance.mock_attr4 = MockFlag.OPTION_B
        expected_dict["mock_attr4"] = MockFlag.OPTION_B
        self.assertEqual(await bo_instance.business_values_as_dict(), expected_dict)

    async def test_119_store(self):
        with patch(
            "business_objects.business_object_base.BOBase.notify_bo_subscribers"
        ) as MockBOBaseNotify:
            bo_instance = MockBO2(
                bo_id=None,
                mock_attr1="test attr 1",
                mock_attr2=None,
                mock_attr3=[],
                mock_attr4=MockFlag.OPTION_A,
            )
            bo_instance.notify_instance_subscribers = Mock()
            await bo_instance.store()
            MockBOBaseNotify.assert_called_once()
            bo_instance.notify_instance_subscribers.assert_called_once()

    def test_120_notify_instance_subscribers(self):
        with patch(
            "business_objects.business_object_base.BOBase.notify_bo_subscribers"
        ) as MockBOBaseNotify:
            bo_instance = MockBO2()
            bo_instance.id = 1
            bo_instance.notify_instance_subscribers()
            MockBOBaseNotify.assert_called_once_with(
                bo_instance._instance_subscribers, bo_instance
            )

    def test_121_notify_change_subscribers(self):
        with patch(
            "business_objects.business_object_base.BOBase.notify_bo_subscribers"
        ) as MockBOBaseNotify:
            bo_instance = MockBO2()
            bo_instance.id = 1
            MockBO2.notify_change_subscribers(bo_instance)
            MockBOBaseNotify.assert_called_once_with(
                bo_instance._change_subscribers, bo_instance
            )

    def test_122_notify_bo_subscribers(self):
        with patch("asyncio.create_task", new=Mock()) as MockCreateTask:
            mockTask = Mock()
            MockCreateTask.return_value = mockTask
            mockTask.add_done_callback = Mock()
            bo_instance = MockBO2()
            bo_instance.id = 1
            callback = Mock()
            callback.__name__ = "callback"
            BOBase.notify_bo_subscribers({1: callback}, bo_instance)
            MockCreateTask.assert_called_once_with(
                callback(bo_instance), name="subscriber_callback_callback_1"
            )
            mockTask.add_done_callback.assert_called_once_with(
                bo_instance.handle_callback_result
            )

    def test_118_get_business_object_by_name(self):
        MockBO2.register_persistant_class()
        self.assertEqual(BOBase.get_business_object_by_name("mockbo2"), MockBO2)
        with self.assertRaises(ValueError):
            BOBase.get_business_object_by_name("non_existent_bo")
