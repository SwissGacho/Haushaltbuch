"""Test suite for Business Objects Base"""

import datetime
import unittest
import weakref
from unittest.mock import AsyncMock, Mock, patch

from business_objects.bo_semantic_role import BOSemanticRole
from business_objects.business_object_base import AttributeDescription, BOBase
from business_objects.business_attribute_base import BaseFlag
from core.exceptions import DataError
from business_objects.bo_descriptors import (
    AttributeAccessLevel,
    AttributeType,
    BOFlag,
    BOStr,
    BODescriptorList,
    BORelation,
    BOColumnConstraint,
    BOBaseBase,
)

MOCK_TAB1 = "mock_table"
MOCK_TAB2 = "mockbo2s"


class MockFlag(BaseFlag):
    OPTION_A = 1
    OPTION_B = 2
    OPTION_C = 3


class MockBO1(BOBase):
    _table = MOCK_TAB1


class MockBO2(BOBase):
    mock_attr1 = BOStr()
    mock_attr2 = BORelation(MockBO1)
    mock_attr3 = BODescriptorList()
    mock_attr4 = BOFlag(MockFlag)


class MockBO3(MockBO2):
    mock_attr5 = BOStr()

    def __init__():
        super().__init__()
        self.mock_attr5 = "mock attribute 5"


mock_attr_desc = [
    AttributeDescription(
        name="bo_name",
        data_type=str,
        constraint=BOColumnConstraint.BOC_NONE,
        constraint_values={"semantic_role": BOSemanticRole.RAW},
        attribute_type=AttributeType.ATYPE_STR,
        access_level=AttributeAccessLevel.AAL_READ_ONLY,
    ),
    AttributeDescription(
        name="id",
        data_type=int,
        constraint=BOColumnConstraint.BOC_PK_INC,
        constraint_values={"semantic_role": BOSemanticRole.RAW},
        attribute_type=AttributeType.ATYPE_INT,
        access_level=AttributeAccessLevel.AAL_READ_ONLY,
    ),
    AttributeDescription(
        name="last_updated",
        data_type=datetime.datetime,
        constraint=BOColumnConstraint.BOC_DEFAULT_CURR
        | BOColumnConstraint.BOC_ON_UPDATE_CURR,
        constraint_values={"semantic_role": BOSemanticRole.RAW},
        attribute_type=AttributeType.ATYPE_DATETIME,
        access_level=AttributeAccessLevel.AAL_READ_ONLY,
    ),
    AttributeDescription(
        name="mock_attr1",
        data_type=str,
        constraint=BOColumnConstraint.BOC_NONE,
        constraint_values={"semantic_role": BOSemanticRole.RAW},
        attribute_type=AttributeType.ATYPE_STR,
        access_level=AttributeAccessLevel.AAL_READ_WRITE,
    ),
    AttributeDescription(
        name="mock_attr2",
        data_type=BOBaseBase,
        constraint=BOColumnConstraint.BOC_FK,
        constraint_values={"semantic_role": BOSemanticRole.RAW, "relation": MockBO1},
        attribute_type=AttributeType.ATYPE_RELATION,
        access_level=AttributeAccessLevel.AAL_READ_WRITE,
    ),
    AttributeDescription(
        name="mock_attr3",
        data_type=list,
        constraint=BOColumnConstraint.BOC_NONE,
        constraint_values={"semantic_role": BOSemanticRole.RAW},
        attribute_type=AttributeType.ATYPE_LIST,
        access_level=AttributeAccessLevel.AAL_READ_WRITE,
    ),
    AttributeDescription(
        name="mock_attr4",
        data_type=BaseFlag,
        constraint=BOColumnConstraint.BOC_NONE,
        constraint_values={"semantic_role": BOSemanticRole.RAW, "flag_type": MockFlag},
        attribute_type=AttributeType.ATYPE_FLAG,
        access_level=AttributeAccessLevel.AAL_READ_WRITE,
    ),
    AttributeDescription(
        name="mock_attr5",
        data_type=str,
        constraint=BOColumnConstraint.BOC_NONE,
        constraint_values={"semantic_role": BOSemanticRole.RAW},
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

    def setUp(self) -> None:
        # MockBO1/2/3 are module-level classes, so their weak, class-level
        # id-registries (_data_objects, _loaded_instances) would otherwise
        # leak state across test methods. Reset them before every test so
        # tests reusing simple literal ids (e.g. bo_id=1) stay isolated.
        for bo_class in (MockBO1, MockBO2, MockBO3):
            bo_class._data_objects = weakref.WeakValueDictionary()
            bo_class._loaded_instances = weakref.WeakSet()

    def test_100_new_instance(self):
        bo_instance_1 = MockBO1(bo_id=1)
        self.assertIsInstance(bo_instance_1, MockBO1)
        self.assertEqual(bo_instance_1.id, 1)

        mock_db_data = {
            "id": 99,
            "last_updated": datetime.datetime(1111, 1, 1, 0, 0),
            "mock_attr1": "db mock attribute 1",
            "mock_attr2": bo_instance_1,
            "mock_attr3": [1, 2, 3],
            "mock_attr4": MockFlag.OPTION_B,
        }
        bo_instance = MockBO2(bo_id=99, mock_attr3=[0, 1])
        self.assertIsInstance(bo_instance, MockBO2)
        self.assertEqual(bo_instance.id, 99)
        self.assertIsNone(bo_instance.mock_attr1)
        self.assertIsNone(bo_instance.mock_attr2)
        self.assertEqual(bo_instance.mock_attr3, [0, 1])
        self.assertIsNone(bo_instance.mock_attr4)

        bo_instance._db_data = mock_db_data
        bo_instance.mock_attr1 = "new mock attribute 1"
        bo_instance.mock_attr2 = bo_instance_1
        bo_instance.mock_attr3 = [1, 2, 3]
        bo_instance.mock_attr4 = MockFlag.OPTION_A
        self.assertEqual(bo_instance.mock_attr1, "new mock attribute 1")
        self.assertEqual(bo_instance.mock_attr2, bo_instance_1)
        self.assertEqual(bo_instance.mock_attr3, [1, 2, 3])
        self.assertEqual(bo_instance.mock_attr4, MockFlag.OPTION_A)

        mock_new_data = {
            "id": 99,
            "last_updated": datetime.datetime(2222, 2, 2, 0, 0),
            "mock_attr1": "new mock data 1",
            "mock_attr2": bo_instance_1,
            "mock_attr3": [11, 22, 33],
            "mock_attr4": MockFlag.OPTION_C,
        }
        bo_new_instance = MockBO2(
            **{("bo_id" if k == "id" else k): v for k, v in mock_new_data.items()}
        )
        self.assertIsNot(
            bo_new_instance,
            bo_instance,
            msg="Creating a new instance with the same id should return the existing instance",
        )
        self.assertIs(
            bo_new_instance._data,
            bo_instance._data,
            msg="_data should be the same for both instances",
        )
        for key, value in mock_new_data.items():
            self.assertEqual(
                bo_new_instance._data[key],
                value,
                msg=f"_data[{key}] should be updated with new value from instance creation",
            )
        self.assertEqual(
            bo_instance._db_data,
            mock_db_data,
            msg="_db_data should not be overwritten by new instance creation",
        )

    def test_101_register_instance(self):
        bo_instance = MockBO1()
        bo_instance._assign_id(1)
        MockBO1.register_instance(bo_instance)
        self.assertIn(bo_instance, MockBO1._loaded_instances)

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

    def test_103_register_bo_class(self):
        self.assertNotIn("mockbo2", BOBase._business_objects)
        MockBO2.register_bo_class()
        self.assertIn("mockbo2", BOBase._business_objects)

    def test_104_all_business_objects(self):
        bos = MockBO2._business_objects
        self.assertEqual(bos, MockBO2.all_business_objects)

    def test_105_get_business_object_by_name(self):
        MockBO2.register_bo_class()
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
        callback = AsyncMock()
        subscriber_id = MockBO2.subscribe_to_all_changes(callback)
        self.assertEqual(
            MockBO2._change_subscribers[subscriber_id],
            callback,
        )

    def test_114_unsubscribe_from_all_changes(self):
        callback = AsyncMock()
        subscriber_id = MockBO2.subscribe_to_all_changes(callback)
        MockBO2.unsubscribe_from_all_changes(subscriber_id)
        self.assertNotIn(subscriber_id, MockBO2._change_subscribers)
        second_unsubscribe = MockBO2.unsubscribe_from_all_changes(subscriber_id)
        self.assertIsNone(second_unsubscribe)

    def test_115_subscribe_to_instance(self):
        bo_instance = MockBO2()
        bo_instance._assign_id(1)
        callback = AsyncMock()
        subscriber_id = bo_instance.subscribe_to_instance(callback)
        self.assertEqual(
            bo_instance._instance_subscribers[subscriber_id],
            callback,
        )

    def test_116_unsubscribe_from_instance(self):
        bo_instance = MockBO2()
        bo_instance._assign_id(1)
        callback = AsyncMock()
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
            "bo_name": None,
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

    def test_118_get_business_object_by_name(self):
        MockBO2.register_bo_class()
        self.assertEqual(BOBase.get_business_object_by_name("mockbo2"), MockBO2)
        with self.assertRaises(ValueError):
            BOBase.get_business_object_by_name("non_existent_bo")

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
            bo_instance._assign_id(1)
            bo_instance.notify_instance_subscribers()
            MockBOBaseNotify.assert_called_once_with(
                bo_instance._instance_subscribers, bo_instance
            )

    def test_121_notify_change_subscribers(self):
        with patch(
            "business_objects.business_object_base.BOBase.notify_bo_subscribers"
        ) as MockBOBaseNotify:
            bo_instance = MockBO2()
            bo_instance._assign_id(1)
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
            bo_instance._assign_id(1)
            callback = Mock()
            callback.__name__ = "callback"
            BOBase.notify_bo_subscribers({1: callback}, bo_instance)
            MockCreateTask.assert_called_once_with(
                callback(bo_instance), name="subscriber_callback_callback_1"
            )
            mockTask.add_done_callback.assert_called_once_with(
                bo_instance.handle_callback_result
            )

    def test_123_get_data_set_data(self):
        bo_instance = MockBO2()

        bo_instance.set_data(MockBO2.mock_attr1, "direct value")
        self.assertEqual(bo_instance.get_data(MockBO2.mock_attr1), "direct value")

        unnamed_descriptor = BOStr()
        with self.assertRaises(ValueError):
            bo_instance.get_data(unnamed_descriptor)
        with self.assertRaises(ValueError):
            bo_instance.set_data(unnamed_descriptor, "value")

    def test_124_register_instance_shares_data_with_deferred_id_assignment(self):
        """When an instance's id becomes known only after construction (as happens
        for a newly inserted object via 'self.id = <new id>'), any other instance
        later constructed with that same bo_id must share the same underlying
        BOData -- not a disconnected, empty one -- so that attribute values and
        change notifications stay consistent across all instances of that id.
        """
        first = MockBO2()
        first.mock_attr1 = "original value"
        first._assign_id(424242)

        second = MockBO2(bo_id=424242)

        self.assertIs(second._data, first._data)
        self.assertEqual(second.mock_attr1, "original value")

        first.mock_attr1 = "updated value"
        self.assertEqual(second.mock_attr1, "updated value")

    def test_125_register_instance_conflicting_data_raises(self):
        """If _data_objects already holds a BOData for a given id (because some
        other instance registered it first) and the registering instance carries
        its own, different BOData, there is no safe way to decide which data
        should win -- this is a genuine data-consistency conflict and must raise
        rather than silently discarding either side's values.
        """
        first = MockBO2()
        first.mock_attr1 = "first value"
        first._assign_id(555555)

        second = MockBO2()
        second.mock_attr1 = "second value, conflicts with first"
        with self.assertRaises(DataError):
            second._assign_id(555555)
