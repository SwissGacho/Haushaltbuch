"""Test suite for Business Objects Base"""

import datetime
import unittest
from unittest.mock import DEFAULT, Mock, AsyncMock, patch, call, ANY

from business_objects.business_object_base import AttributeDescription, BOBase
from business_objects.bo_descriptors import (
    BOStr,
    BOList,
    BORelation,
    BOColumnFlag,
    BOBaseBase,
)

MOCK_TAB1 = "mock_table"
MOCK_TAB2 = "mockbo2s"


class MockBO1(BOBase):
    _table = MOCK_TAB1


class MockBO2(BOBase):
    mock_attr1 = BOStr()
    mock_attr2 = BORelation(MockBO1)
    mock_attr3 = BOList()

    def __init__(
        self,
        bo_id=None,
        mock_attr1="mock attriubute 1",
        mock_attr2=None,
        mock_attr3=[],
    ) -> None:
        super().__init__(bo_id=bo_id)
        self.mock_attr1 = mock_attr1
        self.mock_attr2 = mock_attr2
        self.mock_attr3 = mock_attr3


mock_attr_desc = [
    AttributeDescription("id", int, BOColumnFlag.BOC_PK_INC, {}),
    AttributeDescription(
        "last_updated", datetime.datetime, BOColumnFlag.BOC_DEFAULT_CURR, {}
    ),
    AttributeDescription("mock_attr1", str, BOColumnFlag.BOC_NONE, {}),
    AttributeDescription(
        "mock_attr2", BOBaseBase, BOColumnFlag.BOC_FK, {"relation": MockBO1}
    ),
    AttributeDescription("mock_attr3", list, BOColumnFlag.BOC_NONE, {}),
]

mock_bo2_as_dict = {a.name: a.data_type for a in mock_attr_desc}


class Test_100_BOBase_classmethods(unittest.IsolatedAsyncioTestCase):

    def test_101_register_persistant_class(self):
        self.assertNotIn("mockbo2", BOBase._business_objects)
        MockBO2.register_persistant_class()
        self.assertIn("mockbo2", BOBase._business_objects)

    def test_102_all_business_objects(self):
        bos = MockBO2._business_objects
        self.assertEqual(bos, MockBO2.all_business_objects)

    def test_103_table(self):
        self.assertEqual(MockBO2.table, MOCK_TAB2)
        self.assertEqual(MockBO1.table, MOCK_TAB1)

    def test_104_attributes_as_dict(self):
        self.assertEqual(
            MockBO2.attributes_as_dict().keys(),
            mock_bo2_as_dict.keys(),
        )

    def test_105_attribute_descriptions(self):
        print(f"{MockBO2.attribute_descriptions()=}")
        print(f"{mock_attr_desc=}")
        self.assertEqual(MockBO2.attribute_descriptions(), mock_attr_desc)

    def test_106_get_business_object_by_name(self):
        MockBO2.register_persistant_class()
        self.assertEqual(BOBase.get_business_object_by_name("mockbo2"), MockBO2)
        with self.assertRaises(ValueError):
            BOBase.get_business_object_by_name("non_existent_bo")
