"""Test suite for BOData"""

import unittest

from business_objects.bo_data import BOData


class MockBOClass:
    "Minimal stand-in for a BOBaseBase subclass, only needs attributes_as_dict()"

    @classmethod
    def attributes_as_dict(cls):
        return {"attr_a": str, "attr_b": int}


class MockDescriptor:
    "Minimal stand-in for a _PersistentAttr descriptor, only needs my_name"

    def __init__(self, my_name):
        self.my_name = my_name


class Test_100_BOData(unittest.TestCase):

    def test_101_initialization_prepopulates_attributes_to_none(self):
        data = BOData(MockBOClass)
        self.assertEqual(dict(data.items()), {"attr_a": None, "attr_b": None})

    def test_102_setitem_getitem_round_trip(self):
        data = BOData(MockBOClass)
        data["attr_a"] = "value"
        self.assertEqual(data["attr_a"], "value")
        self.assertIsNone(data["attr_b"])

    def test_103_getitem_unknown_key_returns_none(self):
        data = BOData(MockBOClass)
        self.assertIsNone(data["non_existent"])

    def test_104_contains_and_iter(self):
        data = BOData(MockBOClass)
        self.assertIn("attr_a", data)
        self.assertNotIn("non_existent", data)
        self.assertEqual(set(iter(data)), {"attr_a", "attr_b"})

    def test_105_get_data_set_data_via_descriptor(self):
        data = BOData(MockBOClass)
        descriptor = MockDescriptor("attr_b")
        data.set_data(descriptor, 42)
        self.assertEqual(data.get_data(descriptor), 42)

    def test_106_str_representation(self):
        data = BOData(MockBOClass)
        self.assertEqual(str(data), "BOData(MockBOClass)")
