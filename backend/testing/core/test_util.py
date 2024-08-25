""" Test suite for common utility functions"""

import unittest
from unittest.mock import Mock

import core.util


class MockClass:
    _mock_attr = ""

    def __init__(self) -> None:
        self._mock_attr = "mock instance value"

    @core.util._classproperty
    def get_class_attr(self):
        return self._mock_attr

    @property
    def get_instance_attr(self):
        return self._mock_attr

    @classmethod
    def set_class_attr(cls, val):
        cls._mock_attr = val

    def set_instance_attr(self, val):
        self._mock_attr = val


class Test100ClassProperty(unittest.TestCase):
    def setUp(self) -> None:
        MockClass._mock_attr = "mock class value"
        self.mock_obj = MockClass()
        return super().setUp()

    def test_101_property_like_access(self):
        self.assertEqual(MockClass.get_class_attr, "mock class value")

    def test_102_instance(self):
        self.assertEqual(self.mock_obj.get_instance_attr, "mock instance value")
        self.assertEqual(self.mock_obj.get_class_attr, "mock class value")

    def test_103_changed_class_value_by_func(self):
        self.mock_obj.set_class_attr("new class value")
        self.assertEqual(self.mock_obj.get_instance_attr, "mock instance value")
        self.assertEqual(self.mock_obj.get_class_attr, "new class value")
        self.assertEqual(MockClass.get_class_attr, "new class value")

    def test_104_changed_class_value_by_class(self):
        MockClass._mock_attr = "another class value"
        self.assertEqual(self.mock_obj.get_instance_attr, "mock instance value")
        self.assertEqual(self.mock_obj.get_class_attr, "another class value")
        self.assertEqual(MockClass.get_class_attr, "another class value")

    def test_105_changed_instance_value(self):
        self.mock_obj.set_instance_attr("new instance value")
        self.assertEqual(self.mock_obj.get_instance_attr, "new instance value")
        self.assertEqual(self.mock_obj.get_class_attr, "mock class value")
        self.assertEqual(MockClass.get_class_attr, "mock class value")


class Test200Util(unittest.TestCase):
    "Test module util functions"

    def test_101_get_config_item(self):
        mock_dict = {
            "lvl1a": "val1",
            "lvl1b": {
                "lvl2a": "val2",
                "lvl2b": {"lvl3": "val3"},
            },
        }
        self.assertEqual(core.util.get_config_item(mock_dict, "lvl1a"), "val1")
        self.assertEqual(core.util.get_config_item(mock_dict, "lvl1b/lvl2a"), "val2")
        self.assertEqual(
            core.util.get_config_item(mock_dict, "lvl1b/lvl2b/lvl3"), "val3"
        )
        self.assertIsNone(core.util.get_config_item({}, "mick/mock"))

    def test_201_update_dicts_recursively(self):
        mock_tgt = {
            "lvl1a": "valt1",
            "lvl1b": {
                "lvl2a": "valt2",
                "lvl2b": {"lvl3": "val3"},
            },
            "lvl1c": "oldt1",
        }
        mock_src = {"lvl1c": "new1", "lvl1b": {"lvl2c": "valt2c"}}
        expct = {
            "lvl1a": "valt1",
            "lvl1b": {"lvl2a": "valt2", "lvl2b": {"lvl3": "val3"}, "lvl2c": "valt2c"},
            "lvl1c": "new1",
        }
        core.util.update_dicts_recursively(target=mock_tgt, source=mock_src)
        self.assertEqual(mock_tgt, expct)

    def test_202_update_dicts_recursively_no_dict(self):
        with self.assertRaises(TypeError):
            core.util.update_dicts_recursively(target="val", source={})
        with self.assertRaises(TypeError):
            core.util.update_dicts_recursively(target={}, source=None)
        # no exception:
        core.util.update_dicts_recursively(target={}, source={})
