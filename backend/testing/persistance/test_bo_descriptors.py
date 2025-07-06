"""Test suite for business object attributes descriptors."""

import datetime
from enum import auto
import unittest

import persistance.bo_descriptors


class MockAttr(persistance.bo_descriptors._PersistantAttr):
    @classmethod
    def data_type(cls):
        return str

    def validate(self, value):
        return value is None or isinstance(value, str)


class MockBO:
    mock_attr = MockAttr()
    _add_attributes_args = None

    def __init__(self, attr) -> None:
        self._data = {}
        self.mock_attr = attr

    @classmethod
    def add_attribute(cls, attribute_name, data_type, constraint_flag, **flag_values):
        cls._add_attributes_args = (
            attribute_name,
            data_type,
            constraint_flag,
            flag_values,
        )


class Test_100__PersistantAttr(unittest.TestCase):
    def test_101_initialization(self):
        self.assertEqual(MockBO("mick").mock_attr, "mick")
        print(f"{MockBO.__dict__['mock_attr'].__dict__=}")

    def test_102_set_and_get(self):
        mock_bo = MockBO(None)
        self.assertIsNone(mock_bo.mock_attr)
        mock_bo.mock_attr = "new_value"
        self.assertEqual(mock_bo.mock_attr, "new_value")

    def test_103_set_name(self):
        self.assertEqual(MockBO.mock_attr.my_name, "mock_attr")
        print(f"{MockBO._add_attributes_args=}")
        self.assertEqual(
            MockBO._add_attributes_args,
            (
                "mock_attr",
                str,
                persistance.bo_descriptors.BOColumnFlag.BOC_NONE,
                {},
            ),
        )

    def test_104_get_without_instance(self):
        self.assertIsInstance(MockBO.mock_attr, MockAttr)

    def test_105_with_multiple_instances(self):
        mock_bo1 = MockBO("value1")
        mock_bo2 = MockBO("value2")

        self.assertEqual(mock_bo1.mock_attr, "value1")
        self.assertEqual(mock_bo2.mock_attr, "value2")


class MockRel(persistance.bo_descriptors.BOBaseBase):
    pass


class MockNotRel(persistance.bo_descriptors.BOBaseBase):
    pass


class MockFlag(persistance.bo_descriptors.BaseFlag):
    FLAG_1 = auto()
    FLAG_2 = auto()


class MockObj(persistance.bo_descriptors.BOBaseBase):
    _attributes = {"MockObj": []}
    _data = {}
    int_attr = persistance.bo_descriptors.BOInt(
        persistance.bo_descriptors.BOColumnFlag.BOC_PK_INC
    )
    str_attr = persistance.bo_descriptors.BOStr(
        persistance.bo_descriptors.BOColumnFlag.BOC_NOT_NULL
    )
    dt_attr = persistance.bo_descriptors.BODatetime(
        persistance.bo_descriptors.BOColumnFlag.BOC_DEFAULT_CURR
    )
    d_attr = persistance.bo_descriptors.BODate()
    dict_attr = persistance.bo_descriptors.BODict(
        persistance.bo_descriptors.BOColumnFlag.BOC_DEFAULT, default={"a": 1, "b": 2}
    )
    list_attr = persistance.bo_descriptors.BOList()
    rel_attr = persistance.bo_descriptors.BORelation(MockRel)
    flag_attr = persistance.bo_descriptors.BOFlag(flag_type=MockFlag)

    @classmethod
    def add_attribute(cls, attribute_name, data_type, constraint_flag, **flag_values):
        cls._attributes["MockObj"].append(
            (attribute_name, data_type, constraint_flag, flag_values)
        )


expected_attributes = {
    "MockObj": [
        ("int_attr", int, persistance.bo_descriptors.BOColumnFlag.BOC_PK_INC, {}),
        ("str_attr", str, persistance.bo_descriptors.BOColumnFlag.BOC_NOT_NULL, {}),
        (
            "dt_attr",
            datetime.datetime,
            persistance.bo_descriptors.BOColumnFlag.BOC_DEFAULT_CURR,
            {},
        ),
        (
            "d_attr",
            datetime.date,
            persistance.bo_descriptors.BOColumnFlag.BOC_NONE,
            {},
        ),
        (
            "dict_attr",
            dict,
            persistance.bo_descriptors.BOColumnFlag.BOC_DEFAULT,
            {"default": {"a": 1, "b": 2}},
        ),
        ("list_attr", list, persistance.bo_descriptors.BOColumnFlag.BOC_NONE, {}),
        (
            "rel_attr",
            persistance.bo_descriptors.BOBaseBase,
            persistance.bo_descriptors.BOColumnFlag.BOC_FK,
            {"relation": MockRel},
        ),
        (
            "flag_attr",
            persistance.bo_descriptors.BaseFlag,
            persistance.bo_descriptors.BOColumnFlag.BOC_NONE,
            {"flag_type": MockFlag},
        ),
    ]
}


class Test_200_BOAttributes(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_obj = MockObj()

    def test_201_attributes(self):
        self.maxDiff = None
        self.assertEqual(self.mock_obj._attributes, expected_attributes)

    def test_202_validate_set_get(self):
        self.mock_obj.int_attr = 11
        self.mock_obj.str_attr = "str"
        self.mock_obj.dt_attr = "2020-02-20 20:20"
        self.mock_obj.d_attr = "2020-02-20"
        self.mock_obj.list_attr = [1, 2, 3]
        self.mock_obj.dict_attr = {"dict": 99}
        other_obj = MockRel()
        self.mock_obj.rel_attr = other_obj
        self.mock_obj.flag_attr = MockFlag(1)

        self.assertEqual(self.mock_obj.int_attr, 11)
        self.assertEqual(self.mock_obj.str_attr, "str")
        self.assertEqual(
            self.mock_obj.dt_attr, datetime.datetime.fromisoformat("2020-02-20 20:20")
        )
        self.assertEqual(
            self.mock_obj.d_attr, datetime.date.fromisoformat("2020-02-20")
        )
        self.assertEqual(self.mock_obj.list_attr, [1, 2, 3])
        self.assertEqual(self.mock_obj.dict_attr, {"dict": 99})
        self.assertEqual(self.mock_obj.rel_attr, other_obj)
        self.assertEqual(self.mock_obj.flag_attr, MockFlag.FLAG_1)

    def test_203_validate_fails(self):
        with self.assertRaises(ValueError, msg="BOInt"):
            self.mock_obj.int_attr = "11"
        with self.assertRaises(ValueError, msg="BOStr"):
            self.mock_obj.str_attr = 0
        with self.assertRaises(ValueError, msg="BODatetime"):
            self.mock_obj.dt_attr = "2222-22-22 22:22"
        with self.assertRaises(ValueError, msg="BODate"):
            self.mock_obj.d_attr = "4444-44-44"
        with self.assertRaises(ValueError, msg="BOList"):
            self.mock_obj.list_attr = {}
        with self.assertRaises(ValueError, msg="BODict"):
            self.mock_obj.dict_attr = []
        with self.assertRaises(ValueError, msg="BORelation"):
            self.mock_obj.rel_attr = MockNotRel()
        with self.assertRaises(ValueError, msg="BOFlag"):
            self.mock_obj.flag_attr = 1

    # @unittest.skip("allow set 'NOT NULL' to None")
    def test_204_NOT_NULL(self):
        self.mock_obj.int_attr = None
        with self.assertRaises(ValueError, msg="set 'NOT NULL' attribute to None"):
            self.mock_obj.str_attr = None

    def test_205_Flag(self):
        self.mock_obj.flag_attr = MockFlag(3)
        self.assertEqual(self.mock_obj.flag_attr, MockFlag.FLAG_1 | MockFlag.FLAG_2)
        self.assertEqual(str(self.mock_obj.flag_attr), "flag_1,flag_2")

        self.mock_obj._data["flag_attr"] = "flag_1"
        self.assertIsInstance(self.mock_obj._data["flag_attr"], str)
        self.assertIsInstance(self.mock_obj.flag_attr, MockFlag)
        self.assertEqual(self.mock_obj.flag_attr, MockFlag.FLAG_1)

        self.mock_obj.flag_attr |= MockFlag.FLAG_2
        self.assertEqual(self.mock_obj.flag_attr, MockFlag.FLAG_1 | MockFlag.FLAG_2)
        self.assertIsInstance(self.mock_obj._data["flag_attr"], MockFlag)
        self.assertIsInstance(self.mock_obj.flag_attr, MockFlag)
        self.assertEqual(self.mock_obj.flag_attr, MockFlag(0b11))
