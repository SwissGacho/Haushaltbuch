""" Test suite for business object attributes descriptors."""

import datetime
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
    _attributes = {}

    def __init__(self, attr) -> None:
        self._data = {}
        self.mock_attr = attr


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
        self.assertEqual(
            MockBO._attributes,
            {
                "MockBO": [
                    (
                        "mock_attr",
                        str,
                        persistance.bo_descriptors.BOColumnFlag.BOC_NONE,
                        {},
                    )
                ]
            },
        )

    def test_104_get_without_instance(self):
        self.assertIsInstance(MockBO.mock_attr, MockAttr)

    def test_105_with_multiple_instances(self):
        mock_bo1 = MockBO("value1")
        mock_bo2 = MockBO("value2")

        self.assertEqual(mock_bo1.mock_attr, "value1")
        self.assertEqual(mock_bo2.mock_attr, "value2")


class MockObj(persistance.bo_descriptors.BOBaseBase):
    _attributes = {}
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
    rel_attr = persistance.bo_descriptors.BORelation(
        persistance.bo_descriptors.BOBaseBase
    )


class MockDerived_1(MockObj):
    pass


class MockDerived_2(MockObj):
    pass


class MockNotDerived:
    pass


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
            {"relation": persistance.bo_descriptors.BOBaseBase},
        ),
    ]
}


class Test_200_BOAttributes(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_obj = MockObj()

    def test_201_attributes(self):
        self.assertEqual(self.mock_obj._attributes, expected_attributes)

    def test_202_validate_set_get(self):
        self.mock_obj.int_attr = 11
        self.mock_obj.str_attr = "str"
        self.mock_obj.dt_attr = "2020-02-20 20:20"
        self.mock_obj.d_attr = "2020-02-20"
        self.mock_obj.list_attr = [1, 2, 3]
        self.mock_obj.dict_attr = {"dict": 99}
        other_obj = MockDerived_1()
        self.mock_obj.rel_attr = other_obj

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
        self.assertNotEqual(self.mock_obj.rel_attr, MockDerived_2())

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
            self.mock_obj.rel_attr = MockNotDerived()

    @unittest.skip("allow set 'NOT NULL' to None")
    def test_204_NOT_NULL(self):
        self.mock_obj.int_attr = None
        with self.assertRaises(ValueError, msg="set 'NOT NULL' attribute to None"):
            self.mock_obj.str_attr = None
