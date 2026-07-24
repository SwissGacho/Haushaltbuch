"""Test suite for business object attributes descriptors."""

import datetime
from decimal import Decimal
from enum import Flag, auto

import unittest
from unittest.mock import Mock, ANY, patch

import business_objects.bo_descriptors
from business_objects.bo_descriptors import AttributeAccessLevel, BOSelf
from business_objects.bo_semantic_role import BOSemanticRole
from core.exceptions import OperationalError


class MockAttr(business_objects.bo_descriptors._PersistentAttr):
    @classmethod
    def data_type(cls):
        return str

    @classmethod
    def attribute_type(cls) -> business_objects.bo_descriptors.AttributeType:
        return business_objects.bo_descriptors.AttributeType.ATYPE_STR

    def validate(self, value):
        return value is None or isinstance(value, str)


class MockBO:
    mock_attr = MockAttr()
    _add_attributes_args = None

    def __init__(self, attr) -> None:
        self._data = {}
        self.mock_attr = attr

    @classmethod
    def add_attribute(
        cls,
        attribute_name: str,
        data_type: type,
        constraint_flag,
        attribute_type,
        access_level: AttributeAccessLevel = AttributeAccessLevel.AAL_READ_WRITE,
        **flag_values,
    ):
        cls._add_attributes_args = (
            attribute_name,
            data_type,
            constraint_flag,
            attribute_type,
            access_level,
            flag_values,
        )


class Test_100__PersistentAttr(unittest.TestCase):

    def test_101_initialization(self):
        self.assertEqual(MockBO("mick").mock_attr, "mick")

    def test_102_set_and_get(self):
        mock_bo = MockBO(None)
        self.assertIsNone(mock_bo.mock_attr)
        mock_bo.mock_attr = "new_value"
        self.assertEqual(mock_bo.mock_attr, "new_value")

    def test_103_set_name(self):
        self.assertEqual(MockBO.mock_attr.my_name, "mock_attr")
        self.assertEqual(
            MockBO._add_attributes_args,
            (
                "mock_attr",
                str,
                business_objects.bo_descriptors.BOColumnConstraint.BOC_NONE,
                business_objects.bo_descriptors.AttributeType.ATYPE_STR,
                AttributeAccessLevel.AAL_READ_WRITE,
                {"semantic_role": BOSemanticRole.RAW},
            ),
        )

    def test_104_get_without_instance(self):
        self.assertIsInstance(MockBO.mock_attr, MockAttr)

    def test_105_with_multiple_instances(self):
        mock_bo1 = MockBO("value1")
        mock_bo2 = MockBO("value2")

        self.assertEqual(mock_bo1.mock_attr, "value1")
        self.assertEqual(mock_bo2.mock_attr, "value2")


class MockRel(business_objects.bo_descriptors.BOBaseBase):
    pass


class MockNotRel(business_objects.bo_descriptors.BOBaseBase):
    pass


class MockFlag(business_objects.bo_descriptors.BaseFlag):
    FLAG_1 = auto()
    FLAG_2 = auto()


class MockObj(business_objects.bo_descriptors.BOBaseBase):
    _attributes = {"MockObj": []}
    _data = {}
    int_attr = business_objects.bo_descriptors.BOInt(
        business_objects.bo_descriptors.BOColumnConstraint.BOC_PK_INC
    )
    str_attr = business_objects.bo_descriptors.BOStr(
        business_objects.bo_descriptors.BOColumnConstraint.BOC_NOT_NULL
    )
    dt_attr = business_objects.bo_descriptors.BODatetime(
        business_objects.bo_descriptors.BOColumnConstraint.BOC_DEFAULT_CURR
    )
    d_attr = business_objects.bo_descriptors.BODate()
    dict_attr = business_objects.bo_descriptors.BODict(
        business_objects.bo_descriptors.BOColumnConstraint.BOC_DEFAULT,
        default={"a": 1, "b": 2},
    )
    list_attr = business_objects.bo_descriptors.BOList()
    rel_attr = business_objects.bo_descriptors.BORelation(MockRel)
    rel_self_attr = business_objects.bo_descriptors.BORelation(BOSelf)
    flag_attr = business_objects.bo_descriptors.BOFlag(flag_type=MockFlag)
    decimal_attr = business_objects.bo_descriptors.BODecimal()

    @classmethod
    def add_attribute(
        cls,
        attribute_name,
        data_type,
        constraint_flag,
        attribute_type,
        access_level=AttributeAccessLevel.AAL_READ_WRITE,
        **flag_values,
    ):
        cls._attributes["MockObj"].append(
            (
                attribute_name,
                data_type,
                constraint_flag,
                attribute_type,
                access_level,
                flag_values,
            )
        )


expected_attributes = {
    "MockObj": [
        (
            "int_attr",
            int,
            business_objects.bo_descriptors.BOColumnConstraint.BOC_PK_INC,
            business_objects.bo_descriptors.AttributeType.ATYPE_INT,
            AttributeAccessLevel.AAL_READ_WRITE,
            {"semantic_role": BOSemanticRole.RAW},
        ),
        (
            "str_attr",
            str,
            business_objects.bo_descriptors.BOColumnConstraint.BOC_NOT_NULL,
            business_objects.bo_descriptors.AttributeType.ATYPE_STR,
            AttributeAccessLevel.AAL_READ_WRITE,
            {"semantic_role": BOSemanticRole.RAW},
        ),
        (
            "dt_attr",
            datetime.datetime,
            business_objects.bo_descriptors.BOColumnConstraint.BOC_DEFAULT_CURR,
            business_objects.bo_descriptors.AttributeType.ATYPE_DATETIME,
            AttributeAccessLevel.AAL_READ_WRITE,
            {"semantic_role": BOSemanticRole.RAW},
        ),
        (
            "d_attr",
            datetime.date,
            business_objects.bo_descriptors.BOColumnConstraint.BOC_NONE,
            business_objects.bo_descriptors.AttributeType.ATYPE_DATE,
            AttributeAccessLevel.AAL_READ_WRITE,
            {"semantic_role": BOSemanticRole.RAW},
        ),
        (
            "dict_attr",
            dict,
            business_objects.bo_descriptors.BOColumnConstraint.BOC_DEFAULT,
            business_objects.bo_descriptors.AttributeType.ATYPE_DICT,
            AttributeAccessLevel.AAL_READ_WRITE,
            {"default": {"a": 1, "b": 2}, "semantic_role": BOSemanticRole.RAW},
        ),
        (
            "list_attr",
            list,
            business_objects.bo_descriptors.BOColumnConstraint.BOC_NONE,
            business_objects.bo_descriptors.AttributeType.ATYPE_LIST,
            AttributeAccessLevel.AAL_READ_WRITE,
            {"semantic_role": BOSemanticRole.RAW},
        ),
        (
            "rel_attr",
            business_objects.bo_descriptors.BOBaseBase,
            business_objects.bo_descriptors.BOColumnConstraint.BOC_FK,
            business_objects.bo_descriptors.AttributeType.ATYPE_RELATION,
            AttributeAccessLevel.AAL_READ_WRITE,
            {"semantic_role": BOSemanticRole.RAW, "relation": MockRel},
        ),
        (
            "rel_self_attr",
            business_objects.bo_descriptors.BOBaseBase,
            business_objects.bo_descriptors.BOColumnConstraint.BOC_FK,
            business_objects.bo_descriptors.AttributeType.ATYPE_RELATION,
            AttributeAccessLevel.AAL_READ_WRITE,
            {"relation": MockObj, "semantic_role": BOSemanticRole.RAW},
        ),
        (
            "flag_attr",
            business_objects.bo_descriptors.BaseFlag,
            business_objects.bo_descriptors.BOColumnConstraint.BOC_NONE,
            business_objects.bo_descriptors.AttributeType.ATYPE_FLAG,
            AttributeAccessLevel.AAL_READ_WRITE,
            {"flag_type": MockFlag, "semantic_role": BOSemanticRole.RAW},
        ),
        (
            "decimal_attr",
            Decimal,
            business_objects.bo_descriptors.BOColumnConstraint.BOC_NONE,
            business_objects.bo_descriptors.AttributeType.ATYPE_DECIMAL,
            AttributeAccessLevel.AAL_READ_WRITE,
            {"semantic_role": BOSemanticRole.RAW},
        ),
    ]
}


class Test_200_BOAttributes(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_obj = MockObj()

    def test_201_attributes(self):
        self.maxDiff = None

        self.assertEqual(self.mock_obj._attributes, expected_attributes)
        ix_of_rel_self_attr = 7
        self.assertEqual(
            self.mock_obj._attributes["MockObj"][ix_of_rel_self_attr][0],
            "rel_self_attr",
        )
        self.assertEqual(
            self.mock_obj._attributes["MockObj"][ix_of_rel_self_attr][5]["relation"],
            MockObj,
        )

    def test_202_validate_set_get(self):
        self.mock_obj.int_attr = 11
        self.mock_obj.str_attr = "str"
        self.mock_obj.dt_attr = "2020-02-20 20:20"
        self.mock_obj.d_attr = "2020-02-20"
        self.mock_obj.list_attr = [1, 2, 3]
        self.mock_obj.dict_attr = {"dict": 99}
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
        self.assertEqual(self.mock_obj.flag_attr, MockFlag.FLAG_1)

        mock_init = Mock(name="MockRel.__init__", return_value=None)
        MockRel.__init__ = mock_init
        other_obj = MockRel()
        self.mock_obj.rel_attr = other_obj
        self.assertEqual(self.mock_obj.rel_attr, other_obj)
        mock_init.assert_called_once_with()

        mock_init.reset_mock()
        self.mock_obj.rel_attr = 12
        mock_init.assert_called_once_with(bo_id=12)

        mock_init.reset_mock()
        self.mock_obj.rel_attr = "22"
        mock_init.assert_called_once_with(bo_id=22)

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

        self.mock_obj._data["flag_attr"] = MockFlag.FLAG_1
        self.assertIsInstance(self.mock_obj._data["flag_attr"], Flag)
        self.assertIsInstance(self.mock_obj.flag_attr, MockFlag)
        self.assertEqual(self.mock_obj.flag_attr, MockFlag.FLAG_1)

        self.mock_obj.flag_attr |= MockFlag.FLAG_2
        self.assertEqual(self.mock_obj.flag_attr, MockFlag.FLAG_1 | MockFlag.FLAG_2)
        self.assertIsInstance(self.mock_obj._data["flag_attr"], MockFlag)
        self.assertIsInstance(self.mock_obj.flag_attr, MockFlag)
        self.assertEqual(self.mock_obj.flag_attr, MockFlag(0b11))

    def test_206_BODecimal_data_type(self):

        self.assertEqual(business_objects.bo_descriptors.BODecimal.data_type(), Decimal)

    def test_207_BODecimal_setter(self):
        mock_obj = MockObj()

        # Should raise a ValueError because App.db is not initialized
        with self.assertRaises(
            OperationalError,
            msg="BODecimal setter should raise OperationalError when App.db is not initialized.",
        ):
            mock_obj.decimal_attr = Decimal("10.75")

        # Mock initialized App.db
        with patch("business_objects.bo_descriptors.App", new=Mock()) as mock_app:
            mock_db = Mock()
            mock_app.db = mock_db
            mock_obj.decimal_attr = Decimal("20.75")
            self.assertEqual(mock_obj.decimal_attr, Decimal("20.75"))

            # Patch db.validate decimal to return False to simulate invalid decimal
            with patch.object(mock_db, "validate_decimal", return_value=False):
                with self.assertRaises(
                    ValueError,
                    msg="BODecimal setter should raise ValueError when db.validate_decimal returns False.",
                ):
                    mock_obj.decimal_attr = Decimal("30.75")

            # If super.validate returns False, it should raise ValueError
            with patch.object(MockObj.decimal_attr, "validate", return_value=False):
                with self.assertRaises(
                    ValueError,
                    msg="BODecimal setter should raise ValueError when super().validate returns False.",
                ):
                    mock_obj.decimal_attr = Decimal("40.75")

            # If the value is not a Decimal, it should raise ValueError
            with self.assertRaises(
                ValueError,
                msg="BODecimal setter should raise ValueError when value is not a Decimal.",
            ):
                mock_obj.decimal_attr = "not a decimal"

        with self.assertRaises(ValueError):
            mock_obj.decimal_attr = "not a decimal"

    def test_208_BODecimal_setter_coerces_int_str_float(self):
        mock_obj = MockObj()

        with patch("business_objects.bo_descriptors.App", new=Mock()) as mock_app:
            mock_app.db = Mock()

            mock_obj.decimal_attr = 42
            self.assertEqual(mock_obj.decimal_attr, Decimal(42))

            mock_obj.decimal_attr = "15.50"
            self.assertEqual(mock_obj.decimal_attr, Decimal("15.50"))

            mock_obj.decimal_attr = 3.14
            self.assertEqual(mock_obj.decimal_attr, Decimal(str(3.14)))
