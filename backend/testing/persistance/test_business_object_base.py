"""Test suite for Business Objects Base"""

import datetime
import unittest
from unittest.mock import DEFAULT, Mock, AsyncMock, patch, call

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import (
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
        self, id=None, mock_attr1="mockk attriubute 1", mock_attr2=None, mock_attr3=[]
    ) -> None:
        super().__init__(id=id)
        self.mock_attr1 = mock_attr1
        self.mock_attr2 = mock_attr2
        self.mock_attr3 = mock_attr3


mock_attr_desc = [
    ("id", int, BOColumnFlag.BOC_PK_INC, {}),
    ("last_updated", datetime.datetime, BOColumnFlag.BOC_DEFAULT_CURR, {}),
    ("mock_attr1", str, BOColumnFlag.BOC_NONE, {}),
    ("mock_attr2", BOBaseBase, BOColumnFlag.BOC_FK, {"relation": MockBO1}),
    ("mock_attr3", list, BOColumnFlag.BOC_NONE, {}),
]

mock_bo2_as_dict = {a[0]: a[1] for a in mock_attr_desc}


class Test_100_BOBase_classmethods(unittest.IsolatedAsyncioTestCase):

    def test_101_register_persistant_class(self):
        self.assertNotIn("MockBO2", BOBase._business_objects)
        MockBO2.register_persistant_class()
        self.assertIn("MockBO2", BOBase._business_objects)

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


class Test_200_BOBase_instancemethods(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_bo = MockBO2()

    def test_201_convert_from_db_none(self):
        self.assertIsNone(self.mock_bo.convert_from_db(None, int))

    def test_201_convert_from_db_date(self):
        mock_tz_cet = datetime.timezone(datetime.timedelta(hours=+1), name="CET")
        mock_tz_est = datetime.timezone(datetime.timedelta(hours=-5), name="EST")
        mock_dt_utc = datetime.datetime(2031, 4, 25, 13, 45, tzinfo=datetime.UTC)
        mock_dt_cet = mock_dt_utc.astimezone(mock_tz_cet)
        mock_dt_est = mock_dt_utc.astimezone(mock_tz_est)
        mock_dt_none = datetime.datetime(2031, 4, 25, 13, 45)
        mock_date = datetime.date(2031, 4, 25)

        res = self.mock_bo.convert_from_db(
            value=mock_dt_cet.isoformat(), typ=datetime.datetime
        )
        self.assertEqual(mock_dt_cet, res)
        self.assertEqual(mock_dt_cet.tzinfo, res.tzinfo)

        res = self.mock_bo.convert_from_db(
            value=mock_dt_est.isoformat(), typ=datetime.datetime
        )
        self.assertEqual(mock_dt_est, res)
        self.assertEqual(mock_dt_est.tzinfo, res.tzinfo)

        res = self.mock_bo.convert_from_db(
            value=mock_dt_utc.isoformat(), typ=datetime.datetime
        )
        self.assertEqual(mock_dt_utc, res)
        self.assertEqual(mock_dt_utc.astimezone().tzinfo, res.tzinfo)

        res = self.mock_bo.convert_from_db(
            value=mock_dt_none.isoformat(), typ=datetime.datetime
        )
        self.assertEqual(mock_dt_none.replace(tzinfo=datetime.UTC).astimezone(), res)
        self.assertEqual(mock_dt_none.astimezone().tzinfo, res.tzinfo)

        res = self.mock_bo.convert_from_db(
            value=mock_date.isoformat(), typ=datetime.date
        )
        self.assertEqual(mock_date, res)
