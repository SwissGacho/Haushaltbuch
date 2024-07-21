"""Test suite for Business Objects Base"""

import datetime
from os import name
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
    mock_attr2 = BORelation(flag_values={"relation": MockBO1})
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
        self.assertEqual(MockBO2.attribute_descriptions(), mock_attr_desc)

    async def test_106_sql_create_table(self):
        mock_sql = Mock(name="mock_sql")
        MockSQL = Mock(name="MockSQL", return_value=mock_sql)
        mock_sql.create_table = Mock(return_value=mock_sql)
        mock_sql.column = Mock()
        mock_sql.execute = AsyncMock()
        with patch("persistance.business_object_base.SQL", new=MockSQL):
            await MockBO2.sql_create_table()
            MockSQL.assert_called_once_with()
            mock_sql.create_table.assert_called_once_with(MOCK_TAB2)
            exp_arglist = [(call(a[0], a[1], a[2], **a[3])) for a in mock_attr_desc]
            self.assertEqual(mock_sql.column.call_args_list, exp_arglist)
            mock_sql.execute.assert_awaited_once_with(close=0)

    async def test_107_count_rows(self):
        RESULT = [1, 99]
        FETCH_RESULT = [{"id": i} for i in RESULT]
        mock_cursor = Mock(name="mock_cursor")
        mock_cursor.fetchall = AsyncMock(return_value=FETCH_RESULT)
        MockDBFilter = Mock(name="MockDBFilter")
        mock_sql = Mock(name="mock_sql")
        mock_sql.execute = AsyncMock(return_value=mock_cursor)
        mock_sql.select = Mock(return_value=mock_sql)
        mock_sql.from_ = Mock(return_value=mock_sql)
        mock_sql.get_sql_class = Mock(return_value=MockDBFilter)
        mock_sql.where = Mock(return_value=mock_sql)

        MockSQL = Mock(name="MockSQL", return_value=mock_sql)
        with (
            patch("persistance.business_object_base.SQL", new=MockSQL),
            patch("persistance.business_object_base.Filter") as MockFilter,
        ):
            mock_conditions = "{mock conditions}"

            result = await MockBO2.get_matching_ids(mock_conditions)
            MockSQL.assert_called_once_with()
            mock_sql.select.assert_called_once_with(["id"])
            mock_sql.from_.assert_called_once_with(MOCK_TAB2)
            mock_sql.get_sql_class.assert_called_once_with(MockFilter)
            MockDBFilter.assert_called_once_with(mock_conditions)
            mock_sql.where.assert_called_once_with(MockDBFilter())
            mock_sql.execute.assert_awaited_once_with(close=1)
            mock_cursor.fetchall.assert_awaited_once_with()
            self.assertEqual(result, RESULT)


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


class Test_300_BOBase_access(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_bo = MockBO2()
        self.mock_sql = Mock(name="mock_sql")
        self.mock_cursor = Mock(name="mock_cursor")
        self.FETCH_RESULT = {
            "id": 33,
            "last_updated": "1990-01-31 17:38",
            "mock_attr1": "mick mack",
            "mock_attr2": None,
            "mock_attr3": ["a", "b", "c"],
        }
        self.mock_cursor.fetchone = AsyncMock(return_value=self.FETCH_RESULT)
        self.mock_sql.execute = AsyncMock(return_value=self.mock_cursor)
        self.mock_sql.select = Mock(return_value=self.mock_sql)
        self.mock_sql.from_ = Mock(return_value=self.mock_sql)
        self.Mock_DB_SQLExpr = Mock(
            name="Mock_DB_SQLExpr", return_value="Mock_DB_SQLExpr"
        )
        self.mock_sql.get_sql_class = Mock(return_value=self.Mock_DB_SQLExpr)
        self.mock_sql.where = Mock(return_value=self.mock_sql)
        self.mock_sql.insert = Mock(return_value=self.mock_sql)
        self.mock_sql.update = Mock(return_value=self.mock_sql)
        self.mock_sql.rows = Mock(return_value=self.mock_sql)
        self.mock_sql.returning = Mock(return_value=self.mock_sql)
        self.mock_sql.assignment = Mock(return_value=self.mock_sql)

        self.MockSQL = Mock(name="MockSQL", return_value=self.mock_sql)

    async def test_301_fetch_none(self):
        with (patch("persistance.business_object_base.SQL", new=self.MockSQL),):
            self.mock_bo.id = None
            result = await self.mock_bo.fetch()
            self.MockSQL.assert_not_called()
            self.assertIs(result, self.mock_bo)

    async def _302_fetch(self, patch_exp, exp_params, newest=DEFAULT):
        with (
            patch("persistance.business_object_base.SQL", new=self.MockSQL),
            patch("persistance.business_object_base." + patch_exp) as MockExp,
        ):
            if newest == DEFAULT:
                result = await self.mock_bo.fetch()
            else:
                result = await self.mock_bo.fetch(newest=newest)

            self.MockSQL.assert_called_once_with()
            self.mock_sql.select.assert_called_once_with([], True)
            self.mock_sql.from_.assert_called_once_with(MOCK_TAB2)
            self.mock_sql.get_sql_class.assert_called_once_with(MockExp)
            self.Mock_DB_SQLExpr.assert_called_once_with(*exp_params)
            self.mock_sql.where.assert_called_once_with("Mock_DB_SQLExpr")
            self.mock_sql.execute.assert_awaited_once_with(close=1)
            self.mock_cursor.fetchone.assert_awaited_once_with()
            self.assertIs(result, self.mock_bo)
            for attr in mock_bo2_as_dict:
                if isinstance(result._data[attr], datetime.datetime):
                    self.assertEqual(
                        result._data[attr],
                        datetime.datetime.fromisoformat(self.FETCH_RESULT[attr])
                        .replace(tzinfo=datetime.UTC)
                        .astimezone(),
                    )
                else:
                    self.assertIs(
                        result._data[attr], self.FETCH_RESULT[attr], "attribute result"
                    )
            self.assertEqual(result._db_data, self.FETCH_RESULT, "_db_data")

    async def test_302_fetch_no_param(self):
        REQ_ID = 19
        self.mock_bo.id = REQ_ID
        await self._302_fetch("Eq", ("id", REQ_ID))

    async def test_302_fetch_newest(self):
        await self._302_fetch(
            "SQLExpression", (f"id = (SELECT MAX(id) FROM {MOCK_TAB2})",), newest=True
        )

    async def test_303_store_insert(self):
        self.mock_bo._insert_self = AsyncMock(name="_insert_self")
        self.mock_bo._update_self = AsyncMock(name="_update_self")

        await self.mock_bo.store()

        self.mock_bo._insert_self.assert_awaited_once_with()
        self.mock_bo._update_self.assert_not_awaited()

    async def test_303_store_update_self(self):
        self.mock_bo._insert_self = AsyncMock(name="_insert_self")
        self.mock_bo._update_self = AsyncMock(name="_update_self")
        self.mock_bo.id = 77

        await self.mock_bo.store()

        self.mock_bo._insert_self.assert_not_awaited()
        self.mock_bo._update_self.assert_awaited_once_with()

    async def _304_insert_self(
        self, mock_attr1="micki mock", mock_attr3=[], **mock_attrs
    ):
        mock_attrs |= {"mock_attr1": mock_attr1, "mock_attr3": mock_attr3}
        with (patch("persistance.business_object_base.SQL", new=self.MockSQL),):
            mock_bo = MockBO2(**mock_attrs)

            await mock_bo._insert_self()

            self.MockSQL.assert_called_once_with()
            self.mock_sql.insert.assert_called_once_with(MOCK_TAB2)
            self.mock_sql.rows.assert_called_once_with(
                [(a, mock_attrs[a]) for a in mock_bo2_as_dict if a in mock_attrs]
            )
            self.mock_sql.returning.assert_called_once_with("id")
            self.mock_sql.execute.assert_awaited_once_with(close=1, commit=True)
            self.mock_cursor.fetchone.assert_awaited_once_with()
            self.assertEqual(mock_bo.id, self.FETCH_RESULT["id"])

    async def test_304a_insert_self(self):
        with self.assertRaises(AssertionError):
            self.mock_bo.id = 77
            await self.mock_bo._insert_self()

    async def test_304b_insert_self(self):
        await self._304_insert_self()

    async def test_304c_insert_self(self):
        mock_bo2 = MockBO2()
        await self._304_insert_self(mock_attr2=mock_bo2)

    async def _305_update_self(self, exception=False):
        with (
            patch("persistance.business_object_base.SQL", new=self.MockSQL),
            patch("persistance.business_object_base.Value") as MockValue,
            patch("persistance.business_object_base.Eq") as MockEq,
        ):
            convert_args = [
                call(
                    self.mock_bo._db_data.get(a),
                    mock_bo2_as_dict[a],
                )
                for a in mock_bo2_as_dict
                if a != "id"
            ]
            self.mock_bo.convert_from_db = Mock(
                name="convert_from_db",
                side_effect=[
                    self.mock_bo._db_data.get(a) for a in mock_bo2_as_dict if a != "id"
                ],
            )
            self.mock_bo.attributes_as_dict = Mock(
                name="attributes_as_dict", return_value=mock_bo2_as_dict
            )
            new_vals = [
                (a, self.mock_bo._data[a])
                for a in self.mock_bo._data
                if a != "id" and self.mock_bo._data[a] != self.mock_bo._db_data.get(a)
            ]
            Mock_DB_Val = Mock(
                name="Mock_DB_Val",
                side_effect=[v[1] for v in new_vals],
            )
            self.mock_bo.fetch = AsyncMock(name="fetch")
            Mock_DB_Eq = Mock(name="Mock_DB_Eq", return_value="mock eq")
            self.mock_sql.get_sql_class = Mock(side_effect=[Mock_DB_Val, Mock_DB_Eq])
            id = self.mock_bo.id

            if exception:
                self.mock_sql.execute.side_effect = [Exception]
                with self.assertRaises(Exception):
                    await self.mock_bo._update_self()
            else:
                await self.mock_bo._update_self()

            self.MockSQL.assert_called_once_with()
            self.assertEqual(
                self.mock_sql.get_sql_class.call_count, 2, "DB classfactory calls"
            )
            self.assertEqual(
                self.mock_sql.get_sql_class.call_args_list,
                [call(MockValue), call(MockEq)],
                "DB classfactory calls",
            )
            self.mock_sql.update.assert_called_once_with(MOCK_TAB2)
            Mock_DB_Eq.assert_called_once_with("id", id)
            self.mock_sql.where.assert_called_once_with("mock eq")
            self.assertEqual(
                self.mock_bo.attributes_as_dict.call_count,
                len(convert_args),
                "attributes as dict",
            )
            self.assertEqual(
                self.mock_bo.attributes_as_dict.call_args_list,
                [call() for a in mock_bo2_as_dict if a != "id"],
                "attributes as dict",
            )
            self.assertEqual(
                self.mock_bo.convert_from_db.call_count,
                len(convert_args),
                "attributes converted",
            )
            self.assertEqual(self.mock_bo.convert_from_db.call_args_list, convert_args)
            self.assertEqual(self.mock_sql.assignment.call_count, len(new_vals))
            self.assertEqual(
                self.mock_sql.assignment.call_args_list,
                [call(v[0], v[1]) for v in new_vals],
            )
            self.mock_sql.execute.assert_awaited_once_with(close=0, commit=True)
            self.mock_bo.fetch.assert_awaited_once()

    async def test_305a_update_self(self):
        with self.assertRaises(AssertionError) as exp:
            await self.mock_bo._update_self()

    async def test_305b_update_self(self):
        self.mock_bo = MockBO2(id=55)
        await self._305_update_self()

    async def test_305c_update_self(self):
        self.mock_bo = MockBO2(id=55, mock_attr2=MockBO2())
        await self._305_update_self()

    async def test_305d_update_self(self):
        self.mock_bo = MockBO2(id=55, mock_attr2=MockBO2(), mock_attr3=[1, 2, 3])
        await self._305_update_self()

    async def test_305e_update_self_exception(self):
        self.mock_bo = MockBO2(id=55)
        await self._305_update_self(exception=True)
