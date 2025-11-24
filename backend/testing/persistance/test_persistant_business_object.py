"""Test suite for Business Objects Base"""

import datetime
from os import name
import unittest
from unittest import mock
from unittest.mock import ANY, DEFAULT, Mock, AsyncMock, patch, call

from business_objects.persistant_business_object import PersistentBusinessObject
from business_objects.bo_descriptors import (
    BOStr,
    BOList,
    BORelation,
    BOColumnConstraint,
    BOBaseBase,
)

MOCK_TAB1 = "mock_table"
MOCK_TAB2 = "mockpersistantbo2s"


class MockPersistantBO1(PersistentBusinessObject):
    _table = MOCK_TAB1


class MockPersistantBO2(PersistentBusinessObject):
    mock_attr1 = BOStr()
    mock_attr2 = BORelation(MockPersistantBO1)
    mock_attr3 = BOList()

    def __init__(
        self,
        bo_id=None,
        mock_attr1="mockk attriubute 1",
        mock_attr2=None,
        mock_attr3=[],
    ) -> None:
        super().__init__(bo_id=bo_id)
        self.mock_attr1 = mock_attr1
        self.mock_attr2 = mock_attr2
        self.mock_attr3 = mock_attr3


class MockAttrDesc:
    def __init__(self, name, data_type, constraint, constraint_values):
        self.name = name
        self.data_type = data_type
        self.constraint = constraint
        self.constraint_values = constraint_values


mock_attr_desc = [
    MockAttrDesc("id", int, BOColumnConstraint.BOC_PK_INC, {}),
    MockAttrDesc(
        "last_updated", datetime.datetime, BOColumnConstraint.BOC_DEFAULT_CURR, {}
    ),
    MockAttrDesc("mock_attr1", str, BOColumnConstraint.BOC_NONE, {}),
    MockAttrDesc(
        "mock_attr2",
        BOBaseBase,
        BOColumnConstraint.BOC_FK,
        {"relation": MockPersistantBO1},
    ),
    MockAttrDesc("mock_attr3", list, BOColumnConstraint.BOC_NONE, {}),
]

mock_bo2_as_dict = {a.name: a.data_type for a in mock_attr_desc}
mock_bo2_constr_vals = {a.name: a.constraint_values for a in mock_attr_desc}


class Test_100_Persistant_Business_Object_classmethods(
    unittest.IsolatedAsyncioTestCase
):

    async def test_101_sql_create_table(self):
        mock_sql = Mock(name="mock_sql")
        mock_sql.create_table = Mock(return_value=mock_sql)
        mock_sql.column = Mock()
        mock_sql.execute = AsyncMock()
        mock_tx = AsyncMock(name="mock_transaction")
        mock_tx.__aenter__ = AsyncMock(return_value=mock_tx)
        mock_tx.__aexit__ = AsyncMock(return_value=False)
        mock_tx.sql = Mock(return_value=mock_sql)
        MockSQLTx = Mock(name="MockSQL", return_value=mock_tx)

        with patch(
            "business_objects.persistant_business_object.SQLTransaction",
            new=MockSQLTx,
        ):
            await MockPersistantBO2.sql_create_table()

        MockSQLTx.assert_called_once_with()
        mock_tx.__aenter__.assert_awaited_once_with()
        mock_tx.__aexit__.assert_awaited_once_with(None, None, None)
        mock_sql.create_table.assert_called_once_with(MOCK_TAB2)
        exp_arglist = [
            (
                call(
                    name=a.name,
                    data_type=a.data_type,
                    constraint=a.constraint,
                    **a.constraint_values,
                )
            )
            for a in mock_attr_desc
        ]
        self.assertEqual(mock_sql.column.call_args_list, exp_arglist)
        mock_sql.execute.assert_awaited_once_with()

    async def test_102_count_rows(self):
        RESULT = [1, 99]
        FETCH_RESULT = [{"id": i} for i in RESULT]
        mock_cursor = Mock(name="mock_cursor")
        mock_cursor.fetchall = AsyncMock(return_value=FETCH_RESULT)
        mock_sql = Mock(name="mock_sql")
        mock_sql.execute = AsyncMock(return_value=mock_cursor)
        mock_sql.select = Mock(return_value=mock_sql)
        mock_sql.from_ = Mock(return_value=mock_sql)
        mock_sql.where = Mock(return_value=mock_sql)

        MockSQL = Mock(name="MockSQL", return_value=mock_sql)
        mock_sql.__aenter__ = AsyncMock(return_value=mock_sql)
        mock_sql.__aexit__ = AsyncMock(return_value=None)

        with (
            patch("business_objects.persistant_business_object.SQL", new=MockSQL),
            patch("business_objects.persistant_business_object.Filter") as MockFilter,
        ):
            mock_conditions = "{mock conditions}"

            result = await MockPersistantBO2.get_matching_ids(mock_conditions)
        MockSQL.assert_called_once_with()
        mock_sql.__aenter__.assert_awaited_once_with()
        mock_sql.__aexit__.assert_awaited_once_with(None, None, None)
        mock_sql.select.assert_called_once_with(["id"])
        mock_sql.from_.assert_called_once_with(MOCK_TAB2)
        mock_sql.where.assert_called_once_with(MockFilter())
        mock_sql.execute.assert_awaited_once_with()
        mock_cursor.fetchall.assert_awaited_once_with()
        self.assertEqual(result, RESULT)


class Test_200_BOBase_access(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_bo = MockPersistantBO2()
        self.mock_sql = Mock(name="mock_sql")
        self.mock_sql.__aenter__ = AsyncMock(return_value=self.mock_sql)
        self.mock_sql.__aexit__ = AsyncMock(return_value=None)
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
        self.mock_sql.commit = AsyncMock()
        self.mock_sql.rollback = AsyncMock()
        self.mock_sql.select = Mock(return_value=self.mock_sql)
        self.mock_sql.from_ = Mock(return_value=self.mock_sql)
        self.mock_sql.where = Mock(return_value=self.mock_sql)
        self.mock_sql.insert = Mock(return_value=self.mock_sql)
        self.mock_sql.update = Mock(return_value=self.mock_sql)
        self.mock_sql.rows = Mock(return_value=self.mock_sql)
        self.mock_sql.returning = Mock(return_value=self.mock_sql)
        self.mock_sql.assignment = Mock(return_value=self.mock_sql)
        self.MockSQL = Mock(name="MockSQL", return_value=self.mock_sql)

        self.mock_tx = AsyncMock(name="mock_transaction")
        self.mock_tx.__aenter__ = AsyncMock(return_value=self.mock_tx)
        self.mock_tx.__aexit__ = AsyncMock(return_value=False)
        self.mock_tx.sql = Mock(return_value=self.mock_sql)
        self.MockSQLTx = Mock(name="MockSQL", return_value=self.mock_tx)

    async def test_201_fetch_none(self):
        with patch("business_objects.persistant_business_object.SQL", new=self.MockSQL):
            self.mock_bo.id = None
            result = await self.mock_bo.fetch()
            self.MockSQL.assert_not_called()
            self.assertIs(result, self.mock_bo)

    async def _202_fetch(self, patch_exp, exp_params, newest=DEFAULT):
        with (
            patch("business_objects.persistant_business_object.SQL", new=self.MockSQL),
            patch(
                "business_objects.persistant_business_object." + patch_exp
            ) as MockExp,
        ):
            if newest == DEFAULT:
                result = await self.mock_bo.fetch()
            else:
                result = await self.mock_bo.fetch(newest=newest)

            self.MockSQL.assert_called_once_with()
            self.mock_sql.__aenter__.assert_awaited_once_with()
            self.mock_sql.__aexit__.assert_awaited_once_with(None, None, None)
            self.mock_sql.select.assert_called_once_with([], True)
            self.mock_sql.from_.assert_called_once_with(MOCK_TAB2)
            self.mock_sql.where.assert_called_once_with(MockExp())
            self.mock_sql.execute.assert_awaited_once_with()
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
                    self.assertEqual(
                        result._data[attr], self.FETCH_RESULT[attr], "attribute result"
                    )
                    if isinstance(result._data[attr], (dict, list)):
                        self.assertIsNot(
                            result._data[attr],
                            self.FETCH_RESULT[attr],
                            "structured attribute result",
                        )
            self.assertIs(result._db_data, self.FETCH_RESULT, "_db_data")

    async def test_202_fetch_no_param(self):
        REQ_ID = 19
        self.mock_bo.id = REQ_ID
        await self._202_fetch("Eq", ("id", REQ_ID))

    async def test_202_fetch_newest(self):
        await self._202_fetch(
            "SQLExpression", (f"id = (SELECT MAX(id) FROM {MOCK_TAB2})",), newest=True
        )

    async def test_203_store_insert(self):
        self.mock_bo._insert_self = AsyncMock(name="_insert_self")
        self.mock_bo._update_self = AsyncMock(name="_update_self")

        await self.mock_bo.store()

        self.mock_bo._insert_self.assert_awaited_once_with()
        self.mock_bo._update_self.assert_not_awaited()

    async def test_203_store_update_self(self):
        self.mock_bo._insert_self = AsyncMock(name="_insert_self")
        self.mock_bo._update_self = AsyncMock(name="_update_self")
        self.mock_bo.id = 77

        await self.mock_bo.store()

        self.mock_bo._insert_self.assert_not_awaited()
        self.mock_bo._update_self.assert_awaited_once_with()

    async def _204_insert_self(
        self, mock_attr1="micki mock", mock_attr3=[], **mock_attrs
    ):
        mock_attrs |= {"mock_attr1": mock_attr1, "mock_attr3": mock_attr3}
        with patch(
            "business_objects.persistant_business_object.SQLTransaction",
            new=self.MockSQLTx,
        ):
            mock_bo = MockPersistantBO2(**mock_attrs)

            await mock_bo._insert_self()

            self.MockSQLTx.assert_called_once_with()
            self.mock_tx.__aenter__.assert_awaited_once_with()
            self.mock_tx.__aexit__.assert_awaited_once_with(None, None, None)
            self.mock_sql.insert.assert_called_once_with(MOCK_TAB2)
            self.mock_sql.rows.assert_called_once_with(
                [[(a, mock_attrs[a])] for a in mock_bo2_as_dict if a in mock_attrs]
            )
            self.mock_sql.returning.assert_called_once_with("id")
            self.mock_sql.execute.assert_awaited_once_with()
            self.mock_cursor.fetchone.assert_awaited_once_with()
            self.assertEqual(mock_bo.id, self.FETCH_RESULT["id"])

    async def test_204a_insert_self(self):
        with self.assertRaises(AssertionError):
            self.mock_bo.id = 77
            await self.mock_bo._insert_self()

    async def test_204b_insert_self(self):
        await self._204_insert_self()

    async def test_204c_insert_self(self):
        mock_bo1 = MockPersistantBO1()
        await self._204_insert_self(mock_attr2=mock_bo1)

    async def _205_update_self(self, exception=False):
        mock_convert_from_db = Mock(
            name="convert_from_db",
            side_effect=[
                self.mock_bo._db_data.get(a) for a in mock_bo2_as_dict if a != "id"
            ],
        )
        with (
            patch(
                "business_objects.persistant_business_object.SQLTransaction",
                new=self.MockSQLTx,
            ),
            patch("business_objects.persistant_business_object.Value") as MockValue,
            patch("business_objects.persistant_business_object.Eq") as MockEq,
            patch(
                "business_objects.persistant_business_object.PersistentBusinessObject.convert_from_db",
                new=mock_convert_from_db,
            ),
        ):
            convert_args = [
                call(
                    self.mock_bo._db_data.get(a),
                    mock_bo2_as_dict[a],
                    mock_bo2_constr_vals[a],
                )
                for a in mock_bo2_as_dict
                if a != "id"
            ]
            self.mock_bo.attributes_as_dict = Mock(
                name="attributes_as_dict", return_value=mock_bo2_as_dict
            )
            self.mock_bo.attribute_descriptions = Mock(
                name="attribute_descriptions", return_value=mock_attr_desc
            )
            new_vals = [
                (a, self.mock_bo._data[a])
                for a in self.mock_bo._data
                if a != "id" and self.mock_bo._data[a] != self.mock_bo._db_data.get(a)
            ]
            # Mock_DB_Val = Mock(
            #     name="Mock_DB_Val",
            #     side_effect=[v[1] for v in new_vals],
            # )
            self.mock_bo.fetch = AsyncMock(name="fetch")
            # Mock_DB_Eq = Mock(name="Mock_DB_Eq", return_value="mock eq")
            # self.mock_sql.get_sql_class = Mock(side_effect=[Mock_DB_Val, Mock_DB_Eq])
            id = self.mock_bo.id

            if exception:
                self.mock_sql.execute.side_effect = [Exception]
                with self.assertRaises(Exception):
                    await self.mock_bo._update_self()
            else:
                await self.mock_bo._update_self()

            self.MockSQLTx.assert_called_once_with()
            self.mock_tx.__aenter__.assert_awaited_once_with()
            self.mock_sql.update.assert_called_once_with(MOCK_TAB2)
            MockEq.assert_called_once_with("id", id)
            self.mock_sql.where.assert_called_once_with(MockEq())
            print(f"{mock_attr_desc[0]=}")
            self.mock_bo.attribute_descriptions.assert_called_once_with()
            self.assertEqual(
                PersistentBusinessObject.convert_from_db.call_count,
                len(convert_args),
                "attributes converted",
            )
            self.assertEqual(
                PersistentBusinessObject.convert_from_db.call_args_list, convert_args
            )
            self.assertEqual(self.mock_sql.assignment.call_count, len(new_vals))
            self.assertEqual(MockValue.call_count, len(new_vals))
            for v in new_vals:
                MockValue.assert_any_call(v[0], v[1])
            self.assertEqual(
                self.mock_sql.assignment.call_args_list,
                [call(v[0], MockValue()) for v in new_vals],
            )
            self.mock_sql.execute.assert_awaited_once_with()
            if exception:
                self.mock_tx.__aexit__.assert_awaited_once_with(Exception, ANY, ANY)
            else:
                self.mock_tx.__aexit__.assert_awaited_once_with(None, None, None)
            self.mock_bo.fetch.assert_awaited_once()

    async def test_205a_update_self(self):
        with self.assertRaises(AssertionError) as exp:
            await self.mock_bo._update_self()

    async def test_205b_update_self(self):
        self.mock_bo = MockPersistantBO2(bo_id=55)
        await self._205_update_self()

    async def test_205c_update_self(self):
        self.mock_bo = MockPersistantBO2(bo_id=55, mock_attr2=MockPersistantBO1())
        await self._205_update_self()

    async def test_205d_update_self(self):
        self.mock_bo = MockPersistantBO2(
            bo_id=55, mock_attr2=MockPersistantBO1(), mock_attr3=[1, 2, 3]
        )
        await self._205_update_self()

    async def test_205e_update_self_exception(self):
        self.mock_bo = MockPersistantBO2(bo_id=55)
        await self._205_update_self(exception=True)


class Test_300_BOBase_instancemethods(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_bo = MockPersistantBO2()

    def test_01_convert_from_db_none(self):
        self.assertIsNone(MockPersistantBO2.convert_from_db(None, int, {}))

    def test_01_convert_from_db_date(self):
        mock_tz_cet = datetime.timezone(datetime.timedelta(hours=+1), name="CET")
        mock_tz_est = datetime.timezone(datetime.timedelta(hours=-5), name="EST")
        mock_dt_utc = datetime.datetime(2031, 4, 25, 13, 45, tzinfo=datetime.UTC)
        mock_dt_cet = mock_dt_utc.astimezone(mock_tz_cet)
        mock_dt_est = mock_dt_utc.astimezone(mock_tz_est)
        mock_dt_none = datetime.datetime(2031, 4, 25, 13, 45)
        mock_date = datetime.date(2031, 4, 25)

        res = MockPersistantBO2.convert_from_db(
            value=mock_dt_cet.isoformat(), typ=datetime.datetime, subtyp={}
        )
        self.assertEqual(mock_dt_cet, res)
        self.assertEqual(mock_dt_cet.tzinfo, res.tzinfo)

        res = MockPersistantBO2.convert_from_db(
            value=mock_dt_est.isoformat(), typ=datetime.datetime, subtyp={}
        )
        self.assertEqual(mock_dt_est, res)
        self.assertEqual(mock_dt_est.tzinfo, res.tzinfo)

        res = MockPersistantBO2.convert_from_db(
            value=mock_dt_utc.isoformat(), typ=datetime.datetime, subtyp={}
        )
        self.assertEqual(mock_dt_utc, res)
        self.assertEqual(mock_dt_utc.astimezone().tzinfo, res.tzinfo)

        res = MockPersistantBO2.convert_from_db(
            value=mock_dt_none.isoformat(), typ=datetime.datetime, subtyp={}
        )
        self.assertEqual(mock_dt_none.replace(tzinfo=datetime.UTC).astimezone(), res)
        self.assertEqual(mock_dt_none.astimezone().tzinfo, res.tzinfo)

        res = MockPersistantBO2.convert_from_db(
            value=mock_date.isoformat(), typ=datetime.date, subtyp={}
        )
        self.assertEqual(mock_date, res)
