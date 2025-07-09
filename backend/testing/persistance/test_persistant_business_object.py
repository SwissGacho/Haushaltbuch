"""Test suite for Business Objects Base"""

import datetime
import unittest
from unittest.mock import DEFAULT, Mock, AsyncMock, patch, call

from persistance.persistant_business_object import PersistentBusinessObject
from persistance.bo_descriptors import (
    BOStr,
    BOList,
    BORelation,
    BOColumnFlag,
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
    ("mock_attr2", BOBaseBase, BOColumnFlag.BOC_FK, {"relation": MockPersistantBO1}),
    ("mock_attr3", list, BOColumnFlag.BOC_NONE, {}),
]

mock_bo2_as_dict = {a[0]: a[1] for a in mock_attr_desc}


class Test_100_Persistant_Business_Object_classmethods(
    unittest.IsolatedAsyncioTestCase
):

    async def test_101_sql_create_table(self):
        mock_sql = Mock(name="mock_sql")
        MockSQL = Mock(name="MockSQL", return_value=mock_sql)
        mock_sql.create_table = Mock(return_value=mock_sql)
        mock_sql.column = Mock()
        mock_sql.execute = AsyncMock()
        with patch("persistance.persistant_business_object.SQL", new=MockSQL):
            await MockPersistantBO2.sql_create_table()
            MockSQL.assert_called_once_with()
            mock_sql.create_table.assert_called_once_with(MOCK_TAB2)
            exp_arglist = [(call(a[0], a[1], a[2], **a[3])) for a in mock_attr_desc]
            print(f"exp_arglist: {exp_arglist}")
            print(f"mock_sql.column.call_args_list: {mock_sql.column.call_args_list}")
            self.assertEqual(mock_sql.column.call_args_list, exp_arglist)
            mock_sql.execute.assert_awaited_once_with(close=0)

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
        with (
            patch("persistance.persistant_business_object.SQL", new=MockSQL),
            patch("persistance.persistant_business_object.Filter") as MockFilter,
        ):
            mock_conditions = "{mock conditions}"

            result = await MockPersistantBO2.get_matching_ids(mock_conditions)
            MockSQL.assert_called_once_with()
            mock_sql.select.assert_called_once_with(["id"])
            mock_sql.from_.assert_called_once_with(MOCK_TAB2)
            mock_sql.where.assert_called_once_with(MockFilter())
            mock_sql.execute.assert_awaited_once_with(close=1)
            mock_cursor.fetchall.assert_awaited_once_with()
            self.assertEqual(result, RESULT)


class Test_200_Persistant_Business_Object_access(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_bo = MockPersistantBO2()
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
        self.mock_sql.where = Mock(return_value=self.mock_sql)
        self.mock_sql.insert = Mock(return_value=self.mock_sql)
        self.mock_sql.update = Mock(return_value=self.mock_sql)
        self.mock_sql.rows = Mock(return_value=self.mock_sql)
        self.mock_sql.returning = Mock(return_value=self.mock_sql)
        self.mock_sql.assignment = Mock(return_value=self.mock_sql)

        self.MockSQL = Mock(name="MockSQL", return_value=self.mock_sql)

    async def test_201_fetch_none(self):
        with (patch("persistance.persistant_business_object.SQL", new=self.MockSQL),):
            self.mock_bo.id = None
            result = await self.mock_bo.fetch()
            self.MockSQL.assert_not_called()
            self.assertIs(result, self.mock_bo)

    async def _302_fetch(self, patch_exp, exp_params, newest=DEFAULT):
        with (
            patch("persistance.persistant_business_object.SQL", new=self.MockSQL),
            patch("persistance.persistant_business_object." + patch_exp) as MockExp,
        ):
            if newest == DEFAULT:
                result = await self.mock_bo.fetch()
            else:
                result = await self.mock_bo.fetch(newest=newest)

            self.MockSQL.assert_called_once_with()
            self.mock_sql.select.assert_called_once_with([], True)
            self.mock_sql.from_.assert_called_once_with(MOCK_TAB2)
            self.mock_sql.where.assert_called_once_with(MockExp())
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
        await self._302_fetch("Eq", ("id", REQ_ID))

    async def test_202_fetch_newest(self):
        await self._302_fetch(
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

    async def _304_insert_self(
        self, mock_attr1="micki mock", mock_attr3=[], **mock_attrs
    ):
        mock_attrs |= {"mock_attr1": mock_attr1, "mock_attr3": mock_attr3}
        with (patch("persistance.persistant_business_object.SQL", new=self.MockSQL),):
            mock_bo = MockPersistantBO2(**mock_attrs)

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

    async def test_204a_insert_self(self):
        with self.assertRaises(AssertionError):
            self.mock_bo.id = 77
            await self.mock_bo._insert_self()

    async def test_204b_insert_self(self):
        await self._304_insert_self()

    async def test_204c_insert_self(self):
        mock_bo1 = MockPersistantBO1()
        await self._304_insert_self(mock_attr2=mock_bo1)

    async def _305_update_self(self, exception=False):
        with (
            patch("persistance.persistant_business_object.SQL", new=self.MockSQL),
            patch("persistance.persistant_business_object.Value") as MockValue,
            patch("persistance.persistant_business_object.Eq") as MockEq,
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

            self.MockSQL.assert_called_once_with()
            self.mock_sql.update.assert_called_once_with(MOCK_TAB2)
            MockEq.assert_called_once_with("id", id)
            self.mock_sql.where.assert_called_once_with(MockEq())
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
            self.assertEqual(MockValue.call_count, len(new_vals))
            for v in new_vals:
                MockValue.assert_any_call(v[0], v[1])
            self.assertEqual(
                self.mock_sql.assignment.call_args_list,
                [call(v[0], MockValue()) for v in new_vals],
            )
            self.mock_sql.execute.assert_awaited_once_with(close=0, commit=True)
            self.mock_bo.fetch.assert_awaited_once()

    async def test_205a_update_self(self):
        with self.assertRaises(AssertionError) as exp:
            await self.mock_bo._update_self()

    async def test_205b_update_self(self):
        self.mock_bo = MockPersistantBO2(id=55)
        await self._305_update_self()

    async def test_205c_update_self(self):
        self.mock_bo = MockPersistantBO2(id=55, mock_attr2=MockPersistantBO1())
        await self._305_update_self()

    async def test_205d_update_self(self):
        self.mock_bo = MockPersistantBO2(
            id=55, mock_attr2=MockPersistantBO1(), mock_attr3=[1, 2, 3]
        )
        await self._305_update_self()

    async def test_205e_update_self_exception(self):
        self.mock_bo = MockPersistantBO2(id=55)
        await self._305_update_self(exception=True)
