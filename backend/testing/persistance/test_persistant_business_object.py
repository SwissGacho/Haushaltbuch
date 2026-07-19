"""Test suite for Business Objects Base"""

import datetime
import json
import unittest
from unittest.mock import ANY, DEFAULT, Mock, AsyncMock, patch, call

from business_objects.bo_semantic_role import BOSemanticRole
from business_objects.persistent_business_object import (
    PersistentBusinessObject,
    Specialized,
)
from business_objects.bo_descriptors import (
    BOStr,
    BOList,
    BORelation,
    BOColumnConstraint,
    BOBaseBase,
)

MOCK_TAB1 = "mock_table"
MOCK_TAB2 = "mockpersistentbo2s"


class MockPersistentBO1(PersistentBusinessObject):
    _table = MOCK_TAB1


class MockPersistentBO2(PersistentBusinessObject):
    mock_attr1 = BOStr()
    mock_attr2 = BORelation(MockPersistentBO1)
    mock_attr3 = BOList()

    def __init__(
        self,
        bo_name=None,
        bo_id=None,
        last_updated=None,
        mock_attr1="mockk attriubute 1",
        mock_attr2=None,
        mock_attr3=[],
    ) -> None:
        super().__init__(bo_id=bo_id, last_updated=last_updated)
        self.bo_name = bo_name
        self.mock_attr1 = mock_attr1
        self.mock_attr2 = mock_attr2
        self.mock_attr3 = mock_attr3

    def __eq__(self, other: PersistentBusinessObject) -> bool:
        """This is only for testing purposes and assumes that the _data dict contains all relevant attributes of the BO"""
        return {k: v for k, v in self._data.items()} == {
            k: v for k, v in other._data.items()
        }


class MockAttrDesc:
    def __init__(self, name, data_type, constraint, constraint_values):
        self.name = name
        self.data_type = data_type
        self.constraint = constraint
        self.constraint_values = constraint_values


mock_attr_desc = [
    MockAttrDesc(
        "bo_name",
        str,
        BOColumnConstraint.BOC_NONE,
        {"semantic_role": BOSemanticRole.RAW},
    ),
    MockAttrDesc(
        "id", int, BOColumnConstraint.BOC_PK_INC, {"semantic_role": BOSemanticRole.RAW}
    ),
    MockAttrDesc(
        "last_updated",
        datetime.datetime,
        BOColumnConstraint.BOC_DEFAULT_CURR | BOColumnConstraint.BOC_ON_UPDATE_CURR,
        {"semantic_role": BOSemanticRole.RAW},
    ),
    MockAttrDesc(
        "mock_attr1",
        str,
        BOColumnConstraint.BOC_NONE,
        {"semantic_role": BOSemanticRole.RAW},
    ),
    MockAttrDesc(
        "mock_attr2",
        BOBaseBase,
        BOColumnConstraint.BOC_FK,
        {"semantic_role": BOSemanticRole.RAW, "relation": MockPersistentBO1},
    ),
    MockAttrDesc(
        "mock_attr3",
        list,
        BOColumnConstraint.BOC_NONE,
        {"semantic_role": BOSemanticRole.RAW},
    ),
]

mock_bo2_as_dict = {a.name: a.data_type for a in mock_attr_desc}
mock_bo2_constr_vals = {a.name: a.constraint_values for a in mock_attr_desc}


class Test_100_Persistent_Business_Object_classmethods(
    unittest.IsolatedAsyncioTestCase
):

    async def test_101_convert_from_db(self):
        none_type = PersistentBusinessObject.convert_from_db(None, str, {})
        self.assertIsNone(none_type)

        test_date_str = "2023-08-15"
        date_type = PersistentBusinessObject.convert_from_db(
            test_date_str, datetime.date, {}
        )
        self.assertEqual(datetime.date.fromisoformat(test_date_str), date_type)

        test_date_time_str = "2023-08-15T14:30:00+00:00"
        date_time_type = PersistentBusinessObject.convert_from_db(
            test_date_time_str, datetime.datetime, {}
        )

        # We need to compare to the actual datetime.fromisoformat result, because the timezone conversion may vary
        # This is probably preferable to mocking datetime.fromisoformat, since we don't want to test whether the method under test uses it
        self.assertEqual(
            datetime.datetime.fromisoformat(test_date_time_str), date_time_type
        )

        test_dict = {"key1": "value1", "key2": 2}
        test_list = [1, 2, 3, "four"]
        dict_type = PersistentBusinessObject.convert_from_db(
            json.dumps(test_dict), dict, {}
        )
        self.assertEqual(str(test_dict), str(dict_type))
        list_type = PersistentBusinessObject.convert_from_db(
            json.dumps(test_list), list, {}
        )
        self.assertEqual(str(test_list), str(list_type))

        with patch(
            "business_objects.persistent_business_object.BaseFlag"
        ) as MockBaseFlag:

            class MockFlag(MockBaseFlag):  # type: ignore
                pass

            mock_flag_value = "flag_value"
            flag_type = PersistentBusinessObject.convert_from_db(
                "flag_value", MockFlag, {"flag_type": mock_flag_value}
            )
            self.assertEqual(mock_flag_value, flag_type)

    async def test_102_sql_create_table(self):
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
            "business_objects.persistent_business_object.SQLTransaction",
            new=MockSQLTx,
        ):
            await MockPersistentBO2.sql_create_table()

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
        mock_fetch_result = {"count": 47}
        mock_conditions = "{mock conditions}"
        mock_cursor = Mock(name="mock_cursor")
        mock_cursor.fetchone = AsyncMock(return_value=mock_fetch_result)
        mock_sql = Mock(name="mock_sql")
        mock_sql.execute = AsyncMock(return_value=mock_cursor)
        mock_sql.select = Mock(return_value=mock_sql)
        mock_sql.from_ = Mock(return_value=mock_sql)
        mock_sql.where = Mock(return_value=mock_sql)
        mock_sql.__aenter__ = AsyncMock(return_value=mock_sql)
        mock_sql.__aexit__ = AsyncMock(return_value=None)
        MockSQL = Mock(name="MockSQL", return_value=mock_sql)

        with (
            patch("business_objects.persistent_business_object.SQL", new=MockSQL),
            patch("business_objects.persistent_business_object.Filter") as MockFilter,
        ):
            result = await MockPersistentBO2.count_rows(mock_conditions)
        MockSQL.assert_called_once_with()
        mock_sql.__aenter__.assert_awaited_once_with()
        mock_sql.__aexit__.assert_awaited_once_with(None, None, None)
        mock_sql.select.assert_called_once_with(["count(*) as count"])
        mock_sql.from_.assert_called_once_with(MOCK_TAB2)
        MockFilter.assert_called_once_with(mock_conditions)
        mock_sql.where.assert_called_once_with(MockFilter())
        mock_sql.execute.assert_awaited_once_with()
        mock_cursor.fetchone.assert_awaited_once_with()
        self.assertEqual(result, mock_fetch_result["count"])

    async def test_103_get_matching_ids(self):
        mock_conditions = "{mock conditions}"
        mock_result = [1, 99]
        mock_fetch_result = [{"id": i} for i in mock_result]
        mock_cursor = Mock(name="mock_cursor")
        mock_cursor.fetchall = AsyncMock(return_value=mock_fetch_result)
        mock_sql = Mock(name="mock_sql")
        mock_sql.execute = AsyncMock(return_value=mock_cursor)
        mock_sql.select = Mock(return_value=mock_sql)
        mock_sql.from_ = Mock(return_value=mock_sql)
        mock_sql.where = Mock(return_value=mock_sql)
        mock_sql.__aenter__ = AsyncMock(return_value=mock_sql)
        mock_sql.__aexit__ = AsyncMock(return_value=None)
        MockSQL = Mock(name="MockSQL", return_value=mock_sql)

        with (
            patch("business_objects.persistent_business_object.SQL", new=MockSQL),
            patch("business_objects.persistent_business_object.Filter") as MockFilter,
        ):
            result = await MockPersistentBO2.get_matching_ids(mock_conditions)
        MockSQL.assert_called_once_with()
        mock_sql.__aenter__.assert_awaited_once_with()
        mock_sql.__aexit__.assert_awaited_once_with(None, None, None)
        mock_sql.select.assert_called_once_with(["id"])
        mock_sql.from_.assert_called_once_with(MOCK_TAB2)
        MockFilter.assert_called_once_with(mock_conditions)
        mock_sql.where.assert_called_once_with(MockFilter())
        mock_sql.execute.assert_awaited_once_with()
        mock_cursor.fetchall.assert_awaited_once_with()
        self.assertEqual(result, mock_result)

    async def _104_get_matching_objects(self, attributes=None):
        mock_conditions = "{mock conditions}"
        if attributes:
            mock_cols = [a for a in attributes if a in mock_bo2_as_dict]
            if "id" not in mock_cols:
                mock_cols.append("id")
        else:
            mock_cols = attributes

        mock_fetch_result = [
            {
                a: (
                    "mockpersistentbo2"
                    if a == "bo_name"
                    else (
                        1
                        if a == "id"
                        else (
                            MockPersistentBO1(bo_id=2)
                            if a == "mock_attr2"
                            else (
                                [1, 11]
                                if a == "mock_attr3"
                                else "12341231 123456" if a == "last_updated" else a
                            )
                        )
                    )
                )
                for a in (mock_cols if mock_cols else mock_bo2_as_dict)
            },
            {
                a: (
                    "mockpersistentbo2"
                    if a == "bo_name"
                    else (
                        9
                        if a == "id"
                        else (
                            MockPersistentBO1(bo_id=8)
                            if a == "mock_attr2"
                            else (
                                [99, 99]
                                if a == "mock_attr3"
                                else "12341231 123456" if a == "last_updated" else a
                            )
                        )
                    )
                )
                for a in (mock_cols if mock_cols else mock_bo2_as_dict)
            },
        ]
        mock_result = [
            # {a if a != "id" else "bo_id": v for a, v in r.items()}
            MockPersistentBO2(
                bo_id=r.get("id"), **{a: v for a, v in r.items() if a != "id"}
            )
            for r in mock_fetch_result
        ]
        mock_cursor = Mock(name="mock_cursor")
        mock_cursor.fetchall = AsyncMock(return_value=mock_fetch_result)
        mock_sql = Mock(name="mock_sql")
        mock_sql.execute = AsyncMock(return_value=mock_cursor)
        mock_sql.select = Mock(return_value=mock_sql)
        mock_sql.from_ = Mock(return_value=mock_sql)
        mock_sql.where = Mock(return_value=mock_sql)
        mock_convert_from_db = Mock(
            name="convert_from_db", side_effect=lambda value, typ, subtyp: value
        )

        MockSQL = Mock(name="MockSQL", return_value=mock_sql)
        mock_sql.__aenter__ = AsyncMock(return_value=mock_sql)
        mock_sql.__aexit__ = AsyncMock(return_value=None)

        mock_get_business_object_by_name = Mock(
            name="get_business_object_by_name", return_value=MockPersistentBO2
        )

        with (
            patch("business_objects.persistent_business_object.SQL", new=MockSQL),
            patch("business_objects.persistent_business_object.Filter") as MockFilter,
            patch(
                "business_objects.persistent_business_object.PersistentBusinessObject.convert_from_db",
                new=mock_convert_from_db,
            ),
            patch(
                "business_objects.persistent_business_object.BOBase.get_business_object_by_name",
                new=mock_get_business_object_by_name,
            ),
        ):
            result = await MockPersistentBO2.get_matching_objects(
                mock_conditions, mock_cols
            )
        MockSQL.assert_called_once_with()
        mock_sql.__aenter__.assert_awaited_once_with()
        mock_sql.__aexit__.assert_awaited_once_with(None, None, None)
        mock_sql.select.assert_called_once_with(mock_cols if mock_cols else None)
        mock_sql.from_.assert_called_once_with(MOCK_TAB2)
        MockFilter.assert_called_once_with(mock_conditions)
        mock_sql.where.assert_called_once_with(MockFilter())
        mock_sql.execute.assert_awaited_once_with()
        mock_cursor.fetchall.assert_awaited_once_with()
        convert_args = [
            call(value, mock_bo2_as_dict[key], mock_bo2_constr_vals[key])
            for row in mock_fetch_result
            for key, value in row.items()
            if key != "id"
        ]
        self.assertEqual(
            mock_convert_from_db.call_count, len(convert_args), "attributes converted"
        )
        self.assertEqual(mock_convert_from_db.call_args_list, convert_args)
        self.assertEqual(result, mock_result)

    async def test_104_get_matching_objects(self):
        await self._104_get_matching_objects()
        await self._104_get_matching_objects([])
        await self._104_get_matching_objects(["mock_attr1", "mock_attr3"])


class Test_200_BOBase_access(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_bo = MockPersistentBO2()
        self.mock_sql = Mock(name="mock_sql")
        self.mock_sql.__aenter__ = AsyncMock(return_value=self.mock_sql)
        self.mock_sql.__aexit__ = AsyncMock(return_value=None)
        self.mock_cursor = Mock(name="mock_cursor")
        self.FETCH_RESULT = {
            "bo_name": "mock persistent bo 2",
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
        with patch("business_objects.persistent_business_object.SQL", new=self.MockSQL):
            self.mock_bo.id = None
            result = await self.mock_bo.fetch()
            self.MockSQL.assert_not_called()
            self.assertIs(result, self.mock_bo)

    async def _202_fetch(self, patch_exp, exp_params, newest=DEFAULT):
        with (
            patch("business_objects.persistent_business_object.SQL", new=self.MockSQL),
            patch(
                "business_objects.persistent_business_object." + patch_exp
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
        mock_attrs |= {"bo_name": "mockpersistentbo2"}
        with (
            patch(
                "business_objects.persistent_business_object.SQLTransaction",
                new=self.MockSQLTx,
            ),
            patch(
                "business_objects.persistent_business_object.PersistentBusinessObject._fetch_self"
            ) as mock_fetch_self,
        ):
            mock_bo = MockPersistentBO2(**mock_attrs)

            await mock_bo._insert_self()

            self.MockSQLTx.assert_called_once_with()
            self.mock_tx.__aenter__.assert_awaited_once_with()
            self.mock_tx.__aexit__.assert_awaited_once_with(None, None, None)
            self.mock_sql.insert.assert_called_once_with(MOCK_TAB2)
            self.mock_sql.rows.assert_called_once_with(
                [(a, mock_attrs[a]) for a in mock_bo2_as_dict if a in mock_attrs]
            )
            self.mock_sql.returning.assert_called_once_with("id")
            self.mock_sql.execute.assert_awaited_once_with()
            self.mock_cursor.fetchone.assert_awaited_once_with()
            self.assertEqual(mock_bo.id, self.FETCH_RESULT["id"])
            mock_fetch_self.assert_awaited_once_with(self.mock_sql, id=mock_bo.id)

    async def test_204a_insert_self(self):
        with self.assertRaises(AssertionError):
            self.mock_bo.id = 77
            await self.mock_bo._insert_self()

    async def test_204b_insert_self(self):
        await self._204_insert_self()

    async def test_204c_insert_self(self):
        mock_bo1 = MockPersistentBO1()
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
                "business_objects.persistent_business_object.SQLTransaction",
                new=self.MockSQLTx,
            ),
            patch("business_objects.persistent_business_object.Value") as MockValue,
            patch("business_objects.persistent_business_object.Eq") as MockEq,
            patch(
                "business_objects.persistent_business_object.PersistentBusinessObject.convert_from_db",
                new=mock_convert_from_db,
            ),
            patch(
                "business_objects.persistent_business_object.PersistentBusinessObject._fetch_self"
            ) as mock_fetch_self,
            patch(
                "business_objects.persistent_business_object.datetime"
            ) as mock_datetime,
        ):
            convert_args = [
                call(
                    self.mock_bo._db_data.get(a),
                    mock_bo2_as_dict[a],
                    mock_bo2_constr_vals[a],
                )
                for a in mock_bo2_as_dict
                if a not in ("bo_name", "id")
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
                if a not in ("bo_name", "id")
                and self.mock_bo._data[a] != self.mock_bo._db_data.get(a)
            ]
            last_updated_present = (
                "last_updated" in self.mock_bo._data
                and self.mock_bo._data["last_updated"] is not None
            )
            if not last_updated_present:
                new_vals.append(("last_updated", "mock_CURRENT_TIMESTAMP"))
            mock_dt = Mock(name="mock_dt")
            mock_dt.astimezone = Mock(return_value="mock_CURRENT_TIMESTAMP")
            mock_datetime.now = Mock(name="datetime.now", return_value=mock_dt)
            # Mock_DB_Val = Mock(
            #     name="Mock_DB_Val",
            #     side_effect=[v[1] for v in new_vals],
            # )
            self.mock_bo.fetch = AsyncMock(name="fetch")
            # Mock_DB_Eq = Mock(name="Mock_DB_Eq", return_value="mock eq")
            # self.mock_sql.get_sql_class = Mock(side_effect=[Mock_DB_Val, Mock_DB_Eq])
            id = self.mock_bo.id

            if exception:
                self.mock_sql.execute.side_effect = [Exception, None]
                with self.assertRaises(Exception):
                    await self.mock_bo._update_self()
            else:
                await self.mock_bo._update_self()

            self.MockSQLTx.assert_called_once_with()
            self.mock_tx.__aenter__.assert_awaited_once_with()
            self.mock_sql.update.assert_called_once_with(MOCK_TAB2)
            MockEq.assert_called_once_with("id", id)
            self.mock_sql.where.assert_called_once_with(MockEq())
            self.mock_bo.attribute_descriptions.assert_called_once_with()
            self.assertEqual(
                PersistentBusinessObject.convert_from_db.call_count,  # type: ignore
                len(convert_args),
                "attributes converted",
            )
            self.assertEqual(
                PersistentBusinessObject.convert_from_db.call_args_list, convert_args  # type: ignore
            )
            self.assertEqual(self.mock_sql.assignment.call_count, len(new_vals))
            self.assertEqual(MockValue.call_count, len(new_vals))
            for v in new_vals:
                MockValue.assert_any_call(v[0], v[1])
                self.mock_sql.assignment.assert_any_call(v[0], MockValue())
            if not last_updated_present:
                mock_datetime.now.assert_called_once_with()
                mock_dt.astimezone.assert_called_once_with(datetime.UTC)
            self.mock_sql.execute.assert_awaited_once_with()
            mock_fetch_self.assert_awaited_once_with(self.mock_sql, id=id)
            if exception:
                self.mock_tx.__aexit__.assert_awaited_once_with(Exception, ANY, ANY)
            else:
                self.mock_tx.__aexit__.assert_awaited_once_with(None, None, None)

    async def test_205a_update_self(self):
        with self.assertRaises(AssertionError) as exp:
            await self.mock_bo._update_self()

    async def test_205b_update_self(self):
        self.mock_bo = MockPersistentBO2(bo_id=55)
        await self._205_update_self()

    async def test_205c_update_self(self):
        self.mock_bo = MockPersistentBO2(bo_id=55, mock_attr2=MockPersistentBO1())
        await self._205_update_self()

    async def test_205d_update_self(self):
        self.mock_bo = MockPersistentBO2(
            bo_id=55, mock_attr2=MockPersistentBO1(), mock_attr3=[1, 2, 3]
        )
        await self._205_update_self()

    async def test_205e_update_self_exception(self):
        self.mock_bo = MockPersistentBO2(bo_id=55)
        await self._205_update_self(exception=True)

    async def test_205f_update_last_updated(self):
        self.mock_bo = MockPersistentBO2(bo_id=55)
        self.mock_bo.last_updated = datetime.datetime(2031, 4, 25, 13, 45)
        await self._205_update_self()


class Test_300_BOBase_instancemethods(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_bo = MockPersistentBO2()

    def test_01_convert_from_db_none(self):
        self.assertIsNone(MockPersistentBO2.convert_from_db(None, int, {}))

    def test_01_convert_from_db_date_time(self):
        mock_tz_cet = datetime.timezone(datetime.timedelta(hours=+1), name="CET")
        mock_tz_est = datetime.timezone(datetime.timedelta(hours=-5), name="EST")
        mock_dt_utc = datetime.datetime(2031, 4, 25, 13, 45, tzinfo=datetime.UTC)
        mock_dt_cet = mock_dt_utc.astimezone(mock_tz_cet)
        mock_dt_est = mock_dt_utc.astimezone(mock_tz_est)
        mock_dt_none = datetime.datetime(2031, 4, 25, 13, 45)
        mock_date = datetime.date(2031, 4, 25)

        # convert 'CET' datetime string
        res = MockPersistentBO2.convert_from_db(
            value=mock_dt_cet.isoformat(), typ=datetime.datetime, subtyp={}
        )
        self.assertEqual(mock_dt_cet, res)
        self.assertEqual(datetime.UTC, res.tzinfo)

        # convert 'CET' datetime object
        res = MockPersistentBO2.convert_from_db(
            value=mock_dt_cet, typ=datetime.datetime, subtyp={}
        )
        self.assertEqual(mock_dt_cet, res)
        self.assertEqual(datetime.UTC, res.tzinfo)

        # convert 'EST' datetime string
        res = MockPersistentBO2.convert_from_db(
            value=mock_dt_est.isoformat(), typ=datetime.datetime, subtyp={}
        )
        self.assertEqual(mock_dt_est, res)
        self.assertEqual(datetime.UTC, res.tzinfo)

        # convert 'UTC' datetime string
        res = MockPersistentBO2.convert_from_db(
            value=mock_dt_utc.isoformat(), typ=datetime.datetime, subtyp={}
        )
        self.assertEqual(mock_dt_utc, res)
        self.assertEqual(datetime.UTC, res.tzinfo)

        # convert naive datetime string (assume UTC)
        res = MockPersistentBO2.convert_from_db(
            value=mock_dt_none.isoformat(), typ=datetime.datetime, subtyp={}
        )
        self.assertEqual(mock_dt_none.replace(tzinfo=datetime.UTC), res)
        self.assertEqual(datetime.UTC, res.tzinfo)

        # convert naive datetime (assume UTC)
        res = MockPersistentBO2.convert_from_db(
            value=mock_dt_none, typ=datetime.datetime, subtyp={}
        )
        self.assertEqual(mock_dt_none.replace(tzinfo=datetime.UTC), res)
        self.assertEqual(datetime.UTC, res.tzinfo)

        res = MockPersistentBO2.convert_from_db(
            value=mock_date.isoformat(), typ=datetime.date, subtyp={}
        )
        self.assertEqual(mock_date, res)


class Test_400_Specialized(unittest.IsolatedAsyncioTestCase):
    """Tests for the Specialized mixin and related PersistentBusinessObject behaviour."""

    # ------------------------------------------------------------------ #
    #  Fixture classes – defined locally so each test run is deterministic #
    # ------------------------------------------------------------------ #

    def setUp(self):
        """Create a fresh set of mock BO classes for every test."""

        class GenericBO(PersistentBusinessObject):
            generic_attr = BOStr()

        class SpecializedBO(Specialized, GenericBO):
            special_attr = BOStr()

        class VerySpecializedBO(SpecializedBO):
            very_special_attr = BOStr()

        self.GenericBO = GenericBO
        self.SpecializedBO = SpecializedBO
        self.VerySpecializedBO = VerySpecializedBO

    # ------------------------------------------------------------------ #
    #  is_specializing                                                     #
    # ------------------------------------------------------------------ #

    def test_401_is_specializing_false_for_regular_bo(self):
        self.assertFalse(self.GenericBO.is_specializing())

    def test_401_is_specializing_true_for_specialized_mixin(self):
        self.assertTrue(self.SpecializedBO.is_specializing())

    def test_401_is_specializing_true_for_subclass_of_specialized(self):
        """A class that inherits from a Specialized BO (without re-applying the mixin)
        is also considered specializing."""
        self.assertTrue(self.VerySpecializedBO.is_specializing())

    def test_401_is_specializing_false_for_pbo_base(self):
        """PersistentBusinessObject itself is not a specialization."""
        self.assertFalse(PersistentBusinessObject.is_specializing())

    # ------------------------------------------------------------------ #
    #  register_bo_class – specialist registration                         #
    # ------------------------------------------------------------------ #

    def test_402_register_bo_class_populates_specialists_on_generic(self):
        """Registering a Specialized BO must add it to the base BO's specialists set."""
        self.SpecializedBO.register_bo_class()
        self.assertIn(self.SpecializedBO, self.GenericBO.specialists)

    def test_402_register_bo_class_also_adds_self_to_own_specialists(self):
        """After registration the specializing class must appear in its own specialists set."""
        self.SpecializedBO.register_bo_class()
        self.assertIn(self.SpecializedBO, self.SpecializedBO.specialists)

    def test_402_register_bo_class_multi_level(self):
        """Registering VerySpecializedBO must propagate to both SpecializedBO and GenericBO."""
        self.SpecializedBO.register_bo_class()
        self.VerySpecializedBO.register_bo_class()
        self.assertIn(self.VerySpecializedBO, self.GenericBO.specialists)
        self.assertIn(self.VerySpecializedBO, self.SpecializedBO.specialists)

    def test_402_register_bo_class_non_specializing_unaffected(self):
        """Registering a plain BO must not alter its (empty) specialists set."""
        self.GenericBO.register_bo_class()
        self.assertEqual(self.GenericBO.specialists, set())

    # ------------------------------------------------------------------ #
    #  attribute_descriptions with include_specialized                     #
    # ------------------------------------------------------------------ #

    def test_403_attribute_descriptions_without_include_specialized(self):
        """Without include_specialized the base BO returns only its own attributes."""
        self.SpecializedBO.register_bo_class()
        names = [
            d.name
            for d in self.GenericBO.attribute_descriptions(include_specialized=False)
        ]
        self.assertIn("generic_attr", names)
        self.assertNotIn("special_attr", names)

    def test_403_attribute_descriptions_with_include_specialized(self):
        """With include_specialized=True the base BO also returns attributes of specialist BOs."""
        self.SpecializedBO.register_bo_class()
        names = [
            d.name
            for d in self.GenericBO.attribute_descriptions(include_specialized=True)
        ]
        self.assertIn("generic_attr", names)
        self.assertIn("special_attr", names)

    def test_403_attribute_descriptions_no_duplicates(self):
        """Attributes already present on the base BO must not be duplicated even if a
        specialist declares the same attribute name."""
        self.SpecializedBO.register_bo_class()
        names = [
            d.name
            for d in self.GenericBO.attribute_descriptions(include_specialized=True)
        ]
        self.assertEqual(len(names), len(set(names)))

    # ------------------------------------------------------------------ #
    #  sql_create_table – skip for Specialized                             #
    # ------------------------------------------------------------------ #

    async def test_404_sql_create_table_skipped_for_specialized(self):
        """sql_create_table must be a no-op for a Specialized BO (it shares the base table)."""
        MockSQLTx = Mock(name="MockSQLTx")
        with patch(
            "business_objects.persistent_business_object.SQLTransaction",
            new=MockSQLTx,
        ):
            await self.SpecializedBO.sql_create_table()
        MockSQLTx.assert_not_called()

    async def test_404_sql_create_table_executes_for_generic(self):
        """sql_create_table must proceed normally for a non-Specialized base BO."""
        mock_sql = Mock(name="mock_sql")
        mock_sql.create_table = Mock(return_value=mock_sql)
        mock_sql.column = Mock()
        mock_sql.execute = AsyncMock()
        mock_tx = AsyncMock(name="mock_tx")
        mock_tx.__aenter__ = AsyncMock(return_value=mock_tx)
        mock_tx.__aexit__ = AsyncMock(return_value=False)
        mock_tx.sql = Mock(return_value=mock_sql)
        MockSQLTx = Mock(name="MockSQLTx", return_value=mock_tx)
        with patch(
            "business_objects.persistent_business_object.SQLTransaction",
            new=MockSQLTx,
        ):
            await self.GenericBO.sql_create_table()
        MockSQLTx.assert_called_once_with()
        mock_sql.create_table.assert_called_once_with(self.GenericBO.table)

    # ------------------------------------------------------------------ #
    #  _filter_conditions with specialists                                 #
    # ------------------------------------------------------------------ #

    def test_405_filter_conditions_no_specialists(self):
        """Without specialists _filter_conditions returns the given conditions unchanged."""
        mock_cond = Mock(name="mock_condition")
        result = self.GenericBO._filter_conditions(mock_cond)
        self.assertIs(result, mock_cond)

    def test_405_filter_conditions_no_specialists_none(self):
        """Without specialists and no conditions _filter_conditions returns None."""
        result = self.GenericBO._filter_conditions(None)
        self.assertIsNone(result)

    def test_405_filter_conditions_with_specialists_no_extra_cond(self):
        """With specialists _filter_conditions wraps a bo_name IN expression."""
        self.SpecializedBO.register_bo_class()
        with (
            patch("business_objects.persistent_business_object.In") as MockIn,
            patch(
                "business_objects.persistent_business_object.ColumnName"
            ) as MockColumnName,
            patch(
                "business_objects.persistent_business_object.SQLString"
            ) as MockSQLString,
        ):
            result = self.GenericBO._filter_conditions(None)
        MockColumnName.assert_called_once_with("bo_name")
        MockIn.assert_called_once()
        self.assertIs(result, MockIn())

    def test_405_filter_conditions_with_specialists_and_extra_cond(self):
        """With specialists and extra conditions _filter_conditions combines both with And."""
        self.SpecializedBO.register_bo_class()
        mock_cond = Mock(name="mock_condition")
        with (
            patch("business_objects.persistent_business_object.In") as MockIn,
            patch("business_objects.persistent_business_object.ColumnName"),
            patch("business_objects.persistent_business_object.SQLString"),
            patch("business_objects.persistent_business_object.And") as MockAnd,
        ):
            result = self.GenericBO._filter_conditions(mock_cond)
        MockAnd.assert_called_once_with([MockIn(), mock_cond])
        self.assertIs(result, MockAnd())

    # ------------------------------------------------------------------ #
    #  get_matching_objects – bo_name column added when specialists exist  #
    # ------------------------------------------------------------------ #

    async def test_406_get_matching_objects_adds_bo_name_for_specialists(self):
        """When attributes are requested and specialists exist, 'bo_name' must be
        appended to the SELECT column list automatically."""
        self.SpecializedBO.register_bo_class()
        mock_cursor = Mock(name="mock_cursor")
        mock_cursor.fetchall = AsyncMock(return_value=[])
        mock_sql = Mock(name="mock_sql")
        mock_sql.execute = AsyncMock(return_value=mock_cursor)
        mock_sql.select = Mock(return_value=mock_sql)
        mock_sql.from_ = Mock(return_value=mock_sql)
        mock_sql.where = Mock(return_value=mock_sql)
        mock_sql.__aenter__ = AsyncMock(return_value=mock_sql)
        mock_sql.__aexit__ = AsyncMock(return_value=None)
        MockSQL = Mock(name="MockSQL", return_value=mock_sql)
        with (
            patch("business_objects.persistent_business_object.SQL", new=MockSQL),
            patch("business_objects.persistent_business_object.Filter"),
            patch("business_objects.persistent_business_object.In"),
            patch("business_objects.persistent_business_object.ColumnName"),
            patch("business_objects.persistent_business_object.SQLString"),
        ):
            await self.GenericBO.get_matching_objects(
                conditions=None, attributes=["generic_attr"]
            )
        select_call_args = mock_sql.select.call_args[0][0]
        self.assertIn("bo_name", select_call_args)
