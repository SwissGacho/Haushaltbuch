"""Tests for business_objects.bo_mixins.bo_mixin."""

import unittest
from unittest.mock import Mock, patch

from business_objects.bo_mixins.bo_mixin import MixinBase


class _SpecialistPublic:
    @staticmethod
    def bo_type_name() -> str:
        return "public_bo"

    @classmethod
    def is_personal(cls) -> bool:
        return False

    @classmethod
    def is_admin_only(cls) -> bool:
        return False


class _SpecialistPersonal:
    @staticmethod
    def bo_type_name() -> str:
        return "personal_bo"

    @classmethod
    def is_personal(cls) -> bool:
        return True

    @classmethod
    def is_admin_only(cls) -> bool:
        return False


class _SpecialistAdminOnly:
    @staticmethod
    def bo_type_name() -> str:
        return "admin_bo"

    @classmethod
    def is_personal(cls) -> bool:
        return False

    @classmethod
    def is_admin_only(cls) -> bool:
        return True


class _GenericNoSpecialists:
    specialists = set()


class _GenericPublicOnly:
    specialists = {_SpecialistPublic}


class _GenericPersonalOnly:
    specialists = {_SpecialistPersonal}


class _GenericAdminOnly:
    specialists = {_SpecialistAdminOnly}


class _SpecialistWithConditions(MixinBase):
    @staticmethod
    def bo_type_name() -> str:
        return "conditioned_bo"

    @classmethod
    def specialist_conditions_mixin(cls, specialist_cls, user):
        return ["specialist_cond"]


class _GenericWithSpecialistConditions:
    specialists = {_SpecialistWithConditions}


class _StoreTarget(MixinBase):
    def __init__(self, bo_id=None, update_return_value=None):
        self.id = bo_id
        self.calls = []
        self.update_return_value = (
            {} if update_return_value is None else update_return_value
        )

    async def insert_self(self, db_conn, session=None):
        self.calls.append(("insert", db_conn, session))

    async def update_self(self, db_conn, session=None):
        self.calls.append(("update", db_conn, session))
        return self.update_return_value


class _StoreTargetInvalid(MixinBase):
    def __init__(self):
        self.id = None

    def insert_self(self, db_conn, session=None):
        return None

    def update_self(self, db_conn, session=None):
        return None


class _FetchTarget(MixinBase):
    def __init__(self):
        self.fetch_calls = []

    async def fetch_self(self, db_conn, id=None, newest=None, session=None):
        self.fetch_calls.append((db_conn, id, newest, session))


class _FetchTargetInvalid(MixinBase):
    def fetch_self(self, db_conn, id=None, newest=None, session=None):
        return None


class _CollectorMixin(MixinBase):
    @classmethod
    def special_conditions_mixin(cls, gen_cls, user):
        return ["mixin_cond"]


class _CollectorBO(_CollectorMixin):
    pass


class TestBoMixin(unittest.IsolatedAsyncioTestCase):
    def test_default_flags_are_false(self):
        self.assertFalse(MixinBase.is_specializing())
        self.assertFalse(MixinBase.is_personal())
        self.assertFalse(MixinBase.is_admin_only())
        self.assertFalse(MixinBase.skip_create_table())

    def test_specialist_conditions_without_specialists_uses_generic_class(self):
        with (
            patch("business_objects.bo_mixins.bo_mixin.ColumnName") as mock_column_name,
            patch("business_objects.bo_mixins.bo_mixin.SQLString") as mock_sql_string,
            patch("business_objects.bo_mixins.bo_mixin.In") as mock_in,
        ):
            result = MixinBase.specialist_conditions(_GenericNoSpecialists, user=None)

        mock_column_name.assert_called_once_with("bo_name")
        mock_sql_string.assert_called_once_with("_GenericNoSpecialists")
        mock_in.assert_called_once()
        self.assertEqual([mock_in.return_value], result)

    def test_specialist_conditions_without_user_builds_in_clause(self):
        with (
            patch("business_objects.bo_mixins.bo_mixin.ColumnName") as mock_column_name,
            patch("business_objects.bo_mixins.bo_mixin.SQLString") as mock_sql_string,
            patch("business_objects.bo_mixins.bo_mixin.In") as mock_in,
        ):
            result = MixinBase.specialist_conditions(_GenericPublicOnly, user=None)

        mock_column_name.assert_called_once_with("bo_name")
        mock_sql_string.assert_called_once_with("public_bo")
        mock_in.assert_called_once()
        self.assertEqual([mock_in()], result)

    def test_specialist_conditions_delegates_to_specialist_mixins(self):
        result = MixinBase.specialist_conditions(
            _GenericWithSpecialistConditions, user=None
        )

        self.assertEqual(2, len(result))
        self.assertIn("specialist_cond", result)

    def test_special_conditions_collects_base_and_mixin_conditions(self):
        with patch.object(
            MixinBase, "specialist_conditions", return_value=["base_cond"]
        ) as mock_specialist_conditions:
            result = MixinBase.special_conditions(gen_cls=_CollectorBO, user=None)

        mock_specialist_conditions.assert_called_once_with(_CollectorBO, None)
        self.assertEqual(["base_cond", "mixin_cond"], result)

    async def test_fetch_mixin_forwards_arguments(self):
        target = _FetchTarget()
        db_conn = Mock(name="db_conn")
        session = Mock(name="session")

        await target.fetch_mixin(db_conn=db_conn, id=123, newest=True, session=session)

        self.assertEqual([(db_conn, 123, True, session)], target.fetch_calls)

    async def test_fetch_mixin_raises_for_non_coroutine_fetch_self(self):
        with self.assertRaises(TypeError):
            await _FetchTargetInvalid().fetch_mixin(db_conn=Mock(name="db_conn"))

    async def test_store_mixin_calls_insert_for_new_object(self):
        target = _StoreTarget(bo_id=None)
        db_conn = Mock(name="db_conn")
        session = Mock(name="session")

        await target.store_mixin(db_conn=db_conn, session=session)

        self.assertEqual([("insert", db_conn, session)], target.calls)

    async def test_store_mixin_calls_update_for_existing_object(self):
        expected_old_values = {"some_attr": "old value"}
        target = _StoreTarget(bo_id=123, update_return_value=expected_old_values)
        db_conn = Mock(name="db_conn")
        session = Mock(name="session")

        actual_old_values = await target.store_mixin(db_conn=db_conn, session=session)

        self.assertEqual([("update", db_conn, session)], target.calls)
        self.assertEqual(expected_old_values, actual_old_values)

    async def test_store_mixin_raises_for_non_coroutines(self):
        with self.assertRaises(TypeError):
            await _StoreTargetInvalid().store_mixin(
                db_conn=Mock(name="db_conn"), session=None
            )

    async def test_store_mixin_returns_update_self_result(self):
        expected_old_values = {"name": "old name", "amount": 42}
        target = _StoreTarget(bo_id=123, update_return_value=expected_old_values)
        db_conn = Mock(name="db_conn")
        session = Mock(name="session")

        result = await target.store_mixin(db_conn=db_conn, session=session)

        self.assertEqual([("update", db_conn, session)], target.calls)
        self.assertEqual(expected_old_values, result)
