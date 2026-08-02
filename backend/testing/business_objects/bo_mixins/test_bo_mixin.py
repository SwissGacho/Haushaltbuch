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


class _StoreTarget(MixinBase):
    def __init__(self, bo_id=None):
        self.id = bo_id
        self.calls = []

    async def insert_self(self, session=None):
        self.calls.append(("insert", session))

    async def update_self(self, session=None):
        self.calls.append(("update", session))


class _StoreTargetInvalid(MixinBase):
    def __init__(self):
        self.id = None

    def insert_self(self, session=None):
        return None

    def update_self(self, session=None):
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

    def test_specialist_conditions_without_specialists(self):
        result = MixinBase._specialist_conditions(_GenericNoSpecialists, user=None)
        self.assertEqual([], result)

    def test_specialist_conditions_without_user_builds_in_clause(self):
        with (
            patch("business_objects.bo_mixins.bo_mixin.ColumnName") as mock_column_name,
            patch("business_objects.bo_mixins.bo_mixin.SQLString") as mock_sql_string,
            patch("business_objects.bo_mixins.bo_mixin.In") as mock_in,
        ):
            result = MixinBase._specialist_conditions(_GenericPublicOnly, user=None)

        mock_column_name.assert_called_once_with("bo_name")
        mock_sql_string.assert_called_once_with("public_bo")
        mock_in.assert_called_once()
        self.assertEqual([mock_in()], result)

    def test_specialist_conditions_without_user_and_no_valid_specialist_raises(self):
        with self.assertRaises(ValueError):
            MixinBase._specialist_conditions(_GenericPersonalOnly, user=None)

    def test_specialist_conditions_with_non_admin_user_and_only_admin_specialists(self):
        mock_user = Mock(name="user", is_admin=False)

        with patch("business_objects.bo_mixins.bo_mixin.LOG.warning") as mock_warning:
            result = MixinBase._specialist_conditions(_GenericAdminOnly, user=mock_user)

        self.assertEqual([], result)
        mock_warning.assert_called_once()

    def test_special_conditions_collects_base_and_mixin_conditions(self):
        with patch.object(MixinBase, "_specialist_conditions", return_value=["base_cond"]):
            result = _CollectorBO.special_conditions(gen_cls=Mock(name="gen_cls"), user=None)

        self.assertEqual(["base_cond", "mixin_cond"], result)

    async def test_store_mixin_calls_insert_for_new_object(self):
        target = _StoreTarget(bo_id=None)
        session = Mock(name="session")

        await target.store_mixin(session=session)

        self.assertEqual([("insert", session)], target.calls)

    async def test_store_mixin_calls_update_for_existing_object(self):
        target = _StoreTarget(bo_id=123)
        session = Mock(name="session")

        await target.store_mixin(session=session)

        self.assertEqual([("update", session)], target.calls)

    async def test_store_mixin_raises_for_non_coroutines(self):
        with self.assertRaises(TypeError):
            await _StoreTargetInvalid().store_mixin(session=None)
