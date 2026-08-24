"""Tests for business_objects.bo_mixins.personal."""

import unittest
from unittest.mock import Mock, call, patch

from business_objects.bo_mixins.personal import Personal


class _Desc:
    def __init__(self, name):
        self.name = name


class _User:
    def __init__(self, user_id):
        self.id = user_id


class _PersonalTarget(Personal):
    def __init__(self, bo_id=None, user_id=None):
        self.id = bo_id
        self.user_id = user_id
        self.calls = []

    @staticmethod
    def bo_type_name():
        return "personal_target"

    async def insert_self(self, session=None):
        self.calls.append(("insert", session))

    async def update_self(self, session=None):
        self.calls.append(("update", session))


class _PersonalNoUserIdDesc(Personal):
    @classmethod
    def attribute_descriptions(cls):
        return [_Desc("id")]


class _PersonalWithUserIdDesc(Personal):
    @classmethod
    def attribute_descriptions(cls):
        return [_Desc("id"), _Desc("user_id")]


class TestPersonalMixin(unittest.IsolatedAsyncioTestCase):
    def test_is_personal_true(self):
        self.assertTrue(Personal.is_personal())

    def test_special_conditions_requires_user(self):
        with self.assertRaises(ValueError):
            Personal.special_conditions_mixin(gen_cls=_PersonalTarget, user=None)

    def test_special_conditions_builds_user_filter(self):
        mock_user = Mock(name="user")
        with (
            patch("business_objects.bo_mixins.personal.Value") as mock_value,
            patch("business_objects.bo_mixins.personal.Eq") as mock_eq,
        ):
            result = Personal.special_conditions_mixin(
                gen_cls=_PersonalTarget, user=mock_user
            )

        mock_value.assert_called_once_with(mock_user)
        mock_eq.assert_called_once_with("user_id", mock_value())
        self.assertEqual([mock_eq()], result)

    def test_specialist_conditions_without_user_excludes_personal_specialist(self):
        with (
            patch("business_objects.bo_mixins.personal.ColumnName") as mock_column_name,
            patch("business_objects.bo_mixins.personal.SQLString") as mock_sql_string,
            patch("business_objects.bo_mixins.personal.Eq") as mock_eq,
            patch("business_objects.bo_mixins.personal.Not") as mock_not,
        ):
            result = Personal.specialist_conditions_mixin(
                specialist_cls=_PersonalTarget, user=None
            )

        mock_column_name.assert_called_once_with("bo_name")
        mock_sql_string.assert_called_once_with("personal_target")
        mock_eq.assert_called_once_with(mock_column_name.return_value, mock_sql_string())
        mock_not.assert_called_once_with(mock_eq())
        self.assertEqual([mock_not()], result)

    def test_specialist_conditions_with_user_excludes_other_users(self):
        mock_user = Mock(name="user")
        with (
            patch("business_objects.bo_mixins.personal.ColumnName") as mock_column_name,
            patch("business_objects.bo_mixins.personal.SQLString") as mock_sql_string,
            patch("business_objects.bo_mixins.personal.Value") as mock_value,
            patch("business_objects.bo_mixins.personal.Eq") as mock_eq,
            patch("business_objects.bo_mixins.personal.Not") as mock_not,
            patch("business_objects.bo_mixins.personal.And") as mock_and,
        ):
            result = Personal.specialist_conditions_mixin(
                specialist_cls=_PersonalTarget, user=mock_user
            )

        self.assertEqual(2, mock_column_name.call_count)
        mock_column_name.assert_has_calls([call("bo_name"), call("user_id")])
        mock_sql_string.assert_called_once_with("personal_target")
        mock_value.assert_called_once_with(mock_user)
        mock_eq.assert_has_calls(
            [
                call(mock_column_name.return_value, mock_sql_string.return_value),
                call(mock_column_name.return_value, mock_value.return_value),
            ]
        )
        mock_and.assert_called_once_with(
            [mock_eq.return_value, mock_not.return_value]
        )
        mock_not.assert_has_calls(
            [call(mock_eq.return_value), call(mock_and.return_value)]
        )
        self.assertEqual([mock_not.return_value], result)

    def test_skip_create_table_raises_without_user_id_description(self):
        with self.assertRaises(TypeError):
            _PersonalNoUserIdDesc.skip_create_table()

    def test_skip_create_table_returns_false_with_user_id_description(self):
        self.assertFalse(_PersonalWithUserIdDesc.skip_create_table())

    async def test_store_mixin_requires_session_user(self):
        with self.assertRaises(ValueError):
            await _PersonalTarget().store_mixin(session=None)

    async def test_store_mixin_requires_user_id_attribute(self):
        target = _PersonalTarget()
        delattr(target, "user_id")
        session = Mock(name="session")
        session.user = _User(1)

        with self.assertRaises(ValueError):
            await target.store_mixin(session=session)

    async def test_store_mixin_assigns_user_and_inserts_when_new(self):
        target = _PersonalTarget(bo_id=None, user_id=None)
        session = Mock(name="session")
        session.user = _User(7)

        await target.store_mixin(session=session)

        self.assertIs(target.user_id, session.user)
        self.assertEqual([("insert", session)], target.calls)

    async def test_store_mixin_assigns_user_and_updates_when_existing(self):
        target = _PersonalTarget(bo_id=55, user_id=_User(99))
        session = Mock(name="session")
        session.user = _User(7)

        await target.store_mixin(session=session)

        self.assertIs(target.user_id, session.user)
        self.assertEqual([("update", session)], target.calls)
