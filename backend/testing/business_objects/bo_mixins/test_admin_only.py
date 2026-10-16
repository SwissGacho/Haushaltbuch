"""Tests for business_objects.bo_mixins.admin_only."""

import unittest
from unittest.mock import Mock, patch

from business_objects.bo_mixins.admin_only import AdminOnly


class TestAdminOnlyMixin(unittest.TestCase):
    def test_is_admin_only_true(self):
        self.assertTrue(AdminOnly.is_admin_only())

    def test_specialist_conditions_allow_anonymous_and_admin_users(self):
        class Specialist:
            @staticmethod
            def bo_type_name():
                return "admin_target"

        self.assertEqual([], AdminOnly.specialist_conditions_mixin(Specialist, None))
        self.assertEqual(
            [],
            AdminOnly.specialist_conditions_mixin(
                Specialist, Mock(name="admin_user", is_admin=True)
            ),
        )

    def test_specialist_conditions_non_admin_user_excludes_admin_specialist(self):
        class Specialist:
            @staticmethod
            def bo_type_name():
                return "admin_target"

        user = Mock(name="user", is_admin=False)
        with (
            patch("business_objects.bo_mixins.admin_only.ColumnName") as mock_column_name,
            patch("business_objects.bo_mixins.admin_only.SQLString") as mock_sql_string,
            patch("business_objects.bo_mixins.admin_only.Eq") as mock_eq,
            patch("business_objects.bo_mixins.admin_only.Not") as mock_not,
        ):
            result = AdminOnly.specialist_conditions_mixin(Specialist, user)

        mock_column_name.assert_called_once_with("bo_name")
        mock_sql_string.assert_called_once_with("admin_target")
        mock_eq.assert_called_once_with(mock_column_name.return_value, mock_sql_string())
        mock_not.assert_called_once_with(mock_eq())
        self.assertEqual([mock_not()], result)
