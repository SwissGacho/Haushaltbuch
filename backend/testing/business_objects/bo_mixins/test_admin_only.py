"""Tests for business_objects.bo_mixins.admin_only."""

import unittest

from business_objects.bo_mixins.admin_only import AdminOnly


class TestAdminOnlyMixin(unittest.TestCase):
    def test_is_admin_only_true(self):
        self.assertTrue(AdminOnly.is_admin_only())
