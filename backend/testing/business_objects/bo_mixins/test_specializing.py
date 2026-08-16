"""Tests for business_objects.bo_mixins.specializing."""

import unittest

from business_objects.business_object_base import BOBase
from business_objects.bo_mixins.specializing import Specializing


class TestSpecializingMixin(unittest.TestCase):
    def setUp(self):
        class Seed(BOBase):
            pass

        class GenericBO(BOBase):
            pass

        class SpecialBO(Specializing, GenericBO):
            pass

        class VerySpecialBO(SpecialBO):
            pass

        self.Seed = Seed
        self.GenericBO = GenericBO
        self.SpecialBO = SpecialBO
        self.VerySpecialBO = VerySpecialBO

        self.GenericBO.specialists = {self.Seed}
        self.SpecialBO.specialists = {self.Seed}
        self.VerySpecialBO.specialists = {self.Seed}

    def test_is_specializing_true(self):
        self.assertTrue(self.SpecialBO.is_specializing())

    def test_skip_create_table_true(self):
        self.assertTrue(self.SpecialBO.skip_create_table())

    def test_register_specialist_mixin_adds_to_generic_and_self(self):
        self.SpecialBO.register_specialist_mixin()

        self.assertIn(self.SpecialBO, self.GenericBO.specialists)
        self.assertIn(self.SpecialBO, self.SpecialBO.specialists)

    def test_register_specialist_mixin_propagates_multi_level(self):
        self.SpecialBO.register_specialist_mixin()
        self.VerySpecialBO.register_specialist_mixin()

        self.assertIn(self.VerySpecialBO, self.GenericBO.specialists)
        self.assertIn(self.VerySpecialBO, self.SpecialBO.specialists)
