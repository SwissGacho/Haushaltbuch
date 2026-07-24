"""Test suite for transient BO list objects."""

from typing import Optional
import unittest
from unittest.mock import Mock, patch

from business_objects.business_object_base import BOBase
from bom_transient.navigation_headers import NavigationHeaders
from core.util import _classproperty


class MockBOBase(BOBase):
    @classmethod
    def is_specializing(cls) -> bool:
        return False

    @classmethod
    async def count_rows(cls, conditions: Optional[dict] = None) -> int:
        return 0

    @classmethod
    async def get_matching_ids(cls, conditions: Optional[dict] = None) -> list[int]:
        return []

    @classmethod
    async def get_matching_objects(
        cls, conditions: Optional[dict] = None, attributes: list[str] | None = None
    ) -> list[BOBase]:
        return []

    @_classproperty
    def all_business_objects(cls) -> dict[str, type[BOBase]]:
        return {
            "MockGenericBO": MockGenericBO,
            "MockSpecializedBO": MockSpecializedBO,
        }


class MockGenericBO(MockBOBase):
    """A regular, non-specialized BO: included in the top-level navigation list."""

    @classmethod
    def navigation_header(cls, ref=None):
        if ref:
            return {"name": "MockGenericBO", "type": "referer"}
        return {"name": "MockGenericBO", "type": "generic"}


class MockSpecializedBO(MockBOBase):

    @classmethod
    def is_specializing(cls) -> bool:
        return True

    @classmethod
    def navigation_header(cls, ref=None):
        if ref:
            return {"name": "MockSpecializedBO", "type": "referer"}
        return {"name": "MockSpecializedBO", "type": "specialized"}


class MockRefererBO(MockBOBase):
    @classmethod
    def navigation_header(cls, ref=None):
        return {"name": "MockRefererBO", "type": "referer"}

    @classmethod
    def referenced_by(cls):
        return [(MockGenericBO, "attribute1"), (MockSpecializedBO, "attribute2")]


class Test_100_NavigationHeaders(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_get_business_object_by_name = Mock(
            name="get_business_object_by_name",
            side_effect=lambda name: {
                "mock_bo": MockGenericBO,
                "mock_referer_bo": MockRefererBO,
                "mock_specialized_bo": MockSpecializedBO,
            }.get(name, None),
        )
        MockBOBase.get_business_object_by_name = self.mock_get_business_object_by_name
        self.patchers = [
            patch("bom_transient.navigation_headers.BOBase", MockBOBase),
            patch(
                "bom_transient.navigation_headers.PersistentBusinessObject",
                MockBOBase,
            ),
        ]
        for patcher in self.patchers:
            patcher.start()
        return super().setUp()

    def tearDown(self):
        for patcher in self.patchers:
            patcher.stop()
        return super().tearDown()

    async def test_101_initialization(self):
        nh = NavigationHeaders(index="mock_bo")
        self.assertEqual(nh._parent_bo, MockGenericBO)
        self.mock_get_business_object_by_name.assert_called_once_with("mock_bo")

    async def test_102_initialization_with_index_None(self):
        nh = NavigationHeaders(index=None)
        self.assertIsNone(nh._parent_bo)

    async def test_103_initialization_rejects_non_string_index(self):
        with self.assertRaises(TypeError):
            NavigationHeaders(index=123)

    async def test_104_business_values_as_dict_no_parent(self):
        # Patch a generic and a specialized BO to test the behavior of
        # business_values_as_dict when no parent is provided: specialized BOs
        # (is_specializing() == True) must be excluded from the top-level list.

        nh = NavigationHeaders(index=None)
        result = await nh.business_values_as_dict()
        self.assertIn("headers", result)
        self.assertIsInstance(result["headers"], list)
        self.assertIn({"name": "MockGenericBO", "type": "generic"}, result["headers"])
        self.assertNotIn(
            {"name": "MockSpecializedBO", "type": "specialized"}, result["headers"]
        )

    async def test_105_business_values_as_dict_with_parent(self):
        nh = NavigationHeaders(index="mock_bo")
        result = await nh.business_values_as_dict()
        self.assertEqual(result["headers"], [])

    async def test_106_business_values_as_dict_with_referers(self):

        # Now test with a parent that has referers
        nh_with_referers = NavigationHeaders(index="mock_referer_bo")
        self.assertEqual(nh_with_referers._parent_bo, MockRefererBO)
        result = await nh_with_referers.business_values_as_dict()
        self.assertIn("headers", result)
        self.assertIn({"name": "MockGenericBO", "type": "referer"}, result["headers"])
