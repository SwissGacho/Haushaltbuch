"""Test suite for transient BO list objects."""

from typing import Optional
import unittest
from unittest.mock import Mock, patch

from business_objects.business_object_base import BOBase
from bom_transient.navigation_headers import NavigationHeaders
from core.util import _classproperty


class MockBOBase(BOBase):
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
            "MockRootBO": MockRootBO,
            "MockNonRootBO": MockNonRootBO,
        }


class MockRootBO(MockBOBase):
    is_root_bo = True

    @classmethod
    def navigation_header(cls, ref=None):
        if ref:
            return {"name": "MockRootBO", "type": "referer"}
        return {"name": "MockRootBO", "type": "root"}


class MockNonRootBO(MockBOBase):
    is_root_bo = False

    @classmethod
    def navigation_header(cls, ref=None):
        if ref:
            return {"name": "MockNonRootBO", "type": "referer"}
        return {"name": "MockNonRootBO", "type": "non-root"}


class MockRefererBO(MockBOBase):
    is_root_bo = False

    @classmethod
    def navigation_header(cls, ref=None):
        return {"name": "MockRefererBO", "type": "referer"}

    @classmethod
    def referenced_by(cls):
        return [(MockRootBO, "attribute1"), (MockNonRootBO, "attribute2")]


class Test_100_NavigationHeaders(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.mock_get_business_object_by_name = Mock(
            name="get_business_object_by_name",
            side_effect=lambda name: {
                "mock_bo": MockRootBO,
                "mock_referer_bo": MockRefererBO,
                "mock_non_root_bo": MockNonRootBO,
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
        self.assertEqual(nh._parent_bo, MockRootBO)
        self.mock_get_business_object_by_name.assert_called_once_with("mock_bo")

    async def test_102_initialization_with_index_None(self):
        nh = NavigationHeaders(index=None)
        self.assertIsNone(nh._parent_bo)

    async def test_103_initialization_rejects_non_string_index(self):
        with self.assertRaises(TypeError):
            NavigationHeaders(index=123)

    async def test_104_business_values_as_dict_no_parent(self):
        # Patch a root and a non-root BO to test the behavior of business_values_as_dict when no parent is provided

        nh = NavigationHeaders(index=None)
        result = await nh.business_values_as_dict()
        self.assertIn("headers", result)
        self.assertIsInstance(result["headers"], list)
        self.assertIn({"name": "MockRootBO", "type": "root"}, result["headers"])
        self.assertNotIn(
            {"name": "MockNonRootBO", "type": "non-root"}, result["headers"]
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
        self.assertIn({"name": "MockRootBO", "type": "referer"}, result["headers"])
