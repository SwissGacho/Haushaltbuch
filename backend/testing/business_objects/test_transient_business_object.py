"""Test suit for the TransientBusinessObject class."""

import unittest
import gc
from unittest.mock import patch

from business_objects.transient_business_object import TransientBusinessObject


class MockBOBase:
    """Mock class for BOBase to simulate inheritance."""

    def __init__(self, id=None, **attributes):
        self.id = id
        self.attributes = attributes


class Test_100_Transient_Business_Object_classmethods(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.patcher = patch(
            "business_objects.transient_business_object.BOBase", MockBOBase
        )
        self.patcher.start()

    def tearDown(self):
        self.patcher.stop()

    async def test_101_instance_creation(self):
        """Test that instances are created and added to the weakref set."""
        TransientBusinessObject._instances.clear()
        obj1 = TransientBusinessObject(bo_id=1)
        obj2 = TransientBusinessObject(bo_id=2)

        self.assertIsNotNone(obj1.id)
        self.assertNotEqual(obj2.id, obj1.id)

        self.assertEqual(len(TransientBusinessObject._instances), 2)
        self.assertIn(obj1, TransientBusinessObject._instances)
        self.assertIn(obj2, TransientBusinessObject._instances)

        obj1 = None
        gc.collect()

        self.assertEqual(len(TransientBusinessObject._instances), 1)

    async def test_102_count_rows(self):
        """Test the count_rows method."""
        TransientBusinessObject._instances.clear()
        # Must keep reference, else it gets garbage collected
        bo1 = TransientBusinessObject(bo_id=1)
        bo2 = TransientBusinessObject(bo_id=2)

        count = await TransientBusinessObject.count_rows()

        self.assertEqual(count, 2)

    async def test_103_get_matching_ids(self):
        """Test the get_matching_ids method."""
        TransientBusinessObject._instances.clear()
        bo1 = TransientBusinessObject(bo_id=1)
        bo2 = TransientBusinessObject(bo_id=2)

        matching_ids = sorted((await TransientBusinessObject.get_matching_ids()))
        self.assertEqual(matching_ids, [1, 2])
