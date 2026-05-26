"""Testsuite for persistant business object User"""

import unittest
from unittest.mock import Mock

import data_persistent.management.user


class TestUser(unittest.IsolatedAsyncioTestCase):
    async def test_001_user(self):
        user = data_persistent.management.user.User(name="Mock")
        self.assertIsNone(user.id)
        user.id = 1
        self.assertEqual(user.id, 1)
        self.assertEqual(user.name, "Mock")
