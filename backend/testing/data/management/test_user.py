""" Testsuite for persistant business object User """

import unittest
from unittest.mock import Mock

import data.management.user


class TestUser(unittest.IsolatedAsyncioTestCase):
    def test_001_user(self):
        user = data.app.user.User("Mock")
        self.assertIsNone(user.id)
        user.id = 1
        self.assertEqual(user.id, 1)
        self.assertEqual(user.name, "Mock")
        print(f">>{user.sql_create_table()=}")
