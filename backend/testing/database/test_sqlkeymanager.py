import unittest
from unittest.mock import Mock, AsyncMock
from unittest.mock import patch

from database.sqlkeymanager import SQLKeyManager


class Test_100_SQLKeyManager(unittest.TestCase):

    def setUp(self):
        self.sql_key_manager = SQLKeyManager()

    def test_101_InitDefaults(self):
        self.assertEqual(self.sql_key_manager.params, {})
        self.assertEqual(self.sql_key_manager._last_key, 0)
        self.assertEqual(self.sql_key_manager._keys, set())
        self.assertEqual(self.sql_key_manager._aliases, {})

    def test_102_merge_single_params(self):
        # Test with a simple QUERYPART and params
        self.assertEqual(
            self.sql_key_manager.merge_params("QUERYPART :param", {"param": 1}),
            "QUERYPART :param",
        )
        self.assertEqual(self.sql_key_manager.params, {"param": 1})

    def test_103_merge_2_single_params(self):
        # Test with 2 simple QUERYPARTs and params
        query_part1 = "QUERYPART :param"
        query_part2 = " OTHER :param"
        query = self.sql_key_manager.merge_params(query_part1, {"param": 1})
        query += self.sql_key_manager.merge_params(query_part2, {"param": 2})
        self.assertEqual(query, "QUERYPART :param OTHER :param1")
        self.assertEqual(self.sql_key_manager.params, {"param": 1, "param1": 2})

    def test_104_merge_2_single_params_same_value(self):
        # Test with 2 simple QUERYPARTs and identical params
        query_part1 = "QUERYPART :param"
        query_part2 = " OTHER :param"
        query = self.sql_key_manager.merge_params(query_part1, {"param": 1})
        query += self.sql_key_manager.merge_params(query_part2, {"param": 1})
        self.assertEqual(query, "QUERYPART :param OTHER :param")
        self.assertEqual(self.sql_key_manager.params, {"param": 1})

    def test_105_merge_multiple_params_nonoverlapping(self):
        # Test with multiple params that do not overlap
        query_part1 = "QUERYPART :param1 :param2"
        query_part2 = " OTHER :param3, :param4"
        query = self.sql_key_manager.merge_params(
            query_part1, {"param1": 1, "param2": 2}
        )
        query += self.sql_key_manager.merge_params(
            query_part2, {"param3": 3, "param4": 4}
        )
        self.assertEqual(query, "QUERYPART :param1 :param2 OTHER :param3, :param4")
        self.assertEqual(
            self.sql_key_manager.params,
            {"param1": 1, "param2": 2, "param3": 3, "param4": 4},
        )

    def test_106_merge_multiple_params_overlapping(self):
        # Test with multiple params that overlap
        query_part1 = "QUERYPART :param1 :param2"
        query_part2 = " OTHER :param1, :param3"
        query = self.sql_key_manager.merge_params(
            query_part1, {"param1": 1, "param2": 2}
        )
        query += self.sql_key_manager.merge_params(
            query_part2, {"param1": 3, "param3": 4}
        )
        self.assertEqual(query, "QUERYPART :param1 :param2 OTHER :param11, :param3")
        self.assertEqual(
            self.sql_key_manager.params,
            {"param1": 1, "param2": 2, "param11": 3, "param3": 4},
        )

    def test_107_merge_multiple_params_overlapping_matching_values(self):
        # Test with multiple params that overlap
        query_part1 = "QUERYPART :param1 :param2"
        query_part2 = " OTHER :param1, :param3"
        query = self.sql_key_manager.merge_params(
            query_part1, {"param1": 1, "param2": 2}
        )
        query += self.sql_key_manager.merge_params(
            query_part2, {"param1": 1, "param3": 4}
        )
        self.assertEqual(query, "QUERYPART :param1 :param2 OTHER :param1, :param3")
        self.assertEqual(
            self.sql_key_manager.params,
            {"param1": 1, "param2": 2, "param3": 4},
        )
