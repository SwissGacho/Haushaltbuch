"""Test suit for business object list attributes."""

import unittest
from unittest.mock import Mock, patch
from business_objects.bo_list import BOList
from business_objects.business_object_base import BOBase


class MockBOBase(BOBase):

    def __new__(cls, id: int | None = None, *args, **attributes):
        print(f"MockBOBase.__new__({cls=}, {id=}, {args=}, {attributes=})")
        return super().__new__(cls)


class MockConnection:
    pass


class Test_100__BOList(unittest.IsolatedAsyncioTestCase):

    def setUp(self) -> None:
        self.patcher = patch("business_objects.business_object_base.BOBase", MockBOBase)
        # self.conPatcher = patch("server.ws_connection.WS_Connection", MockConnection)
        # self.conPatcher.start()
        self.patcher.start()

    def tearDown(self):
        # self.conPatcher.stop()
        self.patcher.stop()

    @unittest.skip("Not implemented yet")
    async def test_101_initialization(self):
        con = Mock()
        boList = BOList(bo_type=MockBOBase, connection=con)
        self.assertEqual(boList._bo_type, MockBOBase)
