import unittest

from business_objects.business_object_base import BOBase
from data.management.configuration import Configuration
from data.management.user import User
from messages.object_schema import ObjectSchema


class MockBOBase(BOBase):

    def __new__(cls, id: int | None = None, *args, **attributes):
        print(f"MockBOBase.__new__({cls=}, {id=}, {args=}, {attributes=})")
        return super().__new__(cls)


class Test_100__ObjectSchema(unittest.TestCase):

    def test101_initialization(self):
        test_type = Configuration
        obj = ObjectSchema(object_type=test_type)
        print(obj.message)
        self.assertIs(test_type, obj._object_type)

    def test102_payload_generation(self):
        test_type = Configuration
        obj = ObjectSchema(object_type=test_type)
        payload = obj.generate_payload()
        print(f"Generated payload: {payload=}")
