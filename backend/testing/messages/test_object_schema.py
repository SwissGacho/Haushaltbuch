import unittest

from messages.object_schema import ObjectSchema
from data.management.user import User
from business_objects.business_attribute_base import BaseFlag
from business_objects.business_object_base import BOBase
from business_objects.bo_descriptors import (
    AttributeType,
    BOFlag,
    BOStr,
    BOList,
    BORelation,
)


class OtherMockBO(BOBase):
    pass


class MockBOFlag(BaseFlag):
    FLAG_1 = 1
    FLAG_2 = 2


class MockBO(BOBase):
    def __new__(cls, id: int | None = None, *args, **attributes):
        print(f"MockBOBase.__new__({cls=}, {id=}, {args=}, {attributes=})")
        return super().__new__(cls)

    # Really should mock attribute_descriptions instead...
    mock_str = BOStr()
    mock_list = BOList()
    mock_relation = BORelation(OtherMockBO)
    mock_flag = BOFlag(MockBOFlag)


class MockAttributeDescription:
    def __init__(self, name, attribute_type, flag_values, is_technical=False):
        self.name = name
        self.attribute_type = attribute_type
        self.flag_values = flag_values
        self.is_technical = is_technical


class Test_100__ObjectSchema(unittest.TestCase):

    def bo_payload_test(self, obj_type: type[BOBase]):
        obj = ObjectSchema(object_type=obj_type)
        payload = obj.generate_payload()
        for v in obj_type.attribute_descriptions():
            if v.is_technical:
                self.assertNotIn(v.name, payload)
                continue
            self.assertEqual(
                {
                    "type": obj.attribute_type_representation(v.attribute_type),
                    "flags": {
                        k: (obj.flag_values_representation(val))
                        for k, val in v.flag_values.items()
                    },
                },
                payload[v.name],
            )

    def test101_initialization(self):
        test_type = MockBO
        obj = ObjectSchema(object_type=test_type)
        self.assertIs(test_type, obj._object_type)

    def test102_payload_generation(self):
        self.bo_payload_test(MockBO)
        self.bo_payload_test(User)

    def test_103_flag_representation(self):
        test_type = MockBO
        obj = ObjectSchema(object_type=test_type)
        self.assertEqual("test", obj.flag_values_representation("test"))
        self.assertEqual(MockBO.bo_type_name(), obj.flag_values_representation(MockBO))
        self.assertEqual(
            {"name": MockBOFlag.__name__, "values": [str(v) for v in MockBOFlag]},
            obj.flag_values_representation(MockBOFlag),
        )

    def test_104_attribute_type_representation(self):
        test_type = MockBO
        obj = ObjectSchema(object_type=test_type)
        MockBOAttributeNone = MockAttributeDescription(
            "mock_none", AttributeType.ATYPE_INT, {}
        )
        self.assertEqual(
            obj.attribute_type_representation(MockBOAttributeNone.attribute_type), "int"
        )
        for v in test_type.attribute_descriptions():
            target_type = v.attribute_type.value if v.attribute_type else None
            print(
                f"Testing attribute_type_representation for {v.name=}:"
                f" {v.attribute_type=}, {target_type=}"
            )
            self.assertEqual(
                target_type,
                obj.attribute_type_representation(v.attribute_type),
            )
