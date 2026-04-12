"""Test suite for Message base class."""

import unittest
from json import dumps, loads
from unittest.mock import AsyncMock, MagicMock

from messages.message import Message, MessageType, MessageAttribute


class Test_100_Message(unittest.IsolatedAsyncioTestCase):
    def test_101_message_from_empty_json(self):
        message = Message("")
        self.assertIs(
            message.message[MessageAttribute.WS_ATTR_TYPE], MessageType.WS_TYPE_NONE
        )
        self.assertIsNone(message.message[MessageAttribute.WS_ATTR_TOKEN])
        self.assertIs(message.message_type(), MessageType.WS_TYPE_NONE)
        self.assertIsNone(message.token)

    def test_102_message_from_json_without_type(self):
        message = Message('{"text": "message without type attribute"}')
        self.assertIs(
            message.message[MessageAttribute.WS_ATTR_TYPE], MessageType.WS_TYPE_NONE
        )
        self.assertIs(message.message_type(), MessageType.WS_TYPE_NONE)

    def test_103_message_from_json_with_unknown_type(self):
        unknown_test_type = "unknownTestType"
        message = Message(
            '{"type": "' + unknown_test_type + '", "text": "some attribute"}'
        )
        self.assertEqual(
            message.message[MessageAttribute.WS_ATTR_TYPE], unknown_test_type
        )
        self.assertIs(message.message_type(), MessageType.WS_TYPE_NONE)

    def test_104_get_attributes(self):
        message = Message(
            dumps(
                {
                    MessageAttribute.WS_ATTR_TYPE: "testType",
                    MessageAttribute.WS_ATTR_TOKEN: "mockToken",
                    MessageAttribute.WS_ATTR_INDEX: 42,
                    MessageAttribute.WS_ATTR_IS_PRIMARY: True,
                    MessageAttribute.WS_ATTR_COMPONENT: "testComponent",
                    MessageAttribute.WS_ATTR_PAYLOAD: {"key": "value"},
                }
            )
        )
        self.assertEqual(message.token, "mockToken")
        self.assertEqual(
            message.get_str(MessageAttribute.WS_ATTR_COMPONENT), "testComponent"
        )
        self.assertIsNone(message.get_str(MessageAttribute.WS_ATTR_INDEX))
        self.assertEqual(message.get_int(MessageAttribute.WS_ATTR_INDEX), 42)
        self.assertIsNone(message.get_int(MessageAttribute.WS_ATTR_COMPONENT))
        self.assertEqual(
            message.message[MessageAttribute.WS_ATTR_PAYLOAD], {"key": "value"}
        )

    def test_105_add_single_attribute(self):
        message = Message()
        message.add({MessageAttribute.WS_ATTR_PAYLOAD: {"test": "data"}})
        self.assertEqual(
            message.message[MessageAttribute.WS_ATTR_PAYLOAD], {"test": "data"}
        )

    def test_106_add_multiple_attributes(self):
        message = Message()
        message.add(
            {
                MessageAttribute.WS_ATTR_PAYLOAD: {"key": "value"},
                MessageAttribute.WS_ATTR_STATUS: "success",
                "custom_field": "custom_value",
            }
        )
        self.assertEqual(
            message.message[MessageAttribute.WS_ATTR_PAYLOAD], {"key": "value"}
        )
        self.assertEqual(message.message[MessageAttribute.WS_ATTR_STATUS], "success")
        self.assertEqual(message.message["custom_field"], "custom_value")

    def test_107_add_overwrites_existing_attributes(self):
        message = Message(dumps({MessageAttribute.WS_ATTR_PAYLOAD: {"old": "data"}}))
        message.add({MessageAttribute.WS_ATTR_PAYLOAD: {"new": "data"}})
        self.assertEqual(
            message.message[MessageAttribute.WS_ATTR_PAYLOAD], {"new": "data"}
        )

    def test_108_add_with_various_data_types(self):
        message = Message()
        message.add(
            {
                "string_field": "test string",
                "int_field": 42,
                "bool_field": True,
                "list_field": [1, 2, 3],
                "dict_field": {"nested": "value"},
                "none_field": None,
            }
        )
        self.assertEqual(message.message["string_field"], "test string")
        self.assertEqual(message.message["int_field"], 42)
        self.assertEqual(message.message["bool_field"], True)
        self.assertEqual(message.message["list_field"], [1, 2, 3])
        self.assertEqual(message.message["dict_field"], {"nested": "value"})
        self.assertIsNone(message.message["none_field"])

    async def test_109_serialize_basic_message(self):
        message = Message()
        serialized = await message.serialize()
        self.assertIsInstance(serialized, str)
        deserialized = loads(serialized)
        self.assertEqual(deserialized[MessageAttribute.WS_ATTR_TYPE], "None")
        self.assertIsNone(deserialized[MessageAttribute.WS_ATTR_TOKEN])

    async def test_110_serialize_message_with_payload(self):
        message = Message()
        message.add({MessageAttribute.WS_ATTR_PAYLOAD: {"key": "value"}})
        serialized = await message.serialize()
        deserialized = loads(serialized)
        self.assertEqual(
            deserialized[MessageAttribute.WS_ATTR_PAYLOAD], {"key": "value"}
        )

    async def test_111_serialize_message_with_multiple_attributes(self):
        message = Message()
        message.add(
            {
                MessageAttribute.WS_ATTR_PAYLOAD: {"data": "test"},
                MessageAttribute.WS_ATTR_STATUS: "pending",
                "custom": "field",
            }
        )
        serialized = await message.serialize()
        deserialized = loads(serialized)
        self.assertEqual(
            deserialized[MessageAttribute.WS_ATTR_PAYLOAD], {"data": "test"}
        )
        self.assertEqual(deserialized[MessageAttribute.WS_ATTR_STATUS], "pending")
        self.assertEqual(deserialized["custom"], "field")

    async def test_112_serialize_preserves_json_structure(self):
        original_dict = {
            MessageAttribute.WS_ATTR_TYPE: "testType",
            MessageAttribute.WS_ATTR_TOKEN: "token123",
            MessageAttribute.WS_ATTR_PAYLOAD: {"nested": {"value": 123}},
        }
        message = Message(dumps(original_dict))
        serialized = await message.serialize()
        deserialized = loads(serialized)
        self.assertEqual(deserialized[MessageAttribute.WS_ATTR_TYPE], "testType")
        self.assertEqual(deserialized[MessageAttribute.WS_ATTR_TOKEN], "token123")
        self.assertEqual(
            deserialized[MessageAttribute.WS_ATTR_PAYLOAD], {"nested": {"value": 123}}
        )

    async def test_113_serialize_with_datetime(self):
        from datetime import datetime

        message = Message()
        test_datetime = datetime(2024, 1, 15, 10, 30, 45)
        message.add({"timestamp": test_datetime})
        serialized = await message.serialize()
        deserialized = loads(serialized)
        # datetime is converted to string
        self.assertIsInstance(deserialized["timestamp"], str)
        self.assertIn("2024-01-15", deserialized["timestamp"])

    async def test_114_serialize_with_date(self):
        from datetime import date

        message = Message()
        test_date = date(2024, 1, 15)
        message.add({"date_field": test_date})
        serialized = await message.serialize()
        deserialized = loads(serialized)
        # date is converted to string
        self.assertIsInstance(deserialized["date_field"], str)
        self.assertIn("2024-01-15", deserialized["date_field"])

    async def test_115_serialize_with_object_having_fetch_method(self):
        message = Message()

        # Create a mock object with a fetch method
        mock_object = MagicMock()
        mock_object.fetch = AsyncMock(return_value=None)
        mock_object.json_encode = MagicMock(return_value={"mock": "data"})

        message.add(
            {
                MessageAttribute.WS_ATTR_PAYLOAD: {
                    "business_object": mock_object,
                    "other_data": "value",
                }
            }
        )

        serialized = await message.serialize()

        mock_object.fetch.assert_called_once_with()
        self.assertIsInstance(serialized, str)
        self.assertEqual(
            loads(serialized),
            {
                "token": None,
                "type": "None",
                MessageAttribute.WS_ATTR_PAYLOAD: {
                    "other_data": "value",
                    "business_object": {"mock": "data"},
                },
            },
        )
