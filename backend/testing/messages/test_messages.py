""" Test suite for Message base class. """

import unittest

from messages.message import Message, MessageType, MessageAttribute


class TestMessage(unittest.IsolatedAsyncioTestCase):
    def test_001_message_from_empty_json(self):
        message = Message("")
        self.assertIs(
            message.message[MessageAttribute.WS_ATTR_TYPE], MessageType.WS_TYPE_NONE
        )
        self.assertIsNone(message.message[MessageAttribute.WS_ATTR_TOKEN])
        self.assertIs(message.message_type(), MessageType.WS_TYPE_NONE)
        self.assertIsNone(message.token)

    def test_002_message_from_json_without_type(self):
        message = Message('{"text": "message without type attribute"}')
        self.assertIs(
            message.message[MessageAttribute.WS_ATTR_TYPE], MessageType.WS_TYPE_NONE
        )
        self.assertIs(message.message_type(), MessageType.WS_TYPE_NONE)

    def test_002_message_from_json_with_unknown_type(self):
        unknown_test_type = "unknownTestType"
        message = Message(
            '{"type": "' + unknown_test_type + '", "text": "some attribute"}'
        )
        self.assertEqual(
            message.message[MessageAttribute.WS_ATTR_TYPE], unknown_test_type
        )
        self.assertIs(message.message_type(), MessageType.WS_TYPE_NONE)
