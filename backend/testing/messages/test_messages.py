""" Test suite for Message base class. """

import unittest

from messages.message import Message, MessageAttribute


class TestMessage(unittest.IsolatedAsyncioTestCase):
    def test_001_message_from_empty_json(self):
        message = Message("")
        self.assertIsNone(message.message[MessageAttribute.WS_ATTR_TYPE])
        self.assertIsNone(message.message[MessageAttribute.WS_ATTR_TOKEN])
        self.assertIsNone(message.message_type())
        self.assertIsNone(message.token)

    def test_002_message_from_json_without_type(self):
        message = Message('{"text": "message without type attribute"}')
        self.assertIsNone(message.message[MessageAttribute.WS_ATTR_TYPE])
        self.assertIsNone(message.message[MessageAttribute.WS_ATTR_TOKEN])
        self.assertIsNone(message.message_type())
        self.assertIsNone(message.token)

    def test_002_message_from_json_with_unknown_type(self):
        message = Message('{"type": "unknownTestType", "text": "some attribute"}')
        self.assertIsNone(message.message[MessageAttribute.WS_ATTR_TYPE])
        self.assertIsNone(message.message[MessageAttribute.WS_ATTR_TOKEN])
        self.assertIsNone(message.message_type())
        self.assertIsNone(message.token)
