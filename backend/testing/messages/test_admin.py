"""Tests for admin websocket log message handling."""

import logging
import unittest
from json import dumps
from unittest.mock import patch

from core.app_logging import FrontendLineNumberFilter
from messages.admin import LogLevel, LogMessage
from messages.message import MessageAttribute, MessageType


class Test_120_AdminMessages(unittest.IsolatedAsyncioTestCase):
    async def test_121_log_message_forwards_line_number_as_extra(self):
        msg = LogMessage(
            dumps(
                {
                    MessageAttribute.WS_ATTR_TYPE: MessageType.WS_TYPE_LOG,
                    MessageAttribute.WS_ATTR_LOGLEVEL: LogLevel.LOG_LEVEL_INFO,
                    MessageAttribute.WS_ATTR_MESSAGE: "frontend info",
                    MessageAttribute.WS_ATTR_CALLER: "app.component.ts",
                    MessageAttribute.WS_ATTR_LINE_NUMBER: 257,
                }
            )
        )

        logger = logging.getLogger("test.frontend")
        with patch("messages.admin.getLogger", return_value=logger), patch.object(
            logger, "log"
        ) as mock_log:
            await msg.handle_message(connection=None)

        mock_log.assert_called_once_with(logging.INFO, "frontend info", extra={"line_number": 257})

    async def test_122_log_message_without_line_number_logs_without_extra(self):
        msg = LogMessage(
            dumps(
                {
                    MessageAttribute.WS_ATTR_TYPE: MessageType.WS_TYPE_LOG,
                    MessageAttribute.WS_ATTR_LOGLEVEL: LogLevel.LOG_LEVEL_WARNING,
                    MessageAttribute.WS_ATTR_MESSAGE: "frontend warning",
                    MessageAttribute.WS_ATTR_CALLER: "app.component.ts",
                }
            )
        )

        logger = logging.getLogger("test.frontend")
        with patch("messages.admin.getLogger", return_value=logger), patch.object(
            logger, "log"
        ) as mock_log:
            await msg.handle_message(connection=None)

        mock_log.assert_called_once_with(logging.WARNING, "frontend warning")

    def test_123_frontend_linenumber_filter_overrides_record_lineno(self):
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=10,
            msg="msg",
            args=(),
            exc_info=None,
        )
        record.line_number = "345"

        result = FrontendLineNumberFilter().filter(record)

        self.assertTrue(result)
        self.assertEqual(record.lineno, 345)

    def test_124_frontend_linenumber_filter_ignores_invalid_values(self):
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname=__file__,
            lineno=22,
            msg="msg",
            args=(),
            exc_info=None,
        )
        record.line_number = "not-a-number"

        result = FrontendLineNumberFilter().filter(record)

        self.assertTrue(result)
        self.assertEqual(record.lineno, 22)
