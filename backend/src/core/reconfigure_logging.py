"""Module to reconfigure logging for the application after configuration is loaded."""

from core.app_logging import getLogger, log_exit, configure_logging

LOG = getLogger(__name__)
from core.app import App


def reconfigure_logging():
    "Configure logging level for the application"
    configure_logging(App.configuration)


log_exit(LOG)
