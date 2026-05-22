"""Configuration read from config file"""

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.transient_business_object import TransientBusinessObject


class ConfigFile(TransientBusinessObject):
    """Represents the configuration read from the config file. This is a transient business object, as it is not stored in the database and does not have an id."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


log_exit(LOG)
