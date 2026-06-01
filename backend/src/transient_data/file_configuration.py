"""A representation of the configuration read from the config json file."""

import pprint
from typing import Optional, Self

from core.app_logging import getLogger, log_exit, redact, VERBOSE_DEBUG

LOG = getLogger(__name__)

from core.const import FILE_CONFIG_BOID
from core.base_objects import ConfigDict
from core.configuration.file_config import FileConfig
from business_objects.transient_business_object import TransientBusinessObject
from business_objects.bo_descriptors import BODict, BOStr


class FileConfiguration(TransientBusinessObject):
    """Represents the configuration read from the config file."""

    singleton_instance: Optional[Self] = None

    configuration = BODict()
    file_path = BOStr()

    def __new__(cls, *args, **kwargs):
        if cls.singleton_instance is not None:
            return cls.singleton_instance
        instance = super().__new__(cls)
        cls.singleton_instance = instance
        return instance

    def __init__(self, cmdline_config: Optional[ConfigDict] = None, **kwargs) -> None:
        LOG.debug(f"FileConfiguration.__init__(cmdline_config, {kwargs=})")
        if LOG.isEnabledFor(VERBOSE_DEBUG):
            for line in pprint.pformat(
                redact(cmdline_config), indent=4, width=120, compact=True
            ).splitlines():
                LOG.log(VERBOSE_DEBUG, f" - {line}")
        kwargs["bo_id"] = FILE_CONFIG_BOID
        super().__init__(**kwargs)
        self.configuration = FileConfig.read_file_config_file()
        self.file_path = str(FileConfig.file_config_file_path or "")

    @property
    def display_name(self) -> str:
        """A human-readable name for this business object instance, used in the frontend."""
        return "File Configuration"

    async def store(self):
        """User changed the config file configuration. Adapt the configuration for this run."""
        LOG.debug("FileConfiguration.store(): Storing configuration file.")
        if self.file_path != str(FileConfig.file_config_file_path):
            LOG.warning(
                f"FileConfiguration.store: File path changed from {FileConfig.file_config_file_path} to {self.file_path}. "
                f"However, changing the config file path is not supported. Ignoring the new file path."
            )
        if LOG.isEnabledFor(VERBOSE_DEBUG):
            for line in pprint.pformat(
                redact(self.configuration), indent=4, width=120, compact=True
            ).splitlines():
                LOG.debug(f" - {line}")
        FileConfig.write_file_config_file(self.configuration or {})
        await super().store()


log_exit(LOG)
