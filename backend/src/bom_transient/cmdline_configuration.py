"""A representation of the configuration read from the command line."""

from typing import Optional, Self
import pprint

from core.app_logging import getLogger, log_exit, redact, VERBOSE_DEBUG

LOG = getLogger(__name__)

from core.const import CMDLINE_CONFIG_BOID
from core.base_objects import ConfigDict
from core.configuration.cmd_line import CommandLine
from core.reconfigure_logging import reconfigure_logging
from business_objects.transient_business_object import TransientBusinessObject
from business_objects.bo_descriptors import BODict


class CmdlineConfiguration(TransientBusinessObject):
    """Represents the configuration read from the command line."""

    singleton_instance: Optional[Self] = None

    configuration = BODict()

    def __new__(cls, *args, **kwargs):
        if cls.singleton_instance is not None:
            return cls.singleton_instance
        instance = super().__new__(cls)
        cls.singleton_instance = instance
        return instance

    def __init__(self, **kwargs) -> None:
        LOG.debug(f"CmdlineConfiguration.__init__({kwargs=})")
        kwargs["bo_id"] = CMDLINE_CONFIG_BOID
        super().__init__(**kwargs)
        self.configuration = CommandLine.get_commandline_config()

    @property
    def display_name(self) -> str:
        """A human-readable name for this business object instance, used in the frontend."""
        return "Commandline Configuration"

    async def store(self):
        """User changed the commandline configuration. Adapt the configuration for this run."""
        LOG.debug("CmdlineConfiguration.store(): Storing commandline configuration.")
        if LOG.isEnabledFor(VERBOSE_DEBUG):
            for line in pprint.pformat(
                redact(self.configuration), indent=4, width=120, compact=True
            ).splitlines():
                LOG.debug(f" - {line}")
        CommandLine.set_commandline_config(self.configuration)
        await super().store()


log_exit(LOG)
