"""BO for editing the configuration.
This is a proxy for the configuration read from the DB and the config file and allows their modifications.
"""

import re

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from business_objects.transient_business_object import TransientBusinessObject
from business_objects.bo_descriptors import BODict, BORelation, AttributeDescription
from data.management.configuration import Configuration
from data.management.user import User

FILE_CONFIG_ID = -1
CMDLINE_CONFIG_ID = -2


class EditConfig(TransientBusinessObject):
    """Represents a configuration object."""

    user_id = BORelation(User)
    configuration = BODict()

    def __new__(cls, *args, **kwargs):
        LOG.debug(
            f"++++++++++++++++++++++++++++++++++++++++++++++++++EditConfig.__new__({args=}, {kwargs=})"
        )
        index = kwargs.get("index")
        bo_id = kwargs.get("bo_id")
        if index is None and bo_id is not None:
            index = bo_id
        if isinstance(index, int) and index > 0:
            if bo_id is not None and bo_id != index:
                raise ValueError(
                    f"EditConfig.__new__: bo_id in kwargs ({kwargs['bo_id']}) does not match index ({index})"
                )
            return Configuration(bo_id=index)
        return super().__new__(cls)

    def __init__(self, index, **kwargs) -> None:
        LOG.debug(
            f"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXEditConfig.__init__({index=}, {kwargs=})"
        )
        if index not in [CMDLINE_CONFIG_ID, FILE_CONFIG_ID]:
            raise ValueError(f"Invalid config source for EditConfig: {index}")
        kwargs["bo_id"] = index
        super().__init__(**kwargs)

    @property
    def display_name(self) -> str:
        """A human-readable name for this business object instance, used in the frontend."""
        if self.id == CMDLINE_CONFIG_ID:
            return "Commandline Configuration"
        elif self.id == FILE_CONFIG_ID:
            return "File Configuration"
        return super().display_name

    @classmethod
    def navigation_header(
        cls, ref: AttributeDescription | str | None = None
    ) -> dict[str, str] | None:
        """Navigation header for configuration editing"""
        return {"name": "editconfig", "display_name": "Configuration"}

    @classmethod
    async def get_matching_objects(
        cls, conditions: dict | None = None, attributes: list[str] | None = None
    ) -> list["BOBase"]:
        """Get the business objects matching the conditions"""
        LOG.debug(
            f"EditConfig.get_matching_objects: conditions={conditions}, attributes={attributes}"
        )
        config_objs = await Configuration.get_matching_objects(
            attributes=Configuration.display_name_components() or ["name"]
        )
        config_objs.append(EditConfig(index=FILE_CONFIG_ID))
        LOG.debug(f"- get_matching_objects returning: {config_objs}")
        return config_objs


log_exit(LOG)
