"""BO for editing the configuration.
This is a proxy for the configuration read from the DB and the config file and allows their modifications.
"""

from core.app_logging import getLogger, log_exit, VERBOSE_DEBUG

LOG = getLogger(__name__)

from core.const import FILE_CONFIG_BOID, CMDLINE_CONFIG_BOID
from business_objects.transient_business_object import TransientBusinessObject
from business_objects.bo_descriptors import (
    AttributeDescription,
    BODict,
    BORelation,
    BOStr,
)
from bom_persistent.management.configuration import CommonConfiguration, Configuration
from bom_persistent.management.user import User
from bom_transient.cmdline_configuration import CmdlineConfiguration
from bom_transient.file_configuration import FileConfiguration


class EditConfig(TransientBusinessObject):
    """Represents a configuration object."""

    user_id = BORelation(User)
    file_path = BOStr()
    configuration = BODict()

    def __new__(cls, *args, **kwargs):
        index = kwargs.get("index")
        bo_id = kwargs.get("bo_id")
        if index is None and bo_id is not None:
            index = bo_id
        if isinstance(index, int):
            if bo_id is not None and bo_id != index:
                raise ValueError(
                    f"EditConfig.__new__: bo_id in kwargs ({kwargs['bo_id']}) "
                    f"does not match index ({index})"
                )
            if index >= 0:  # A persistent config was requested by index
                return Configuration(bo_id=index)
            elif index == CMDLINE_CONFIG_BOID:
                return CmdlineConfiguration(bo_id=CMDLINE_CONFIG_BOID)
            elif index == FILE_CONFIG_BOID:
                return FileConfiguration(bo_id=FILE_CONFIG_BOID)
        return super().__new__(cls)

    def __init__(self, index, **kwargs) -> None:
        LOG.debug(f"EditConfig.__init__({index=}, {kwargs=})")
        if index not in (CMDLINE_CONFIG_BOID, FILE_CONFIG_BOID):
            raise ValueError(f"Invalid config source for EditConfig: {index}")
        kwargs["bo_id"] = index
        super().__init__(**kwargs)

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
            f"EditConfig.get_matching_objects: conditions={conditions}, attributes={attributes} <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
        )
        if conditions or (attributes and attributes != ["name"]):
            LOG.warning(
                f"EditConfig.get_matching_objects() cannot be called with {conditions=} or {attributes=}"
            )
        config_objs = await Configuration.get_matching_objects(
            attributes=Configuration.display_name_components() or ["name"]
        )
        config_objs.append(EditConfig(index=FILE_CONFIG_BOID))
        config_objs.append(EditConfig(index=CMDLINE_CONFIG_BOID))
        LOG.debug(f"- get_matching_objects returning {len(config_objs)} objects")
        if LOG.isEnabledFor(VERBOSE_DEBUG):
            for obj in config_objs:
                LOG.debug(f"  - {str(obj)}")
        return config_objs


log_exit(LOG)
