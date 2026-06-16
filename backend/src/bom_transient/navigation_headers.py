"""NavigationHeader provides a list of object categories depending on a parent.
- If the parent is empty, it provides the list of root categories (i.e. all business objects).
- If the parent is a business object instance, it provides the list of business objects
    that are related to the parent object (i.e. all business objects that have a reference to the parent object).
"""

import pprint
from typing import Any

from core.app_logging import getLogger, log_exit, VERBOSE_DEBUG

LOG = getLogger(__name__)

from business_objects.business_object_base import BOBase
from business_objects.transient_business_object import TransientBusinessObject


class NavigationHeaders(TransientBusinessObject):
    """Represents a list of business objects of a certain type."""

    def __init__(self, index: str, conditions: dict | None = None, **kwargs) -> None:
        LOG.debug(f"NavigationHeaders.__init__({index=}, {kwargs=})")
        if index and not isinstance(index, str):
            raise TypeError(
                f"NavigationHeaders.__init__: index must be a string, got {type(index)}"
            )
        self._parent_bo: type[BOBase] | None = (
            BOBase.get_business_object_by_name(index) if index else None
        )
        super().__init__(**kwargs)

    async def business_values_as_dict(self) -> dict[str, Any]:
        LOG.debug(
            f"{str(self)}.business_values_as_dict: parent "
            f"BO={self._parent_bo.__name__ if self._parent_bo else None}"
        )
        if self._parent_bo:
            navigation_list = [
                referer.navigation_header(ref=attribute)
                for referer, attribute in self._parent_bo.referenced_by()
            ]
        else:
            navigation_list = [
                o.navigation_header()
                for o in BOBase.all_business_objects.values()  # pylint: disable=no-member
                if issubclass(o, PersistentBusinessObject) and o.is_root_bo
            ]
        navigation_list = [item for item in navigation_list if item is not None]
        if LOG.isEnabledFor(VERBOSE_DEBUG):
            LOG.log(
                VERBOSE_DEBUG,
                f"Navigationlist for parent BO "
                f"{self._parent_bo.__name__ if self._parent_bo else None} "
                f"with {len(navigation_list)} BOs:",
            )
            for item in pprint.pformat(
                navigation_list, indent=4, width=120, compact=True
            ).splitlines():
                LOG.log(VERBOSE_DEBUG, f" - {item}")
        else:
            LOG.debug(
                f"Navigationlist for parent BO "
                f"{self._parent_bo.__name__ if self._parent_bo else None}: "
                f"{len(navigation_list)} BOs."
            )
        return {"headers": navigation_list}


log_exit(LOG)
