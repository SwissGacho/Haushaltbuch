"""Data objects containing the data of a business object (BO). Unique per BO instance"""

from core.app_logging import getLogger, log_exit, ERROR, VERBOSE_DEBUG

LOG = getLogger(__name__)

from typing import Any, Type

from business_objects.bo_descriptors import BOBaseBase


class BOData:
    """Data object containing the data of a business object (BO). Unique per BO instance"""

    def __init__(self, object: Type[BOBaseBase]):
        self._object = object
        self._data: dict[str, Any] = {}
        attributes = object.attributes_as_dict()
        for attr_name in attributes:
            self._data[attr_name] = None
        LOG.log(
            VERBOSE_DEBUG,
            f"BOData: Initialized data for {object.__name__}: {self._data}",
        )

    def __setitem__(self, name: str, value) -> None:
        self.set_raw_data(name, value)

    def __getitem__(self, name: str) -> Any:
        return self._get_raw_data(name)

    def __contains__(self, name: str) -> bool:
        return self._data.__contains__(name)

    def __iter__(self):
        return iter(self._data)

    def get_data(self, bo_descriptor) -> Any:
        """Get the value of the attribute described by 'bo_descriptor'"""
        return self._get_raw_data(bo_descriptor.my_name)

    def _get_raw_data(self, name: str) -> Any:
        return self._data.get(name)

    def set_data(self, bo_descriptor, value) -> None:
        """Set the value of the attribute described by 'bo_descriptor'"""
        self.set_raw_data(bo_descriptor.my_name, value)

    def set_raw_data(self, name: str, value) -> None:
        self._data[name] = value

    def items(self):
        """Return an iterator over the attribute names and their values"""
        return self._data.items()


log_exit(LOG)
