"""Data objects containing the data of a business object (BO). Unique per BO instance"""

from core.app_logging import getLogger, log_exit, VERBOSE_DEBUG

LOG = getLogger(__name__)

from typing import Any, Type, TypeVar

from business_objects.bo_descriptors import BOBaseBase, _PersistentAttr

T = TypeVar("T")


class BOData:
    """Data object containing the data of a business object (BO). Unique per BO instance"""

    def __init__(self, object: Type[BOBaseBase], id: int | None = None):
        """Initialize the BOData object with the given BO class. If an ID is provided, it is set for any attribute named 'id'."""
        self._object = object
        self._data: dict[str, Any] = {}
        attributes = object.attributes_as_dict()
        for attr_name in attributes:
            LOG.log(
                VERBOSE_DEBUG,
                f"Initializing attribute '{attr_name}' of type {attributes[attr_name]}",
            )
            if attr_name == "id" and id is not None:
                self._set_raw_data(attr_name, id)
                object._loaded_instances.add(self)  # type: ignore
            else:
                self._data[attr_name] = None

        LOG.log(
            VERBOSE_DEBUG,
            f"BOData: Initialized data for {object.__name__}: {self._data}",
        )

    def __setitem__(self, name: str, value) -> None:
        self._set_raw_data(name, value)

    def __getitem__(self, name: str) -> Any:
        return self._get_raw_data(name)

    def __contains__(self, name: str) -> bool:
        return self._data.__contains__(name)

    def __iter__(self):
        return iter(self._data)

    def __str__(self):
        return f"BOData({self._object.__name__})"

    def get_data(self, bo_descriptor) -> Any:
        """Get the value of the attribute described by 'bo_descriptor'"""
        return self._get_raw_data(bo_descriptor.my_name)

    def _get_raw_data(self, name: str) -> Any:
        return self._data.get(name)

    def set_data(self, bo_descriptor: _PersistentAttr[T], value: T | None) -> None:
        """Set the value of the attribute described by 'bo_descriptor'"""
        if bo_descriptor.my_name is None:
            raise ValueError(
                f"Attribute descriptor {bo_descriptor} has no name assigned"
            )
        self._set_raw_data(bo_descriptor.my_name, value)

    def _set_raw_data(self, name: str, value) -> None:
        self._data[name] = value

    def items(self):
        """Return an iterator over the attribute names and their values"""
        return self._data.items()


log_exit(LOG)
