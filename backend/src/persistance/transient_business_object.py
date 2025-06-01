"""Base class for transient Business Objects

Transient Business Objects are not stored in the database, but are used to
temporarily hold data during processing or for communication with the frontend."""

from typing import Optional
import weakref
from persistance.business_object_base import BOBase


class TransientBusinessObject(BOBase):
    """Transient Business Objects base class.
    Because they are not stored in the database, the class itself
    handles object storage and retrieval. Transient objects will be
    destroyed when the application is closed.
    """

    _instances: weakref.WeakSet["TransientBusinessObject"] = weakref.WeakSet()

    # When creating a new instance, add it to the list of instances
    def __init__(self, id=None, **attributes) -> None:
        super().__init__(id=id, **attributes)
        self._instances.add(self)

    @classmethod
    async def count_rows(cls, conditions: Optional[dict] = None) -> int:
        # TODO: implement conditions
        return len(cls._instances)

    @classmethod
    async def get_matching_ids(cls, conditions: dict | None = None) -> list[int]:
        return [bo.id for bo in cls._instances if isinstance(bo.id, int)]
