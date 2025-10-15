"""Base class for transient Business Objects

Transient Business Objects are not stored in the database, but are used to
temporarily hold data during processing or for communication with the frontend."""

import itertools
from core.app_logging import getLogger
from typing import Optional
import weakref
from business_objects.business_object_base import BOBase

LOG = getLogger(__name__)


class TransientBusinessObject(BOBase):
    """Transient Business Objects base class.
    Because they are not stored in the database, the class itself
    handles object storage and retrieval. Transient objects will be
    destroyed when the application is closed.
    """

    _instances: weakref.WeakSet["TransientBusinessObject"] = weakref.WeakSet()
    _next_id = itertools.count(1)

    # When creating a new instance, add it to the list of instances
    def __init__(self, *args, bo_id=None, **attributes) -> None:
        LOG.debug(f"TransientBusinessObject.__init__({bo_id=}, {args=}, {attributes=})")
        super().__init__(bo_id=bo_id, *args, **attributes)
        if bo_id is None:
            self.id = next(self._next_id)
        LOG.debug(f"TransientBusinessObject.__init__: assigned id {self.id}")
        self._instances.add(self)

    @classmethod
    async def count_rows(cls, conditions: Optional[dict] = None) -> int:
        # TODO: implement conditions
        return len(cls._instances)

    @classmethod
    async def get_matching_ids(cls, conditions: dict | None = None) -> list[int]:
        return [bo.id for bo in cls._instances if isinstance(bo.id, int)]
