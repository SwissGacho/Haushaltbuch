""" Maintain database schemas """

from typing import Optional

from core.app_logging import getLogger, log_exit

LOG = getLogger(__name__)

from core.app_logging import getLogger
from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOInt

LOG = getLogger(__name__)


class DBSchema(BOBase):
    "Version of the DB schema"
    _table = "schema_versions"
    version_nr = BOInt()

    def __init__(self, id=None, v_nr: Optional[int] = None) -> None:
        super().__init__(id=id)
        self.version_nr = v_nr

    def __repr__(self) -> str:
        return f"<DBSchema (id:{self.id}, version:{self.version_nr})>"


log_exit(LOG)
