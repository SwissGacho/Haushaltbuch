""" Maintain database schemas """

from persistance.business_object_base import BOBase
from persistance.bo_descriptors import BOInt
from core.app_logging import getLogger

LOG = getLogger(__name__)


class DB_Schema(BOBase):
    _table = "schema_versions"
    version_nr = BOInt()

    def __init__(self, id=None, v_nr: int = None) -> None:
        super().__init__(id=id)
        self.version_nr = v_nr

    def __repr__(self) -> str:
        return f"<DB_Schema (id:{self.id}, version:{self.version_nr})>"
