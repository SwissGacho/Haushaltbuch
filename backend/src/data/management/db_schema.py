""" Maintain database schemas """

from persistance.business_object_base import BO_Base
from persistance.bo_descriptors import BO_int
from core.app_logging import getLogger

LOG = getLogger(__name__)


class DB_Schema(BO_Base):
    _table = "schema_versions"
    version_nr = BO_int()

    def __init__(self, id=None, v_nr: int = None) -> None:
        super().__init__(id=id)
        self.version_nr = v_nr

    def __repr__(self) -> str:
        return f"<DB_Schema (id:{self.id}, version:{self.version_nr})>"
